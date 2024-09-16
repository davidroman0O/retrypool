package retrypool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Worker interface for task processing
type Worker[T any] interface {
	Run(ctx context.Context, data T) error
}

// DeadTask struct to hold failed task information
type DeadTask[T any] struct {
	Data          T
	Retries       int
	TotalDuration time.Duration
	Errors        []error
}

// TaskWrapper now includes scheduledTime, triedWorkers, errors, and durations for worker tracking
type TaskWrapper[T any] struct {
	data          T
	retries       int
	totalDuration time.Duration
	timeLimit     time.Duration   // Zero means no limit
	scheduledTime time.Time       // For delay between retries
	triedWorkers  map[int]bool    // Track workers that have tried this task
	errors        []error         // Track errors for each attempt
	durations     []time.Duration // Track duration for each attempt
	ctx           context.Context
	cancel        context.CancelFunc
}

func (t *TaskWrapper[T]) Data() T {
	return t.data
}

func (t *TaskWrapper[T]) Retries() int {
	return t.retries
}

func (t *TaskWrapper[T]) TotalDuration() time.Duration {
	return t.totalDuration
}

func (t *TaskWrapper[T]) TimeLimit() time.Duration {
	return t.timeLimit
}

func (t *TaskWrapper[T]) ScheduledTime() time.Time {
	return t.scheduledTime
}

func (t *TaskWrapper[T]) TriedWorkers() map[int]bool {
	return t.triedWorkers
}

func (t *TaskWrapper[T]) Errors() []error {
	return t.errors
}

func (t *TaskWrapper[T]) Durations() []time.Duration {
	return t.durations
}

// taskQueue now stores pointers to taskWrapper
type taskQueue[T any] struct {
	tasks []*TaskWrapper[T]
}

// Option type for configuring the Pool
type Option[T any] func(*Pool[T])

// TaskOption type for configuring individual tasks
type TaskOption[T any] func(*TaskWrapper[T])

// Config struct to hold retry configurations
type Config[T any] struct {
	attempts         int
	attemptsForError map[error]int
	delay            time.Duration
	maxDelay         time.Duration
	maxJitter        time.Duration
	onRetry          OnRetryFunc[T]
	retryIf          RetryIfFunc
	delayType        DelayTypeFunc[T]
	lastErrorOnly    bool
	context          context.Context
	timer            Timer

	onTaskSuccess OnTaskSuccessFunc[T] // Callback when a task succeeds
	onTaskFailure OnTaskFailureFunc[T] // Callback when a task fails

	maxBackOffN uint

	contextFunc ContextFunc
}

// Pool struct updated to include Config and support dynamic worker management
type Pool[T any] struct {
	workers       map[int]Worker[T]          // Map of workers with unique worker IDs
	nextWorkerID  int                        // Counter for assigning unique worker IDs
	workerChans   map[int]chan struct{}      // Channels to signal workers to stop
	workerCancels map[int]context.CancelFunc // Cancel functions for worker contexts
	taskQueues    map[int]taskQueue[T]
	processing    int
	mu            sync.Mutex
	cond          *sync.Cond
	wg            sync.WaitGroup
	stopped       bool
	closed        atomic.Bool
	ctx           context.Context
	deadTasks     []DeadTask[T]

	config Config[T]
	timer  Timer
}

// NewPool initializes the Pool with given workers and options
func NewPool[T any](ctx context.Context, workers []Worker[T], options ...Option[T]) *Pool[T] {
	if len(workers) == 0 {
		panic("worker count does not match provided workers")
	}

	pool := &Pool[T]{
		workers:       make(map[int]Worker[T]),
		nextWorkerID:  0,
		workerChans:   make(map[int]chan struct{}),
		workerCancels: make(map[int]context.CancelFunc),
		taskQueues:    make(map[int]taskQueue[T]),
		config:        newDefaultConfig[T](),
		timer:         &timerImpl{},
		ctx:           ctx,
	}
	for _, option := range options {
		option(pool)
	}

	// Initialize workers with unique IDs
	for _, worker := range workers {
		workerID := pool.nextWorkerID
		pool.nextWorkerID++
		pool.workers[workerID] = worker
		pool.workerChans[workerID] = make(chan struct{})
	}

	pool.cond = sync.NewCond(&pool.mu)
	pool.startWorkers()
	return pool
}

// startWorkers updated to handle dynamic workers
func (p *Pool[T]) startWorkers() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for workerID := range p.workers {
		p.wg.Add(1)
		go p.workerLoop(workerID)
	}
}

// AddWorker adds a new worker to the pool dynamically
func (p *Pool[T]) AddWorker(worker Worker[T]) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	workerID := p.nextWorkerID
	p.nextWorkerID++

	p.workers[workerID] = worker
	p.workerChans[workerID] = make(chan struct{})

	p.wg.Add(1)
	go p.workerLoop(workerID)

	return workerID
}

// WorkerController interface provides methods to control workers
type WorkerController interface {
	InterruptWorker(workerID int) error
	RemoveWorker(workerID int) error
}

// InterruptWorker interrupts a worker's execution
// TODO: add resume worker...
func (p *Pool[T]) InterruptWorker(workerID int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	cancel, exists := p.workerCancels[workerID]
	if !exists {
		return fmt.Errorf("worker %d does not exist", workerID)
	}

	cancel()

	// Signal the worker to stop
	close(p.workerChans[workerID])

	// Requeue any tasks assigned to this worker
	for retries, queue := range p.taskQueues {
		newTasks := make([]*TaskWrapper[T], 0, len(queue.tasks))
		for _, task := range queue.tasks {
			if task.triedWorkers[workerID] {
				delete(task.triedWorkers, workerID)
				task.retries--
				p.requeueTask(task, errors.New("worker interrupted"))
			} else {
				newTasks = append(newTasks, task)
			}
		}
		p.taskQueues[retries] = taskQueue[T]{tasks: newTasks}
	}

	p.cond.Broadcast() // Signal that the queue has changed

	return nil
}

// RemoveWorker removes a worker from the pool
func (p *Pool[T]) RemoveWorker(workerID int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, exists := p.workers[workerID]
	if !exists {
		return fmt.Errorf("worker %d does not exist", workerID)
	}

	// Signal the worker to stop
	close(p.workerChans[workerID])

	// Wait for the worker to finish its current task
	for p.processing > 0 {
		p.cond.Wait()
	}

	// Remove the worker from the pool
	delete(p.workers, workerID)
	delete(p.workerChans, workerID)

	// Cancel the worker's context
	if cancel, exists := p.workerCancels[workerID]; exists {
		cancel()
		delete(p.workerCancels, workerID)
	}

	// Requeue any tasks assigned to this worker
	for retries, queue := range p.taskQueues {
		newTasks := make([]*TaskWrapper[T], 0, len(queue.tasks))
		for _, task := range queue.tasks {
			if task.triedWorkers[workerID] {
				delete(task.triedWorkers, workerID)
				if len(task.triedWorkers) == 0 {
					// If this was the only worker that tried this task, reset it
					task.retries = 0
					newTasks = append(newTasks, task)
				}
			} else {
				newTasks = append(newTasks, task)
			}
		}
		p.taskQueues[retries] = taskQueue[T]{tasks: newTasks}
	}

	return nil
}

// workerLoop updated to handle scheduledTime, triedWorkers, and worker interruption
func (p *Pool[T]) workerLoop(workerID int) {
	defer p.wg.Done()
	stopChan := p.workerChans[workerID]

	var currentTask *TaskWrapper[T]
	for {
		select {
		case <-stopChan:
			if currentTask != nil {
				// Requeue the current task
				p.mu.Lock()
				p.requeueTask(currentTask, errors.New("worker interrupted"))
				p.mu.Unlock()
			}
			return
		default:
		}

		p.mu.Lock()
		for p.isAllQueuesEmpty() && !p.stopped {
			p.cond.Wait()
		}

		if p.stopped && p.isAllQueuesEmpty() {
			p.mu.Unlock()
			return
		}

		// Check if context is canceled
		select {
		case <-p.ctx.Done():
			p.mu.Unlock()
			return
		default:
		}

		// Get next task for this worker
		retries, idx, task, ok := p.getNextTask(workerID)
		if !ok {
			// No tasks available for this worker
			p.mu.Unlock()
			continue
		}

		now := time.Now()
		if now.Before(task.scheduledTime) {
			waitDuration := task.scheduledTime.Sub(now)
			p.mu.Unlock()
			select {
			case <-p.timer.After(waitDuration):
				// Time to process the task
			case <-p.ctx.Done():
				return
			}
			continue // Re-acquire the lock and re-check conditions
		}

		// Remove the task from the queue
		q := p.taskQueues[retries]
		q.tasks = append(q.tasks[:idx], q.tasks[idx+1:]...)
		if len(q.tasks) == 0 {
			delete(p.taskQueues, retries)
		} else {
			p.taskQueues[retries] = q
		}

		// Mark the task as tried by this worker
		if task.triedWorkers == nil {
			task.triedWorkers = make(map[int]bool)
		}
		task.triedWorkers[workerID] = true

		p.processing++
		p.mu.Unlock()

		currentTask = task
		if task.timeLimit > 0 {
			go p.enforceTimeLimit(task)
		}
		p.runWorkerWithFailsafe(workerID, task)
		currentTask = nil

		p.mu.Lock()
		p.processing--
		p.cond.Signal()
		p.mu.Unlock()
	}
}

func (p *Pool[T]) enforceTimeLimit(task *TaskWrapper[T]) {
	if task.timeLimit <= 0 {
		return
	}

	select {
	case <-time.After(task.timeLimit - task.totalDuration):
		task.cancel() // This will cancel the task's context
	case <-task.ctx.Done():
		// Task completed or was cancelled for other reasons
	}
}

// isAllQueuesEmpty checks if all task queues are empty
func (p *Pool[T]) isAllQueuesEmpty() bool {
	for _, q := range p.taskQueues {
		if len(q.tasks) > 0 {
			return false
		}
	}
	return true
}

// getNextTask returns the next task that the worker hasn't tried
func (p *Pool[T]) getNextTask(workerID int) (int, int, *TaskWrapper[T], bool) {
	for retries, q := range p.taskQueues {
		for idx, task := range q.tasks {
			if task.triedWorkers == nil {
				task.triedWorkers = make(map[int]bool)
			}
			if len(task.triedWorkers) >= len(p.workers) {
				// All workers have tried this task; reset the list
				task.triedWorkers = make(map[int]bool)
			}
			if !task.triedWorkers[workerID] {
				// Found a task the worker hasn't tried
				return retries, idx, task, true
			}
		}
	}
	return 0, 0, nil, false
}

// runWorkerWithFailsafe updated to handle OnRetry, RetryIf, and callbacks
func (p *Pool[T]) runWorkerWithFailsafe(workerID int, task *TaskWrapper[T]) {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic occurred: %v", r)
			task.errors = append(task.errors, err)
			p.requeueTask(task, err)
		}
	}()

	start := time.Now()
	err := p.workers[workerID].Run(task.ctx, task.data)
	duration := time.Since(start)
	task.totalDuration += duration
	task.durations = append(task.durations, duration)

	if err != nil {
		task.errors = append(task.errors, err)
		if IsUnrecoverable(err) {
			p.addToDeadTasks(task, err)
			return
		}
		// Check if the error is due to time limit exceeded
		if err == context.DeadlineExceeded || (err == context.Canceled && task.totalDuration >= task.timeLimit) {
			p.addToDeadTasks(task, fmt.Errorf("task exceeded time limit of %v", task.timeLimit))
			return
		}
		if err != context.Canceled && p.config.retryIf(err) {
			p.config.onRetry(task.retries, err, task.data)
			if p.config.onTaskFailure != nil {
				p.config.onTaskFailure(p, workerID, p.workers[workerID], task, err)
			}
			p.requeueTask(task, err)
		} else {
			log.Printf("Task not retried due to RetryIf policy: %v\n", err)
			p.addToDeadTasks(task, err)
		}
	} else {
		if p.config.onTaskSuccess != nil {
			p.config.onTaskSuccess(p, workerID, p.workers[workerID], task)
		}
	}
}

func IsUnrecoverable(err error) bool {
	var unrecoverableErr unrecoverableError
	return errors.As(err, &unrecoverableErr)
}

// requeueTask updated to handle delays and keep triedWorkers intact
func (p *Pool[T]) requeueTask(task *TaskWrapper[T], err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	task.retries++

	// Check if task has exceeded time limit
	if task.timeLimit > 0 && task.totalDuration >= task.timeLimit {
		log.Printf("Task exceeded time limit after %d attempts: %v\n", task.retries, err)
		p.addToDeadTasks(task, err)
		return
	}

	// Check if max attempts reached (unless unlimited retries)
	if p.config.attempts != UnlimitedAttempts && task.retries >= p.config.attempts {
		log.Printf("Task failed after %d attempts: %v\n", task.retries, err)
		p.addToDeadTasks(task, err)
		return
	}

	// Calculate delay before next retry
	delay := p.calculateDelay(task.retries, err)
	task.scheduledTime = time.Now().Add(delay)

	// Requeue task
	q := p.taskQueues[task.retries]
	q.tasks = append(q.tasks, task)
	p.taskQueues[task.retries] = q
	p.cond.Signal()
}

// calculateDelay calculates delay based on DelayType
func (p *Pool[T]) calculateDelay(n int, err error) time.Duration {
	delayTime := p.config.delayType(n, err, &p.config)
	if p.config.maxDelay > 0 && delayTime > p.config.maxDelay {
		delayTime = p.config.maxDelay
	}
	return delayTime
}

// addToDeadTasks adds task to dead tasks list
func (p *Pool[T]) addToDeadTasks(task *TaskWrapper[T], finalError error) {
	totalDuration := task.totalDuration
	for _, duration := range task.durations {
		totalDuration += duration
	}
	errors := make([]error, len(task.errors))
	copy(errors, task.errors)
	if finalError != nil && (len(errors) == 0 || finalError.Error() != errors[len(errors)-1].Error()) {
		errors = append(errors, finalError)
	}
	p.deadTasks = append(p.deadTasks, DeadTask[T]{
		Data:          task.data,
		Retries:       task.retries,
		TotalDuration: totalDuration,
		Errors:        errors,
	})
}

// Dispatch updated to accept TaskOptions
func (p *Pool[T]) Dispatch(data T, options ...TaskOption[T]) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stopped {
		return errors.New("pool is closed")
	}

	taskCtx, cancel := context.WithCancel(p.ctx)
	task := &TaskWrapper[T]{
		data:         data,
		retries:      0,
		triedWorkers: make(map[int]bool),
		errors:       make([]error, 0),
		durations:    make([]time.Duration, 0),
		ctx:          taskCtx,
		cancel:       cancel,
	}
	for _, opt := range options {
		opt(task)
	}
	task.scheduledTime = time.Now()

	// Find the queue with the least number of tasks
	minQueueSize := int(^uint(0) >> 1) // Max int
	var minQueue int
	for retries, queue := range p.taskQueues {
		if len(queue.tasks) < minQueueSize {
			minQueueSize = len(queue.tasks)
			minQueue = retries
		}
	}

	// If all queues are empty, start with queue 0
	if minQueueSize == int(^uint(0)>>1) {
		minQueue = 0
	}

	q := p.taskQueues[minQueue]
	q.tasks = append(q.tasks, task)
	p.taskQueues[minQueue] = q

	// Signal all waiting workers that there's a new task
	p.cond.Broadcast()

	return nil
}

// DeadTasks returns a copy of the dead tasks list
func (p *Pool[T]) DeadTasks() []DeadTask[T] {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]DeadTask[T](nil), p.deadTasks...)
}

// QueueSize returns the total number of tasks in the queue
func (p *Pool[T]) QueueSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	total := 0
	for _, q := range p.taskQueues {
		total += len(q.tasks)
	}
	return total
}

// ProcessingCount returns the number of tasks currently being processed
func (p *Pool[T]) ProcessingCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.processing
}

// Close stops the pool and waits for all tasks to complete
func (p *Pool[T]) Close() {
	if p.closed.Load() {
		return
	}
	p.closed.Store(true)
	p.mu.Lock()
	p.stopped = true
	p.mu.Unlock()
	p.cond.Broadcast()
	p.wg.Wait()
}

// ForceClose stops the pool without waiting for tasks to complete
func (p *Pool[T]) ForceClose() {
	if p.closed.Load() {
		return
	}
	p.closed.Store(true)
	p.mu.Lock()
	p.stopped = true
	for k := range p.taskQueues {
		q := p.taskQueues[k]
		q.tasks = nil
		p.taskQueues[k] = q
	}
	p.mu.Unlock()
	p.cond.Broadcast()
}

// WaitWithCallback waits for the pool to complete while calling a callback function
func (p *Pool[T]) WaitWithCallback(ctx context.Context, callback func(queueSize, processingCount int) bool, interval time.Duration) error {
	// defer p.Close()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if !callback(p.QueueSize(), p.ProcessingCount()) {
				return nil
			}
			time.Sleep(interval)
		}
	}
}

// newDefaultConfig initializes default retry configurations
func newDefaultConfig[T any]() Config[T] {
	return Config[T]{
		attempts:         10,
		attemptsForError: make(map[error]int),
		delay:            100 * time.Millisecond,
		maxJitter:        100 * time.Millisecond,
		onRetry:          func(n int, err error, task T) {},
		retryIf:          IsRecoverable,
		delayType:        CombineDelay[T](BackOffDelay[T], RandomDelay[T]),
		lastErrorOnly:    false,
		context:          context.Background(),
		timer:            &timerImpl{},
		onTaskSuccess:    nil, // Default is nil; can be set via options
		onTaskFailure:    nil, // Default is nil; can be set via options
	}
}

// IsRecoverable checks if an error is recoverable
func IsRecoverable(err error) bool {
	return !errors.Is(err, unrecoverableError{})
}

// Unrecoverable wraps an error as unrecoverable
func Unrecoverable(err error) error {
	return unrecoverableError{err}
}

// unrecoverableError type
type unrecoverableError struct {
	error
}

// Is method for unrecoverableError
func (unrecoverableError) Is(err error) bool {
	_, isUnrecoverable := err.(unrecoverableError)
	return isUnrecoverable
}

// Timer interface for custom timers
type Timer interface {
	After(time.Duration) <-chan time.Time
}

// timerImpl is the default timer
type timerImpl struct{}

func (t *timerImpl) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// Option functions for configuring the Pool

// WithAttempts sets the maximum number of attempts
func WithAttempts[T any](attempts int) Option[T] {
	return func(p *Pool[T]) {
		p.config.attempts = attempts
	}
}

// WithWorkerContext create a specific context for each worker
func WithWorkerContext[T any](fn ContextFunc) Option[T] {
	return func(t *Pool[T]) {
		t.config.contextFunc = fn
	}
}

// WithDelay sets the delay between retries
func WithDelay[T any](delay time.Duration) Option[T] {
	return func(p *Pool[T]) {
		p.config.delay = delay
	}
}

// WithMaxDelay sets the maximum delay between retries
func WithMaxDelay[T any](maxDelay time.Duration) Option[T] {
	return func(p *Pool[T]) {
		p.config.maxDelay = maxDelay
	}
}

// WithMaxJitter sets the maximum random jitter between retries
func WithMaxJitter[T any](maxJitter time.Duration) Option[T] {
	return func(p *Pool[T]) {
		p.config.maxJitter = maxJitter
	}
}

// WithDelayType sets the delay type function
func WithDelayType[T any](delayType DelayTypeFunc[T]) Option[T] {
	return func(p *Pool[T]) {
		p.config.delayType = delayType
	}
}

// WithOnRetry sets the OnRetry callback function
func WithOnRetry[T any](onRetry OnRetryFunc[T]) Option[T] {
	return func(p *Pool[T]) {
		p.config.onRetry = onRetry
	}
}

// WithRetryIf sets the RetryIf function
func WithRetryIf[T any](retryIf RetryIfFunc) Option[T] {
	return func(p *Pool[T]) {
		p.config.retryIf = retryIf
	}
}

// WithContext sets the context for the Pool
func WithContext[T any](ctx context.Context) Option[T] {
	return func(p *Pool[T]) {
		p.config.context = ctx
	}
}

// WithTimer allows setting a custom timer
func WithTimer[T any](timer Timer) Option[T] {
	return func(p *Pool[T]) {
		p.timer = timer
	}
}

// WithOnTaskSuccess sets the OnTaskSuccess callback function
func WithOnTaskSuccess[T any](onTaskSuccess OnTaskSuccessFunc[T]) Option[T] {
	return func(p *Pool[T]) {
		p.config.onTaskSuccess = onTaskSuccess
	}
}

// WithOnTaskFailure sets the OnTaskFailure callback function
func WithOnTaskFailure[T any](onTaskFailure OnTaskFailureFunc[T]) Option[T] {
	return func(p *Pool[T]) {
		p.config.onTaskFailure = onTaskFailure
	}
}

// TaskOption functions for configuring individual tasks

// WithTimeLimit sets a time limit for a task
func WithTimeLimit[T any](limit time.Duration) TaskOption[T] {
	return func(t *TaskWrapper[T]) {
		t.timeLimit = limit
	}
}

// DelayTypeFunc signature
type DelayTypeFunc[T any] func(n int, err error, config *Config[T]) time.Duration

// OnRetryFunc signature
type OnRetryFunc[T any] func(attempt int, err error, task T)

// OnTaskSuccessFunc is the type of function called when a task succeeds
type OnTaskSuccessFunc[T any] func(controller WorkerController, workerID int, worker Worker[T], task *TaskWrapper[T])

// OnTaskFailureFunc is the type of function called when a task fails
type OnTaskFailureFunc[T any] func(controller WorkerController, workerID int, worker Worker[T], task *TaskWrapper[T], err error)

// RetryIfFunc signature
type RetryIfFunc func(error) bool

// ContextFunc signature
type ContextFunc func() context.Context

// DelayType functions

// BackOffDelay increases delay exponentially
func BackOffDelay[T any](n int, _ error, config *Config[T]) time.Duration {
	const max = 62

	if config.maxBackOffN == 0 {
		if config.delay <= 0 {
			config.delay = 1
		}
		config.maxBackOffN = max - uint(math.Floor(math.Log2(float64(config.delay))))
	}

	if n > int(config.maxBackOffN) {
		n = int(config.maxBackOffN)
	}

	return config.delay << n
}

// FixedDelay keeps the delay constant
func FixedDelay[T any](_ int, _ error, config *Config[T]) time.Duration {
	return config.delay
}

// RandomDelay adds random jitter
func RandomDelay[T any](_ int, _ error, config *Config[T]) time.Duration {
	return time.Duration(rand.Int63n(int64(config.maxJitter)))
}

// CombineDelay combines multiple DelayType functions
func CombineDelay[T any](delays ...DelayTypeFunc[T]) DelayTypeFunc[T] {
	const maxInt64 = uint64(math.MaxInt64)

	return func(n int, err error, config *Config[T]) time.Duration {
		var total uint64
		for _, delay := range delays {
			total += uint64(delay(n, err, config))
			if total > maxInt64 {
				total = maxInt64
			}
		}
		return time.Duration(total)
	}
}

// Constants
const UnlimitedAttempts = -1

// Config getters
func (c *Config[T]) Attempts() int {
	return c.attempts
}

func (c *Config[T]) AttemptsForError() map[error]int {
	return c.attemptsForError
}

func (c *Config[T]) Delay() time.Duration {
	return c.delay
}

func (c *Config[T]) MaxDelay() time.Duration {
	return c.maxDelay
}

func (c *Config[T]) MaxJitter() time.Duration {
	return c.maxJitter
}

func (c *Config[T]) OnRetry() OnRetryFunc[T] {
	return c.onRetry
}

func (c *Config[T]) RetryIf() RetryIfFunc {
	return c.retryIf
}

func (c *Config[T]) DelayType() DelayTypeFunc[T] {
	return c.delayType
}

func (c *Config[T]) LastErrorOnly() bool {
	return c.lastErrorOnly
}

func (c *Config[T]) Context() context.Context {
	return c.context
}

func (c *Config[T]) Timer() Timer {
	return c.timer
}

func (c *Config[T]) MaxBackOffN() uint {
	return c.maxBackOffN
}
