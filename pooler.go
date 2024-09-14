package retrypool

import (
	"context"
	"errors"
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
	Error         error
}

// taskWrapper now includes scheduledTime and triedWorkers for worker tracking
type taskWrapper[T any] struct {
	data          T
	retries       int
	totalDuration time.Duration
	timeLimit     time.Duration // Zero means no limit
	scheduledTime time.Time     // For delay between retries
	triedWorkers  map[int]bool  // Track workers that have tried this task
}

// taskQueue now stores pointers to taskWrapper
type taskQueue[T any] struct {
	tasks []*taskWrapper[T]
}

// Option type for configuring the Pool
type Option[T any] func(*Pool[T])

// TaskOption type for configuring individual tasks
type TaskOption[T any] func(*taskWrapper[T])

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

	maxBackOffN uint
}

// Pool struct updated to include Config
type Pool[T any] struct {
	workers    []Worker[T]
	taskQueues map[int]taskQueue[T]
	processing int
	mu         sync.Mutex
	cond       *sync.Cond
	wg         sync.WaitGroup
	stopped    bool
	closed     atomic.Bool
	ctx        context.Context
	deadTasks  []DeadTask[T]

	config Config[T]
	timer  Timer
}

// NewPool initializes the Pool with given workers and options
func NewPool[T any](ctx context.Context, workers []Worker[T], options ...Option[T]) *Pool[T] {
	if len(workers) == 0 {
		panic("worker count does not match provided workers")
	}

	pool := &Pool[T]{
		workers:    workers,
		ctx:        ctx,
		taskQueues: make(map[int]taskQueue[T]),
		config:     newDefaultConfig[T](),
		timer:      &timerImpl{},
	}
	for _, option := range options {
		option(pool)
	}

	pool.cond = sync.NewCond(&pool.mu)
	pool.startWorkers()
	return pool
}

// startWorkers updated to handle scheduledTime and triedWorkers
func (p *Pool[T]) startWorkers() {
	for i := 0; i < len(p.workers); i++ {
		p.wg.Add(1)
		go p.workerLoop(i)
	}
}

// workerLoop updated to handle scheduledTime and triedWorkers
func (p *Pool[T]) workerLoop(workerID int) {
	defer p.wg.Done()
	for {
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

		p.runWorkerWithFailsafe(workerID, task)

		p.mu.Lock()
		p.processing--
		p.cond.Signal()
		p.mu.Unlock()
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
func (p *Pool[T]) getNextTask(workerID int) (int, int, *taskWrapper[T], bool) {
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

// runWorkerWithFailsafe updated to handle OnRetry and RetryIf
func (p *Pool[T]) runWorkerWithFailsafe(workerID int, task *taskWrapper[T]) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Worker %d panicked: %v\n", workerID, r)
			p.requeueTask(task, errors.New("panic occurred"))
		}
	}()

	start := time.Now()
	err := p.workers[workerID].Run(p.ctx, task.data)
	duration := time.Since(start)
	task.totalDuration += duration

	if err != nil {
		if err != context.Canceled {
			if p.config.retryIf(err) {
				p.config.onRetry(task.retries, err, task.data)
				p.requeueTask(task, err)
			} else {
				log.Printf("Task not retried due to RetryIf policy: %v\n", err)
				p.addToDeadTasks(task, err)
			}
		}
	}
}

// requeueTask updated to handle delays and keep triedWorkers intact
func (p *Pool[T]) requeueTask(task *taskWrapper[T], err error) {
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
func (p *Pool[T]) addToDeadTasks(task *taskWrapper[T], err error) {
	p.deadTasks = append(p.deadTasks, DeadTask[T]{
		Data:          task.data,
		Retries:       task.retries,
		TotalDuration: task.totalDuration,
		Error:         err,
	})
}

// Dispatch updated to accept TaskOptions
func (p *Pool[T]) Dispatch(data T, options ...TaskOption[T]) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stopped {
		return errors.New("pool is closed")
	}

	task := &taskWrapper[T]{data: data, retries: 0}
	for _, opt := range options {
		opt(task)
	}
	task.scheduledTime = time.Now()

	q := p.taskQueues[0]
	q.tasks = append(q.tasks, task)
	p.taskQueues[0] = q
	p.cond.Signal()
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
	defer p.Close()
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

// TaskOption functions for configuring individual tasks

// WithTimeLimit sets a time limit for a task
func WithTimeLimit[T any](limit time.Duration) TaskOption[T] {
	return func(t *taskWrapper[T]) {
		t.timeLimit = limit
	}
}

// DelayTypeFunc signature
type DelayTypeFunc[T any] func(n int, err error, config *Config[T]) time.Duration

// OnRetryFunc signature
type OnRetryFunc[T any] func(attempt int, err error, task T)

// RetryIfFunc signature
type RetryIfFunc func(error) bool

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
