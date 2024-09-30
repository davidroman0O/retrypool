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

// TaskWrapper includes scheduledTime, triedWorkers, errors, durations, and panicOnTimeout
type TaskWrapper[T any] struct {
	data           T
	retries        int
	totalDuration  time.Duration
	timeLimit      time.Duration   // Zero means no overall limit
	maxDuration    time.Duration   // Max duration per attempt
	scheduledTime  time.Time       // For delay between retries
	triedWorkers   map[int]bool    // Track workers that have tried this task
	errors         []error         // Track errors for each attempt
	durations      []time.Duration // Track duration for each attempt
	ctx            context.Context
	cancel         context.CancelFunc
	immediateRetry bool
	panicOnTimeout bool // Field to trigger panic on timeout
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

// taskQueue stores pointers to TaskWrapper
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

	panicHandler PanicHandlerFunc[T]
}

// workerState holds all per-worker data
type workerState[T any] struct {
	worker         Worker[T]
	stopChan       chan struct{}
	doneChan       chan struct{}
	cancel         context.CancelFunc
	ctx            context.Context
	forcePanicFlag *atomic.Bool
	currentTask    *TaskWrapper[T] // Field to track the current task
	interrupted    bool            // New field to track if the worker has been interrupted

}

// Pool struct updated to include Config and support dynamic worker management
type Pool[T any] struct {
	workers         map[int]*workerState[T] // Map of workers with unique worker IDs
	nextWorkerID    int                     // Counter for assigning unique worker IDs
	workersToRemove map[int]bool
	taskQueues      map[int]taskQueue[T]
	processing      int
	mu              sync.Mutex
	cond            *sync.Cond
	wg              sync.WaitGroup
	stopped         bool
	closed          atomic.Bool
	ctx             context.Context
	deadTasks       []DeadTask[T]

	config Config[T]
	timer  Timer
}

// New initializes the Pool with given workers and options
func New[T any](ctx context.Context, workers []Worker[T], options ...Option[T]) *Pool[T] {

	pool := &Pool[T]{
		workers:         make(map[int]*workerState[T]),
		nextWorkerID:    0,
		workersToRemove: make(map[int]bool),
		taskQueues:      make(map[int]taskQueue[T]),
		config:          newDefaultConfig[T](),
		timer:           &timerImpl{},
		ctx:             ctx,
	}
	for _, option := range options {
		option(pool)
	}

	pool.cond = sync.NewCond(&pool.mu)

	// Initialize workers with unique IDs
	for _, worker := range workers {
		pool.AddWorker(worker)
	}

	return pool
}

// AddWorker adds a new worker to the pool dynamically
func (p *Pool[T]) AddWorker(worker Worker[T]) int {
	done := make(chan int)
	go func() {
		defer close(done)
		p.mu.Lock()
		defer p.mu.Unlock()

		workerID := p.nextWorkerID
		p.nextWorkerID++

		var workerCtx context.Context
		var workerCancel context.CancelFunc
		if p.config.contextFunc != nil {
			workerCtx = p.config.contextFunc()
		} else {
			workerCtx = p.ctx
		}
		workerCtx, workerCancel = context.WithCancel(workerCtx)
		state := &workerState[T]{
			worker:         worker,
			stopChan:       make(chan struct{}),
			doneChan:       make(chan struct{}),
			cancel:         workerCancel,
			ctx:            workerCtx,
			forcePanicFlag: new(atomic.Bool),
		}

		p.workers[workerID] = state

		p.wg.Add(1)
		go p.workerLoop(workerID)
		done <- workerID
	}()

	return <-done
}

// Method to mark worker for removal
func (p *Pool[T]) RemovalWorker(workerID int) {
	p.mu.Lock()
	p.workersToRemove[workerID] = true
	p.mu.Unlock()
}

// WorkerController interface provides methods to control workers
type WorkerController[T any] interface {
	AddWorker(worker Worker[T]) int
	RemovalWorker(workerID int)
	RestartWorker(workerID int) error
	InterruptWorker(workerID int, options ...WorkerInterruptOption) error
}

func (p *Pool[T]) RestartWorker(workerID int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	state, exists := p.workers[workerID]
	if !exists {
		return fmt.Errorf("worker %d does not exist", workerID)
	}

	if !state.interrupted {
		return fmt.Errorf("worker %d is not interrupted and cannot be restarted", workerID)
	}

	// Create a new context for the worker
	workerCtx, workerCancel := context.WithCancel(p.ctx)
	state.ctx = workerCtx
	state.cancel = workerCancel
	state.forcePanicFlag.Store(false)
	state.interrupted = false

	// Create a new stopChan and doneChan
	state.stopChan = make(chan struct{})
	state.doneChan = make(chan struct{})

	// Start a new goroutine for the worker
	p.wg.Add(1)
	go p.workerLoop(workerID)

	fmt.Printf("Worker %d has been restarted\n", workerID)
	return nil
}

// RemoveWorker removes a worker from the pool
func (p *Pool[T]) RemoveWorker(workerID int) error {
	p.mu.Lock()

	state, exists := p.workers[workerID]
	if !exists {
		p.mu.Unlock()
		return fmt.Errorf("worker %d does not exist", workerID)
	}

	// Cancel the worker's context
	state.cancel()

	// Close the worker's stop channel
	close(state.stopChan)

	// Signal the worker to wake up if it's waiting
	p.cond.Broadcast()

	doneChan := state.doneChan
	p.mu.Unlock() // Release lock while waiting

	const workerStopTimeout = 5 * time.Second
	const workerForceStopTimeout = 5 * time.Second

	// Wait for the worker to finish, with a timeout
	select {
	case <-doneChan:
		// Worker has exited
	case <-time.After(workerStopTimeout):
		// Worker did not exit in time, force panic
		p.mu.Lock()
		state.forcePanicFlag.Store(true)
		p.mu.Unlock()

		// Signal the worker again
		p.cond.Broadcast()

		// Wait again for the worker to exit
		select {
		case <-doneChan:
			// Worker has exited
		case <-time.After(workerForceStopTimeout):
			// Even after forcing panic, worker did not exit
			return fmt.Errorf("worker %d did not exit after forced panic", workerID)
		}
	}

	// Now safe to remove worker from the pool
	p.mu.Lock()
	delete(p.workers, workerID)
	p.mu.Unlock()

	// Requeue any tasks assigned to this worker
	p.requeueTasksFromWorker(workerID)

	return nil
}

// requeueTasksFromWorker reassigns tasks from the removed worker to other workers
func (p *Pool[T]) requeueTasksFromWorker(workerID int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Remove the worker's triedTasks from all tasks
	for retries, queue := range p.taskQueues {
		newTasks := make([]*TaskWrapper[T], 0, len(queue.tasks))
		for _, task := range queue.tasks {
			if task.triedWorkers != nil {
				delete(task.triedWorkers, workerID)
			}
			newTasks = append(newTasks, task)
		}
		p.taskQueues[retries] = taskQueue[T]{tasks: newTasks}
	}

	// Signal workers that the task queues have changed
	p.cond.Broadcast()
}

// workerLoop updated to handle scheduledTime, triedWorkers, and worker interruption
func (p *Pool[T]) workerLoop(workerID int) {
	defer p.wg.Done()

	p.mu.Lock()
	state, exists := p.workers[workerID]
	if !exists {
		p.mu.Unlock()
		return
	}
	stopChan := state.stopChan
	doneChan := state.doneChan
	ctx := state.ctx
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		defer p.mu.Unlock()

		if r := recover(); r != nil {
			fmt.Printf("Worker %d recovered from panic: %v\n", workerID, r)
		}

		select {
		case <-doneChan: // Channel is already closed
		default:
			close(doneChan)
		}
	}()

	for {
		select {
		case <-stopChan:
			return
		default:
		}

		p.mu.Lock()
		for p.isAllQueuesEmpty() && !p.stopped {
			p.cond.Wait()

			// Check if context is canceled
			if ctx.Err() != nil {
				p.mu.Unlock()
				return
			}
		}

		if p.stopped && p.isAllQueuesEmpty() {
			p.mu.Unlock()
			return
		}

		// Check if context is canceled before proceeding
		if ctx.Err() != nil {
			p.mu.Unlock()
			return
		}

		retries, idx, task, ok := p.getNextTask(workerID)
		if !ok {
			p.mu.Unlock()
			continue
		}

		now := time.Now()
		if now.Before(task.scheduledTime) {
			waitDuration := task.scheduledTime.Sub(now)
			p.mu.Unlock()
			select {
			case <-p.timer.After(waitDuration):
			case <-p.ctx.Done():
				return
			}
			continue
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

		// Set the current task
		state.currentTask = task

		p.processing++
		p.mu.Unlock()

		// Check if context is canceled before processing the task
		if ctx.Err() != nil {
			p.mu.Lock()
			p.processing--
			p.mu.Unlock()
			return
		}

		p.runWorkerWithFailsafe(workerID, task)

		p.mu.Lock()
		if p.workersToRemove[workerID] {
			delete(p.workersToRemove, workerID)
			p.mu.Unlock()
			go p.RemoveWorker(workerID)
			return
		}

		// Unset the current task
		state.currentTask = nil

		if state.interrupted {
			p.processing--
			p.mu.Unlock()
			return // Exit the loop if the worker was interrupted
		}

		p.processing--
		p.cond.Signal()
		p.mu.Unlock()
	}
}

func (p *Pool[T]) enforceTimeLimit(cancelFunc context.CancelFunc, timeLimit, totalDuration time.Duration, ctx context.Context) {
	if timeLimit <= 0 {
		return
	}

	remainingTime := timeLimit - totalDuration
	if remainingTime <= 0 {
		cancelFunc()
		return
	}

	select {
	case <-p.timer.After(remainingTime):
		cancelFunc() // This cancels the attempt's context
	case <-ctx.Done():
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
	// First, check for immediate retry tasks this worker hasn't tried
	for retries, q := range p.taskQueues {
		for idx, task := range q.tasks {
			if task.immediateRetry && !task.triedWorkers[workerID] {
				return retries, idx, task, true
			}
		}
	}

	// Then, check for any task this worker hasn't tried
	for retries, q := range p.taskQueues {
		for idx, task := range q.tasks {
			if task.triedWorkers == nil {
				task.triedWorkers = make(map[int]bool)
			}
			if !task.triedWorkers[workerID] {
				return retries, idx, task, true
			}
		}
	}

	// If all tasks have been tried by this worker, return no task
	return 0, 0, nil, false
}

// RangeTasks iterates over all tasks in the pool, including those currently being processed.
// The callback function receives the task data and the worker ID (-1 if the task is in the queue).
// If the callback returns false, the iteration stops.
type TaskStatus int

const (
	TaskStatusQueued TaskStatus = iota
	TaskStatusProcessing
)

func (p *Pool[T]) RangeTasks(cb func(data T, workerID int, status TaskStatus) bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Iterate over tasks currently being processed
	for workerID, state := range p.workers {
		if state.currentTask != nil {
			if !cb(state.currentTask.data, workerID, TaskStatusProcessing) {
				return false
			}
		}
	}

	// Iterate over tasks in the queues
	for workerID, queue := range p.taskQueues {
		for _, task := range queue.tasks {
			if !cb(task.data, workerID, TaskStatusQueued) {
				return false
			}
		}
	}

	return true
}

type WorkerInterruptOption func(*WorkerInterruptConfig)

type WorkerInterruptConfig struct {
	RemoveWorker bool
	RemoveTask   bool
	ReassignTask bool
	ForcePanic   bool
	Restart      bool
}

// Option to restart the worker after interruption
func WithRestart() WorkerInterruptOption {
	return func(cfg *WorkerInterruptConfig) {
		cfg.Restart = true
	}
}

// Option to remove the worker after interruption.
func WithRemoveWorker() WorkerInterruptOption {
	return func(cfg *WorkerInterruptConfig) {
		cfg.RemoveWorker = true
	}
}

// Option to remove the task the worker was processing.
func WithRemoveTask() WorkerInterruptOption {
	return func(cfg *WorkerInterruptConfig) {
		cfg.RemoveTask = true
	}
}

// Option to reassign the task for retrying.
func WithReassignTask() WorkerInterruptOption {
	return func(cfg *WorkerInterruptConfig) {
		cfg.ReassignTask = true
	}
}

// Option to force the worker to panic.
func WithForcePanic() WorkerInterruptOption {
	return func(cfg *WorkerInterruptConfig) {
		cfg.ForcePanic = true
	}
}

// InterruptWorker cancels a worker's current task and optionally removes the worker.
// It can also force the worker to panic.
func (p *Pool[T]) InterruptWorker(workerID int, options ...WorkerInterruptOption) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	state, exists := p.workers[workerID]
	if !exists {
		return fmt.Errorf("worker %d does not exist", workerID)
	}

	// Apply options
	cfg := &WorkerInterruptConfig{
		RemoveWorker: false,
		RemoveTask:   false,
		ReassignTask: false,
		ForcePanic:   false,
		Restart:      false,
	}
	for _, opt := range options {
		opt(cfg)
	}

	// Set the forcePanicFlag if requested
	if cfg.ForcePanic {
		state.forcePanicFlag.Store(true)
	}

	// Cancel the worker's context to trigger the interrupt
	state.cancel()

	// Cancel the current task's context if it's running
	task := state.currentTask
	if task != nil {
		task.cancel()
	}

	// Close the stopChan to signal the worker to stop
	close(state.stopChan)

	// Handle task options
	if task != nil {
		if cfg.RemoveTask {
			// Task is already canceled and will not be retried
		} else if cfg.ReassignTask {
			// Reset the task's context
			taskCtx, cancel := context.WithCancel(p.ctx)
			task.ctx = taskCtx
			task.cancel = cancel
			task.triedWorkers = make(map[int]bool)
			task.scheduledTime = time.Now()
			// Add the task back to the taskQueues for the current retry count
			q := p.taskQueues[task.retries]
			q.tasks = append(q.tasks, task)
			p.taskQueues[task.retries] = q
		}
	}

	if cfg.RemoveWorker {
		// delete(p.workers, workerID)
		err := p.RemoveWorker(workerID)
		if err != nil {
			return fmt.Errorf("failed to remove worker %d: %v", workerID, err)
		}
		// fmt.Printf("Worker %d has been removed\n", workerID)
	} else {
		state.interrupted = true
		// fmt.Printf("Worker %d has been interrupted\n", workerID)

		if cfg.Restart {
			p.mu.Unlock()
			err := p.RestartWorker(workerID)
			p.mu.Lock()
			if err != nil {
				return fmt.Errorf("failed to restart worker %d: %v", workerID, err)
			}
		}
	}

	p.cond.Broadcast() // Signal all workers

	return nil
}

// runWorkerWithFailsafe updated to handle OnRetry, RetryIf, and callbacks
func (p *Pool[T]) runWorkerWithFailsafe(workerID int, task *TaskWrapper[T]) {
	// Create attempt-specific context
	attemptCtx, attemptCancel := context.WithCancel(task.ctx)
	defer attemptCancel() // Ensure the context is canceled when done

	// Wrap context with panicContext by default
	attemptCtx = &panicContext{Context: attemptCtx}

	// Wrap context with panicOnTimeoutContext if enabled
	if task.panicOnTimeout {
		attemptCtx = &panicOnTimeoutContext{Context: attemptCtx}
	}

	// Reset attempt-specific duration tracking
	start := time.Now()

	// If maxDuration is set, enforce it by cancelling the attempt's context when exceeded
	if task.maxDuration > 0 {
		go func() {
			select {
			case <-p.timer.After(task.maxDuration):
				attemptCancel() // Cancel the attempt's context when maxDuration is exceeded
			case <-attemptCtx.Done():
			}
		}()
	}

	// Enforce time limit for the overall task duration
	if task.timeLimit > 0 {
		// Capture the current total duration to avoid data races
		p.mu.Lock()
		currentTotalDuration := task.totalDuration
		p.mu.Unlock()

		// Pass necessary data to avoid accessing shared fields concurrently
		go func() {
			p.enforceTimeLimit(attemptCancel, task.timeLimit, currentTotalDuration, task.ctx)
		}()
	}

	var err error

	// Attempt to run the worker within a panic-catching function
	func() {
		defer func() {
			if r := recover(); r != nil {
				if p.config.panicHandler != nil {
					p.config.panicHandler(task, r)
				}
				// Also set err to indicate that a panic occurred
				err = fmt.Errorf("panic occurred in worker %d: %v", workerID, r)
			}
		}()

		// Attempt to run the worker
		p.mu.Lock()
		state, exists := p.workers[workerID]
		p.mu.Unlock()
		if !exists {
			err = fmt.Errorf("worker %d does not exist", workerID)
			return
		}
		err = state.worker.Run(attemptCtx, task.data)
	}()

	// Check for panicOnTimeoutError
	var timeoutErr *panicOnTimeoutError
	if errors.As(err, &timeoutErr) {
		// Handle the timeout error
		err = fmt.Errorf("worker %d stopped due to timeout: %w", workerID, timeoutErr)
	}

	duration := time.Since(start)

	// Safely update shared fields
	p.mu.Lock()
	task.totalDuration += duration
	task.durations = append(task.durations, duration)
	p.mu.Unlock()

	if err != nil {
		p.mu.Lock()
		task.errors = append(task.errors, err)
		p.mu.Unlock()

		if IsUnrecoverable(err) {
			p.addToDeadTasks(task, err)
			return
		}
		// Check if the error is due to time limit or max duration exceeded
		if errors.Is(err, context.DeadlineExceeded) || (errors.Is(err, context.Canceled) && duration >= task.maxDuration) {
			// Exceeded maxDuration for this attempt
			p.requeueTask(task, fmt.Errorf("task exceeded max duration of %v for attempt", task.maxDuration))
			return
		}
		if err != context.Canceled && p.config.retryIf(err) {
			p.config.onRetry(task.retries, err, task)
			if p.config.onTaskFailure != nil {
				p.mu.Lock()
				state, exists := p.workers[workerID]
				p.mu.Unlock()
				if exists {
					p.config.onTaskFailure(p, workerID, state.worker, task, err)
				}
			}
			p.requeueTask(task, err)
		} else {
			log.Printf("Task not retried due to RetryIf policy: %v\n", err)
			p.addToDeadTasks(task, err)
		}
	} else {
		if p.config.onTaskSuccess != nil {
			p.mu.Lock()
			state, exists := p.workers[workerID]
			p.mu.Unlock()
			if exists {
				p.config.onTaskSuccess(p, workerID, state.worker, task)
			}
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
		p.addToDeadTasks(task, err)
		return
	}

	// Reset the per-attempt duration for the next attempt
	task.durations = nil

	// Check if max attempts reached (unless unlimited retries)
	if p.config.attempts != UnlimitedAttempts && task.retries >= p.config.attempts {
		p.addToDeadTasks(task, err)
		return
	}

	// Calculate delay before next retry
	delay := p.calculateDelay(task.retries, err)
	task.scheduledTime = time.Now().Add(delay)

	if !task.immediateRetry {
		// Randomly select a worker that hasn't tried this task
		availableWorkers := make([]int, 0)
		for workerID := range p.workers {
			if !task.triedWorkers[workerID] {
				availableWorkers = append(availableWorkers, workerID)
			}
		}

		var selectedWorkerID int
		if len(availableWorkers) > 0 {
			selectedWorkerID = availableWorkers[rand.Intn(len(availableWorkers))]
		} else {
			// If all workers have tried, reset triedWorkers and select a random worker
			task.triedWorkers = make(map[int]bool)
			workerIDs := make([]int, 0, len(p.workers))
			for workerID := range p.workers {
				workerIDs = append(workerIDs, workerID)
			}
			selectedWorkerID = workerIDs[rand.Intn(len(workerIDs))]
		}

		q := p.taskQueues[selectedWorkerID]
		q.tasks = append(q.tasks, task) // Put at the back of the queue
		p.taskQueues[selectedWorkerID] = q
	} else {
		// Immediate retry
		if len(task.triedWorkers) < len(p.workers) {
			// Find a worker that hasn't tried this task
			for workerID := range p.workers {
				if !task.triedWorkers[workerID] {
					q := p.taskQueues[workerID]
					q.tasks = append([]*TaskWrapper[T]{task}, q.tasks...) // Put at the front of the queue
					p.taskQueues[workerID] = q
					break
				}
			}
		} else {
			// All workers have tried, reset triedWorkers and put at the back of a random worker's queue
			task.triedWorkers = make(map[int]bool)
			workerIDs := make([]int, 0, len(p.workers))
			for workerID := range p.workers {
				workerIDs = append(workerIDs, workerID)
			}
			randomWorkerID := workerIDs[rand.Intn(len(workerIDs))]
			q := p.taskQueues[randomWorkerID]
			q.tasks = append(q.tasks, task) // Put at the back of the queue
			p.taskQueues[randomWorkerID] = q
		}
	}

	p.cond.Broadcast() // Signal all workers
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
		data:           data,
		retries:        0,
		triedWorkers:   make(map[int]bool),
		errors:         make([]error, 0),
		durations:      make([]time.Duration, 0),
		ctx:            taskCtx,
		cancel:         cancel,
		immediateRetry: false,
	}
	for _, opt := range options {
		opt(task)
	}
	task.scheduledTime = time.Now()

	workerIDs := make([]int, 0, len(p.workers))
	for workerID := range p.workers {
		workerIDs = append(workerIDs, workerID)
	}

	if len(workerIDs) == 0 {
		return errors.New("no workers available")
	}

	var selectedWorkerID int
	// Find the worker with the smallest queue
	minQueueSize := int(^uint(0) >> 1) // Max int
	for _, workerID := range workerIDs {
		queueSize := len(p.taskQueues[workerID].tasks)
		if queueSize < minQueueSize {
			minQueueSize = queueSize
			selectedWorkerID = workerID
		}
	}

	q := p.taskQueues[selectedWorkerID]
	q.tasks = append(q.tasks, task)
	p.taskQueues[selectedWorkerID] = q

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
		onRetry:          func(n int, err error, task *TaskWrapper[T]) {},
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

// WithPanicHandler sets a custom panic handler for the pool.
func WithPanicHandler[T any](handler PanicHandlerFunc[T]) Option[T] {
	return func(p *Pool[T]) {
		p.config.panicHandler = handler
	}
}

// WithAttempts sets the maximum number of attempts
func WithAttempts[T any](attempts int) Option[T] {
	return func(p *Pool[T]) {
		p.config.attempts = attempts
	}
}

// WithWorkerContext creates a specific context for each worker
func WithWorkerContext[T any](fn ContextFunc) Option[T] {
	return func(p *Pool[T]) {
		p.config.contextFunc = fn
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

// WithMaxDuration TaskOption to set per-attempt max duration
func WithMaxDuration[T any](maxDuration time.Duration) TaskOption[T] {
	return func(t *TaskWrapper[T]) {
		t.maxDuration = maxDuration
	}
}

// WithTimeLimit sets a time limit for a task, considering all retries
func WithTimeLimit[T any](limit time.Duration) TaskOption[T] {
	return func(t *TaskWrapper[T]) {
		t.timeLimit = limit
	}
}

// WithImmediateRetry enables immediate retry for a task
func WithImmediateRetry[T any]() TaskOption[T] {
	return func(t *TaskWrapper[T]) {
		t.immediateRetry = true
	}
}

// WithPanicOnTimeout enables panic on timeout for a task
func WithPanicOnTimeout[T any]() TaskOption[T] {
	return func(t *TaskWrapper[T]) {
		t.panicOnTimeout = true
	}
}

// DelayTypeFunc signature
type DelayTypeFunc[T any] func(n int, err error, config *Config[T]) time.Duration

// OnRetryFunc signature
type OnRetryFunc[T any] func(attempt int, err error, task *TaskWrapper[T])

// OnTaskSuccessFunc is the type of function called when a task succeeds
type OnTaskSuccessFunc[T any] func(controller WorkerController[T], workerID int, worker Worker[T], task *TaskWrapper[T])

// OnTaskFailureFunc is the type of function called when a task fails
type OnTaskFailureFunc[T any] func(controller WorkerController[T], workerID int, worker Worker[T], task *TaskWrapper[T], err error)

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

// panicContext is a custom context that causes a panic when Err() is called after cancellation.
type panicContext struct {
	context.Context
}

func (p *panicContext) Err() error {
	if err := p.Context.Err(); err != nil {
		panic(&panicError{cause: err})
	}
	return nil
}

// panicError is an error type that represents a panic caused by context cancellation.
type panicError struct {
	cause error
}

func (e *panicError) Error() string {
	return fmt.Sprintf("panic due to context cancellation: %v", e.cause)
}

func (e *panicError) Unwrap() error {
	return e.cause
}

// panicOnTimeoutContext is a custom context that returns a specific error on timeout
type panicOnTimeoutContext struct {
	context.Context
}

func (p *panicOnTimeoutContext) Err() error {
	if err := p.Context.Err(); err != nil {
		return &panicOnTimeoutError{cause: err}
	}
	return nil
}

// panicOnTimeoutError indicates that the context was canceled due to a timeout
type panicOnTimeoutError struct {
	cause error
}

func (e *panicOnTimeoutError) Error() string {
	return fmt.Sprintf("context canceled due to timeout: %v", e.cause)
}

func (e *panicOnTimeoutError) Unwrap() error {
	return e.cause
}

// PanicHandlerFunc is the type of function called when a panic occurs in a task.
type PanicHandlerFunc[T any] func(task *TaskWrapper[T], v interface{})
