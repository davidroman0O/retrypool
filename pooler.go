package retrypool

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/davidroman0O/retrypool/logs"
)

// Predefined errors
var (
	ErrPoolClosed           = errors.New("pool is closed")
	ErrRateLimitExceeded    = errors.New("rate limit exceeded")
	ErrNoWorkersAvailable   = errors.New("no workers available")
	ErrInvalidWorkerID      = errors.New("invalid worker ID")
	ErrTaskExceedsTimeLimit = errors.New("task exceeds total time limit")
	ErrWorkerNotInterrupted = errors.New("worker is not interrupted and cannot be restarted")
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

	beingProcessed processedNotification // optional

	queuedAt    []time.Time
	processedAt []time.Time
}

func (t *TaskWrapper[T]) QueuedAt() []time.Time {
	return t.queuedAt
}

func (t *TaskWrapper[T]) ProcessedAt() []time.Time {
	return t.processedAt
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
	attempts      int
	delay         time.Duration
	maxDelay      time.Duration
	maxJitter     time.Duration
	onRetry       OnRetryFunc[T]
	retryIf       RetryIfFunc
	delayType     DelayTypeFunc[T]
	lastErrorOnly bool
	context       context.Context

	onTaskSuccess OnTaskSuccessFunc[T] // Callback when a task succeeds
	onTaskFailure OnTaskFailureFunc[T] // Callback when a task fails
	onNewDeadTask OnNewDeadTaskFunc[T]

	contextFunc ContextFunc

	panicHandler PanicHandlerFunc[T]
	panicWorker  PanicWorker

	logLevel logs.Level
}

// workerState holds all per-worker data
type workerState[T any] struct {
	worker      Worker[T]
	stopChan    chan struct{}
	doneChan    chan struct{}
	cancel      context.CancelFunc
	ctx         context.Context
	forcePanic  bool
	currentTask *TaskWrapper[T] // Field to track the current task
	interrupted bool            // Field to track if the worker has been interrupted
	removed     bool            // Field to track if the worker has been removed
}

// Metrics struct holds atomic counters for pool metrics
type Metrics struct {
	TasksSubmitted int64
	TasksProcessed int64
	TasksSucceeded int64
	TasksFailed    int64
	DeadTasks      int64
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
	stopped         bool
	closed          bool
	ctx             context.Context
	cancel          context.CancelFunc
	deadTasks       []DeadTask[T]
	deadTasksMutex  sync.Mutex

	config Config[T]

	metrics Metrics

	limiter *rate.Limiter

	errGroup *errgroup.Group
}

// New initializes the Pool with given workers and options
func New[T any](ctx context.Context, workers []Worker[T], options ...Option[T]) *Pool[T] {
	// Create a context that can be canceled
	ctx, cancel := context.WithCancel(ctx)

	// Create an errgroup with the context
	errGroup, ctx := errgroup.WithContext(ctx)

	pool := &Pool[T]{
		workers:         make(map[int]*workerState[T]),
		nextWorkerID:    0,
		workersToRemove: make(map[int]bool),
		taskQueues:      make(map[int]taskQueue[T]),
		config:          newDefaultConfig[T](),
		ctx:             ctx,
		cancel:          cancel,
		errGroup:        errGroup,
	}

	for _, option := range options {
		option(pool)
	}

	pool.cond = sync.NewCond(&pool.mu)

	// Initialize logger if not already initialized
	if logs.Log == nil {
		logs.Initialize(pool.config.logLevel)
	}

	// Initialize workers with unique IDs
	for _, worker := range workers {
		pool.AddWorker(worker)
	}

	logs.Debug(context.Background(), "Pool created", "workers", len(pool.workers))

	return pool
}

// Close gracefully shuts down the pool, conforming to the io.Closer interface
func (p *Pool[T]) Close() error {
	logs.Debug(context.Background(), "Closing the pool")
	if err := p.Shutdown(); err != nil {
		logs.Error(context.Background(), "Failed to shutdown pool", "error", err)
		return fmt.Errorf("failed to shutdown pool: %w", err)
	}
	logs.Debug(context.Background(), "Pool has been closed")
	return nil
}

// Shutdown stops the pool and waits for all workers to finish
func (p *Pool[T]) Shutdown() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.stopped = true
	p.cancel()
	p.mu.Unlock()

	logs.Info(context.Background(), "Pool is shutting down")

	p.cond.Broadcast()

	// Wait for all workers to finish
	err := p.errGroup.Wait()
	if err != nil {
		if err != context.Canceled {
			logs.Error(context.Background(), "Error while waiting for workers to finish", "error", err)
		} else {
			logs.Info(context.Background(), "Pool has been shut down")
		}
	} else {
		logs.Info(context.Background(), "All workers have been shut down")
	}
	return err
}

// Metrics returns a snapshot of the current metrics
func (p *Pool[T]) Metrics() Metrics {
	return Metrics{
		TasksSubmitted: atomic.LoadInt64(&p.metrics.TasksSubmitted),
		TasksProcessed: atomic.LoadInt64(&p.metrics.TasksProcessed),
		TasksSucceeded: atomic.LoadInt64(&p.metrics.TasksSucceeded),
		TasksFailed:    atomic.LoadInt64(&p.metrics.TasksFailed),
		DeadTasks:      atomic.LoadInt64(&p.metrics.DeadTasks),
	}
}

type WorkerItem[T any] struct {
	Worker Worker[T]
	ID     int
}

func (p *Pool[T]) ListWorkers() []WorkerItem[T] {
	p.mu.Lock()
	defer p.mu.Unlock()

	workers := make([]WorkerItem[T], 0, len(p.workers))
	for id, state := range p.workers {
		workers = append(workers, WorkerItem[T]{Worker: state.worker, ID: id})
	}
	return workers
}

// AddWorker adds a new worker to the pool dynamically
func (p *Pool[T]) AddWorker(worker Worker[T]) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	workerID := p.nextWorkerID
	p.nextWorkerID++

	var workerCtx context.Context
	var workerCancel context.CancelFunc
	if p.config.contextFunc != nil {
		workerCtx = p.config.contextFunc()
	} else {
		workerCtx, workerCancel = context.WithCancel(p.ctx)
	}
	state := &workerState[T]{
		worker:   worker,
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),
		cancel:   workerCancel,
		ctx:      workerCtx,
	}

	p.workers[workerID] = state

	// Start the worker goroutine using errgroup
	p.errGroup.Go(func() error {
		return p.workerLoop(workerID)
	})

	logs.Info(context.Background(), "Added new worker", "workerID", workerID)

	return workerID
}

func (p *Pool[T]) GetRandomWorkerID() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	workerIDs := make([]int, 0, len(p.workers))
	for workerID := range p.workers {
		workerIDs = append(workerIDs, workerID)
	}
	if len(workerIDs) == 0 {
		return -1
	}
	return workerIDs[rand.Intn(len(workerIDs))]
}

func (p *Pool[T]) GetWorkerCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.workers)
}

func (p *Pool[T]) GetWorkerIDs() []int {
	p.mu.Lock()
	defer p.mu.Unlock()

	workerIDs := make([]int, 0, len(p.workers))
	for workerID := range p.workers {
		workerIDs = append(workerIDs, workerID)
	}
	return workerIDs
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
		return fmt.Errorf("%w: worker %d does not exist", ErrInvalidWorkerID, workerID)
	}

	if !state.interrupted {
		return fmt.Errorf("%w: worker %d", ErrWorkerNotInterrupted, workerID)
	}

	// Create a new context for the worker
	workerCtx, workerCancel := context.WithCancel(p.ctx)
	state.ctx = workerCtx
	state.cancel = workerCancel
	state.forcePanic = false
	state.interrupted = false

	// Create a new stopChan and doneChan
	state.stopChan = make(chan struct{})
	state.doneChan = make(chan struct{})

	// Start a new goroutine for the worker
	p.errGroup.Go(func() error {
		return p.workerLoop(workerID)
	})

	logs.Info(context.Background(), "Worker has been restarted", "workerID", workerID)
	return nil
}

// RemoveWorker removes a worker from the pool
// RemoveWorker removes a worker from the pool
func (p *Pool[T]) RemoveWorker(workerID int) error {
	p.mu.Lock()

	state, exists := p.workers[workerID]
	if !exists {
		p.mu.Unlock()
		return fmt.Errorf("%w: worker %d does not exist", ErrInvalidWorkerID, workerID)
	}

	// Set the removed flag
	state.removed = true

	// Cancel the worker's context
	state.cancel()

	// Close the worker's stop channel
	close(state.stopChan)

	logs.Info(context.Background(), "Removing worker", "workerID", workerID)

	// Signal the worker to wake up if it's waiting
	p.cond.Broadcast()

	p.mu.Unlock() // Release lock while waiting

	// Requeue any tasks assigned to this worker
	p.requeueTasksFromWorker(workerID)

	return nil
}

// requeueTasksFromWorker reassigns tasks from the removed worker to other workers
func (p *Pool[T]) requeueTasksFromWorker(workerID int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Remove the worker's triedWorkers from all tasks
	for retries, queue := range p.taskQueues {
		newTasks := make([]*TaskWrapper[T], 0, len(queue.tasks))
		for _, task := range queue.tasks {
			if task.triedWorkers != nil {
				delete(task.triedWorkers, workerID)
			}
			task.queuedAt = append(task.queuedAt, time.Now())
			newTasks = append(newTasks, task)
		}
		p.taskQueues[retries] = taskQueue[T]{tasks: newTasks}
	}

	// Signal workers that the task queues have changed
	p.cond.Broadcast()
}

// workerLoop handles the lifecycle of a worker
func (p *Pool[T]) workerLoop(workerID int) error {
	logs.Debug(context.Background(), "Worker loop started", "workerID", workerID)

	p.mu.Lock()
	state, exists := p.workers[workerID]
	p.mu.Unlock()

	if !exists {
		return fmt.Errorf("%w: worker %d does not exist", ErrInvalidWorkerID, workerID)
	}

	defer func() {
		p.mu.Lock()
		defer p.mu.Unlock()

		if state.removed {
			// Remove worker from the pool when it exits
			delete(p.workers, workerID)
			logs.Debug(context.Background(), "Worker loop exited and removed", "workerID", workerID)
		} else {
			logs.Debug(context.Background(), "Worker loop exited but not removed", "workerID", workerID)
		}
	}()

	if !exists {
		return fmt.Errorf("%w: worker %d does not exist", ErrInvalidWorkerID, workerID)
	}

	for {
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		case <-state.ctx.Done():
			return state.ctx.Err()
		default:
		}

		p.mu.Lock()
		now := time.Now()
		retries, idx, task, ok := p.getNextTask(workerID)
		if !ok {
			// No immediate tasks; find the next scheduled task time
			nextScheduledTime := time.Time{}
			hasTask := false
			for _, q := range p.taskQueues {
				for _, t := range q.tasks {
					if t.triedWorkers == nil {
						t.triedWorkers = make(map[int]bool)
					}
					if !t.triedWorkers[workerID] {
						if !hasTask || t.scheduledTime.Before(nextScheduledTime) {
							nextScheduledTime = t.scheduledTime
							hasTask = true
						}
					}
				}
			}

			if !hasTask && p.stopped {
				p.mu.Unlock()
				return nil
			}

			if !hasTask {
				// No tasks at all; wait for new tasks
				logs.Debug(context.Background(), "Worker is waiting for tasks", "workerID", workerID)
				p.cond.Wait()
				p.mu.Unlock()
				continue
			}

			// Wait until the next task is scheduled
			waitDuration := nextScheduledTime.Sub(now)
			p.mu.Unlock()
			select {
			case <-time.After(waitDuration):
			case <-p.ctx.Done():
				return p.ctx.Err()
			}
			continue
		}

		now = time.Now()
		if now.Before(task.scheduledTime) {
			// Task is scheduled in the future; put it back and wait
			// Since getNextTask has removed it, we need to reinsert it
			p.reinsertTask(retries, idx, task)
			waitDuration := task.scheduledTime.Sub(now)
			p.mu.Unlock()
			select {
			case <-time.After(waitDuration):
			case <-p.ctx.Done():
				return p.ctx.Err()
			}
			continue
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
		if p.ctx.Err() != nil {
			p.mu.Lock()
			p.processing--
			p.mu.Unlock()
			return p.ctx.Err()
		}

		logs.Debug(context.Background(), "Worker is processing task", "workerID", workerID, "taskData", task.data)

		p.runWorkerWithFailsafe(workerID, task)

		p.mu.Lock()
		if p.workersToRemove[workerID] {
			delete(p.workersToRemove, workerID)
			p.mu.Unlock()
			go p.RemoveWorker(workerID)
			return nil
		}

		// Unset the current task
		state.currentTask = nil

		if state.interrupted {
			p.processing--
			p.mu.Unlock()
			return nil // Exit the loop if the worker was interrupted
		}

		p.processing--
		p.cond.Signal()
		p.mu.Unlock()
	}
}

func (p *Pool[T]) reinsertTask(retries, idx int, task *TaskWrapper[T]) {
	// Re-insert task into the queue at the same position
	q := p.taskQueues[retries]
	q.tasks = append(q.tasks[:idx], append([]*TaskWrapper[T]{task}, q.tasks[idx:]...)...)
	p.taskQueues[retries] = q
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
	now := time.Now()

	// First, check for immediate retry tasks this worker hasn't tried and are ready to process
	for retries, q := range p.taskQueues {
		for idx, task := range q.tasks {
			if task.immediateRetry && !task.triedWorkers[workerID] && !task.scheduledTime.After(now) {
				// Remove the task from the queue
				q.tasks = append(q.tasks[:idx], q.tasks[idx+1:]...)
				if len(q.tasks) == 0 {
					delete(p.taskQueues, retries)
				} else {
					p.taskQueues[retries] = q
				}
				return retries, idx, task, true
			}
		}
	}

	// Then, check for any task this worker hasn't tried and are ready to process
	for retries, q := range p.taskQueues {
		for idx, task := range q.tasks {
			if task.triedWorkers == nil {
				task.triedWorkers = make(map[int]bool)
			}
			if !task.triedWorkers[workerID] && !task.scheduledTime.After(now) {
				// Remove the task from the queue
				q.tasks = append(q.tasks[:idx], q.tasks[idx+1:]...)
				if len(q.tasks) == 0 {
					delete(p.taskQueues, retries)
				} else {
					p.taskQueues[retries] = q
				}
				return retries, idx, task, true
			}
		}
	}

	// If no tasks are ready to process, return no task
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

func (p *Pool[T]) RangeTasks(cb func(data TaskWrapper[T], workerID int, status TaskStatus) bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Create a copy of the workers map to avoid concurrent modification issues
	workersCopy := make(map[int]*workerState[T], len(p.workers))
	for workerID, state := range p.workers {
		workersCopy[workerID] = state
	}

	// Iterate over tasks currently being processed
	for workerID, state := range workersCopy {
		if state.currentTask != nil {
			if !cb(*state.currentTask, workerID, TaskStatusProcessing) {
				return false
			}
		}
	}

	// Create a copy of the taskQueues map to avoid concurrent modification issues
	taskQueuesCopy := make(map[int]taskQueue[T], len(p.taskQueues))
	for retries, queue := range p.taskQueues {
		taskQueuesCopy[retries] = queue
	}

	// Iterate over tasks in the queues
	for retries, queue := range taskQueuesCopy {
		for _, task := range queue.tasks {
			if !cb(*task, retries, TaskStatusQueued) {
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
		return fmt.Errorf("%w: worker %d does not exist", ErrInvalidWorkerID, workerID)
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

	logs.Debug(context.Background(), "Interrupting worker", "workerID", workerID, "options", cfg)

	// Set the forcePanic if requested
	if cfg.ForcePanic {
		state.forcePanic = true
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
			logs.Debug(context.Background(), "Task removed due to interrupt", "workerID", workerID, "taskData", task.data)
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

			logs.Debug(context.Background(), "Task reassigned due to interrupt", "workerID", workerID, "taskData", task.data)
		}
	}

	if cfg.RemoveWorker {
		// Remove the worker
		logs.Debug(context.Background(), "Worker will be removed after interrupt", "workerID", workerID)
		err := p.RemoveWorker(workerID)
		if err != nil {
			return fmt.Errorf("failed to remove worker %d: %w", workerID, err)
		}
	} else {
		state.interrupted = true

		if cfg.Restart {
			p.mu.Unlock()
			err := p.RestartWorker(workerID)
			p.mu.Lock()
			if err != nil {
				return fmt.Errorf("failed to restart worker %d: %w", workerID, err)
			}
		}
	}

	p.cond.Broadcast() // Signal all workers

	return nil
}

// runWorkerWithFailsafe handles the execution of a task, including panic recovery and retries
func (p *Pool[T]) runWorkerWithFailsafe(workerID int, task *TaskWrapper[T]) {
	// Create attempt-specific context
	attemptCtx, attemptCancel := p.createAttemptContext(task)
	if attemptCtx == nil {
		err := fmt.Errorf("%w: %v", ErrTaskExceedsTimeLimit, task.timeLimit)
		p.addToDeadTasks(task, err)
		return
	}
	defer attemptCancel()

	// Reset attempt-specific duration tracking
	start := time.Now()

	// Notify the task that it's being processed
	if task.beingProcessed != nil {
		task.beingProcessed <- struct{}{}
	}

	// Collect when the task was processed
	task.processedAt = append(task.processedAt, start)

	var err error

	// Increment tasks processed metric
	atomic.AddInt64(&p.metrics.TasksProcessed, 1)

	// Attempt to run the worker within a panic-catching function
	func() {
		defer func() {
			if r := recover(); r != nil {
				// Capture the stack trace
				buf := make([]byte, 4096)
				n := runtime.Stack(buf, false)
				stackTrace := string(buf[:n])

				// Create a concise error message
				err = fmt.Errorf("panic occurred in worker %d: %v", workerID, r)

				if p.config.panicHandler != nil {
					// Call the panic handler with the task, panic error, and stack trace
					p.config.panicHandler(task.Data(), r, stackTrace)
				}

				// Log the panic
				logs.Error(context.Background(), "Panic in worker", "workerID", workerID, "error", err, "stackTrace", stackTrace)
			}
		}()

		// Attempt to run the worker
		p.mu.Lock()
		state, exists := p.workers[workerID]
		p.mu.Unlock()
		if !exists {
			err = fmt.Errorf("%w: worker %d does not exist", ErrInvalidWorkerID, workerID)
			return
		}
		err = state.worker.Run(attemptCtx, task.data)
	}()

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

		// Increment tasks failed metric
		atomic.AddInt64(&p.metrics.TasksFailed, 1)

		// Check if the error is due to time limit or max duration exceeded
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			// Exceeded maxDuration or time limit
			logs.Warn(context.Background(), "Task exceeded time limit or was canceled", "workerID", workerID, "error", err)
			p.requeueTask(task, err, false)
			return
		}

		var action DeadTaskAction = DeadTaskActionRetry // Default action

		if p.config.onTaskFailure != nil {
			p.mu.Lock()
			state, exists := p.workers[workerID]
			p.mu.Unlock()
			if exists {
				action = p.config.onTaskFailure(p, workerID, state.worker, task, err)
			}
		}

		switch action {
		case DeadTaskActionAddToDeadTasks:
			if task.beingProcessed != nil {
				task.beingProcessed.Close()
			}
			p.addToDeadTasks(task, err)
		case DeadTaskActionRetry:
			if err != context.Canceled && err != context.DeadlineExceeded && p.config.retryIf(err) && task.retries < p.config.attempts {
				logs.Debug(context.Background(), "Retrying task", "workerID", workerID, "attempt", task.retries, "error", err)
				p.config.onRetry(task.retries, err, task)
				p.requeueTask(task, err, false)
			} else {
				if task.beingProcessed != nil {
					task.beingProcessed.Close()
				}
				p.addToDeadTasks(task, err)
			}
		case DeadTaskActionForceRetry:
			logs.Debug(context.Background(), "Force retrying task", "workerID", workerID, "attempt", task.retries, "error", err)
			p.config.onRetry(task.retries, err, task)
			p.requeueTask(task, err, true)
		case DeadTaskActionDoNothing:
			// Do nothing, as requested
		}
	} else {
		// Increment tasks succeeded metric
		atomic.AddInt64(&p.metrics.TasksSucceeded, 1)

		if p.config.onTaskSuccess != nil {
			p.mu.Lock()
			state, exists := p.workers[workerID]
			p.mu.Unlock()
			if exists {
				p.config.onTaskSuccess(p, workerID, state.worker, task)
			}
		}

		logs.Debug(context.Background(), "Task completed successfully", "workerID", workerID, "taskData", task.data)

		if task.beingProcessed != nil {
			task.beingProcessed.Close()
		}
	}
}

// createAttemptContext creates the context for an attempt, considering maxDuration and timeLimit
func (p *Pool[T]) createAttemptContext(task *TaskWrapper[T]) (context.Context, context.CancelFunc) {
	var attemptCtx context.Context
	var attemptCancel context.CancelFunc
	var remainingTime time.Duration
	if task.timeLimit > 0 {
		p.mu.Lock()
		currentTotalDuration := task.totalDuration
		p.mu.Unlock()
		remainingTime = task.timeLimit - currentTotalDuration
		if remainingTime <= 0 {
			// Time limit already exceeded
			return nil, func() {}
		}
	} else {
		remainingTime = 0
	}

	if task.maxDuration > 0 {
		if remainingTime > 0 {
			// Both timeLimit and maxDuration are set, take the minimum
			var minDuration time.Duration
			if task.maxDuration < remainingTime {
				minDuration = task.maxDuration
			} else {
				minDuration = remainingTime
			}
			attemptCtx, attemptCancel = context.WithTimeout(task.ctx, minDuration)
		} else {
			attemptCtx, attemptCancel = context.WithTimeout(task.ctx, task.maxDuration)
		}
	} else if remainingTime > 0 {
		attemptCtx, attemptCancel = context.WithTimeout(task.ctx, remainingTime)
	} else {
		attemptCtx, attemptCancel = context.WithCancel(task.ctx)
	}

	return attemptCtx, attemptCancel
}

// requeueTask updated to handle delays and keep triedWorkers intact
func (p *Pool[T]) requeueTask(task *TaskWrapper[T], err error, forceRetry bool) {
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
	if !forceRetry && p.config.attempts != UnlimitedAttempts && task.retries >= p.config.attempts {
		p.addToDeadTasks(task, err)
		return
	}

	task.queuedAt = append(task.queuedAt, time.Now())

	// Calculate delay before next retry
	delay := p.calculateDelay(task.retries, err)
	task.scheduledTime = time.Now().Add(delay)

	logs.Debug(context.Background(), "Requeuing task", "taskData", task.data, "retries", task.retries, "delay", delay)

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
	p.deadTasksMutex.Lock()
	defer p.deadTasksMutex.Unlock()

	totalDuration := task.totalDuration
	for _, duration := range task.durations {
		totalDuration += duration
	}
	errors := make([]error, len(task.errors))
	copy(errors, task.errors)
	if finalError != nil && (len(errors) == 0 || finalError.Error() != errors[len(errors)-1].Error()) {
		errors = append(errors, finalError)
	}

	deadTask := DeadTask[T]{
		Data:          task.data,
		Retries:       task.retries,
		TotalDuration: totalDuration,
		Errors:        errors,
	}

	p.deadTasks = append(p.deadTasks, deadTask)

	logs.Warn(context.Background(), "Task added to dead tasks", "taskData", task.data, "retries", task.retries, "errors", errors)

	// Increment dead tasks metric
	atomic.AddInt64(&p.metrics.DeadTasks, 1)

	if p.config.onNewDeadTask != nil {
		p.config.onNewDeadTask(&deadTask)
	}
}

// Submit adds a new task to the pool
func (p *Pool[T]) Submit(data T, options ...TaskOption[T]) error {
	if p.limiter != nil {
		// Wait for rate limiter
		if err := p.limiter.Wait(p.ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return ErrPoolClosed
			}
			return fmt.Errorf("%w: %v", ErrRateLimitExceeded, err)
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stopped {
		return ErrPoolClosed
	}

	// Increment tasks submitted metric
	atomic.AddInt64(&p.metrics.TasksSubmitted, 1)

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
		processedAt:    []time.Time{},
		queuedAt:       []time.Time{},
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
		return ErrNoWorkersAvailable
	}

	// Find the worker with the smallest queue
	minQueueSize := int(^uint(0) >> 1) // Max int
	var selectedWorkerID int
	for _, workerID := range workerIDs {
		queueSize := len(p.taskQueues[workerID].tasks)
		if queueSize < minQueueSize {
			minQueueSize = queueSize
			selectedWorkerID = workerID
		}
	}

	task.queuedAt = append(task.queuedAt, time.Now())

	q := p.taskQueues[selectedWorkerID]
	q.tasks = append(q.tasks, task)
	p.taskQueues[selectedWorkerID] = q

	logs.Debug(context.Background(), "Task submitted", "workerID", selectedWorkerID, "taskData", task.data)

	// Signal all waiting workers that there's a new task
	p.cond.Broadcast()

	return nil
}

// PullDeadTask removes and returns a dead task from the pool
func (p *Pool[T]) PullDeadTask(idx int) (*DeadTask[T], error) {
	p.deadTasksMutex.Lock()
	defer p.deadTasksMutex.Unlock()

	if idx < 0 || idx >= len(p.deadTasks) {
		return nil, fmt.Errorf("invalid dead task index: %d", idx)
	}

	deadTask := p.deadTasks[idx]
	p.deadTasks = append(p.deadTasks[:idx], p.deadTasks[idx+1:]...)

	return &deadTask, nil
}

// DeadTasks returns a copy of the dead tasks list
func (p *Pool[T]) DeadTasks() []DeadTask[T] {
	p.deadTasksMutex.Lock()
	defer p.deadTasksMutex.Unlock()
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

// ForceClose stops the pool without waiting for tasks to complete
func (p *Pool[T]) ForceClose() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.stopped = true
	for k := range p.taskQueues {
		q := p.taskQueues[k]
		q.tasks = nil
		p.taskQueues[k] = q
	}
	p.mu.Unlock()
	p.cancel()
	logs.Warn(context.Background(), "Force closing the pool, tasks will not be completed")
	p.cond.Broadcast()
}

// DeadTaskCount returns the number of dead tasks
func (p *Pool[T]) DeadTaskCount() int {
	p.deadTasksMutex.Lock()
	defer p.deadTasksMutex.Unlock()
	return len(p.deadTasks)
}

// WaitWithCallback waits for the pool to complete while calling a callback function
func (p *Pool[T]) WaitWithCallback(ctx context.Context, callback func(queueSize, processingCount, deadTaskCount int) bool, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if !callback(p.QueueSize(), p.ProcessingCount(), p.DeadTaskCount()) {
				return nil
			}
		}
	}
}

// newDefaultConfig initializes default retry configurations
func newDefaultConfig[T any]() Config[T] {
	return Config[T]{
		attempts:  10,
		delay:     100 * time.Millisecond,
		maxJitter: 100 * time.Millisecond,
		onRetry:   func(n int, err error, task *TaskWrapper[T]) {},
		retryIf: func(err error) bool {
			return err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
		},
		delayType:     ExponentialBackoffWithJitter[T],
		lastErrorOnly: false,
		context:       context.Background(),
		onTaskSuccess: nil, // Default is nil; can be set via options
		onTaskFailure: nil, // Default is nil; can be set via options

		logLevel: logs.LevelError,
	}
}

// Option functions for configuring the Pool

// WithPanicHandler sets a custom panic handler for the pool.
func WithPanicHandler[T any](handler PanicHandlerFunc[T]) Option[T] {
	return func(p *Pool[T]) {
		p.config.panicHandler = handler
	}
}

// WithPanicWorker sets a custom panic handler for the pool.
func WithPanicWorker[T any](handler PanicWorker) Option[T] {
	return func(p *Pool[T]) {
		p.config.panicWorker = handler
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

// WithLogLevel sets the log level for the pool
func WithLogLevel[T any](level logs.Level) Option[T] {
	return func(p *Pool[T]) {
		p.config.logLevel = level
	}
}

// WithContext sets the context for the Pool
func WithContext[T any](ctx context.Context) Option[T] {
	return func(p *Pool[T]) {
		p.config.context = ctx
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

// WithOnNewDeadTask is a new option for handling new dead tasks
func WithOnNewDeadTask[T any](onNewDeadTask OnNewDeadTaskFunc[T]) Option[T] {
	return func(p *Pool[T]) {
		p.config.onNewDeadTask = onNewDeadTask
	}
}

// WithRateLimit sets the rate limit for task submissions
func WithRateLimit[T any](rps float64) Option[T] {
	return func(p *Pool[T]) {
		p.limiter = rate.NewLimiter(rate.Limit(rps), 1)
	}
}

// TaskOption functions for configuring individual tasks

// WithMaxContextDuration TaskOption to set per-attempt max duration
func WithMaxContextDuration[T any](maxDuration time.Duration) TaskOption[T] {
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

type processedNotification chan struct{}

func NewProcessedNotification() processedNotification {
	return make(chan struct{}, 1)
}

func (p processedNotification) Close() {
	if p != nil {
		close(p)
	}
}

// WithBeingProcessed sets a channel to indicate that a task is being processed after dispatch
func WithBeingProcessed[T any](chn processedNotification) TaskOption[T] {
	return func(t *TaskWrapper[T]) {
		t.beingProcessed = chn
	}
}

// DelayTypeFunc signature
type DelayTypeFunc[T any] func(n int, err error, config *Config[T]) time.Duration

// OnRetryFunc signature
type OnRetryFunc[T any] func(attempt int, err error, task *TaskWrapper[T])

// DeadTaskAction represents the action to take for a failed task
type DeadTaskAction int

const (
	DeadTaskActionRetry DeadTaskAction = iota
	DeadTaskActionAddToDeadTasks
	DeadTaskActionDoNothing
	DeadTaskActionForceRetry
)

// OnTaskSuccessFunc is the type of function called when a task succeeds
type OnTaskSuccessFunc[T any] func(controller WorkerController[T], workerID int, worker Worker[T], task *TaskWrapper[T])

// OnTaskFailureFunc is the type of function called when a task fails
type OnTaskFailureFunc[T any] func(controller WorkerController[T], workerID int, worker Worker[T], task *TaskWrapper[T], err error) DeadTaskAction

// OnNewDeadTaskFunc is a new type for handling new dead tasks
type OnNewDeadTaskFunc[T any] func(task *DeadTask[T])

// RetryIfFunc signature
type RetryIfFunc func(error) bool

// ContextFunc signature
type ContextFunc func() context.Context

// DelayType functions

// ExponentialBackoffWithJitter implements exponential backoff with jitter
func ExponentialBackoffWithJitter[T any](n int, _ error, config *Config[T]) time.Duration {
	baseDelay := config.delay
	maxDelay := config.maxDelay

	// Calculate exponential backoff
	delay := baseDelay * (1 << n)
	if delay > maxDelay {
		delay = maxDelay
	}

	// Add jitter
	jitter := time.Duration(rand.Int63n(int64(config.maxJitter)))
	return delay + jitter
}

// Constants
const UnlimitedAttempts = -1

// PanicHandlerFunc is the type of function called when a panic occurs in a task.
type PanicHandlerFunc[T any] func(task T, v interface{}, stackTrace string)

type PanicWorker func(worker int, recovery any, err error, stackTrace string)

// RequestResponse manages the lifecycle of a task request and its response
type RequestResponse[T any, R any] struct {
	Request     T             // The request data
	done        chan struct{} // Channel to signal completion
	response    R             // Stores the successful response
	err         error         // Stores any error that occurred
	mu          sync.Mutex    // Protects response and err
	isCompleted bool          // Indicates if request is completed
}

// NewRequestResponse creates a new RequestResponse instance
func NewRequestResponse[T any, R any](request T) *RequestResponse[T, R] {
	return &RequestResponse[T, R]{
		Request: request,
		done:    make(chan struct{}),
	}
}

// Complete safely marks the request as complete with a response
func (rr *RequestResponse[T, R]) Complete(response R) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if !rr.isCompleted {
		rr.response = response
		rr.isCompleted = true
		close(rr.done)
	}
}

// CompleteWithError safely marks the request as complete with an error
func (rr *RequestResponse[T, R]) CompleteWithError(err error) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if !rr.isCompleted {
		rr.err = err
		rr.isCompleted = true
		close(rr.done)
	}
}

// Done returns a channel that's closed when the request is complete
func (rr *RequestResponse[T, R]) Done() <-chan struct{} {
	return rr.done
}

// Wait waits for the request to complete and returns the response and any error
func (rr *RequestResponse[T, R]) Wait(ctx context.Context) (R, error) {
	select {
	case <-rr.done:
		rr.mu.Lock()
		defer rr.mu.Unlock()
		return rr.response, rr.err
	case <-ctx.Done():
		rr.mu.Lock()
		defer rr.mu.Unlock()
		var zero R
		if !rr.isCompleted {
			rr.err = ctx.Err()
			rr.isCompleted = true
			close(rr.done)
		}
		return zero, rr.err
	}
}

// Config getters
func (c *Config[T]) Attempts() int {
	return c.attempts
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
