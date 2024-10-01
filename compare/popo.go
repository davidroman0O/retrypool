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

// Hashable is the interface used to get the hash code of a value.
type Hashable interface {
	Hashcode() interface{}
}

// hashcode returns the hashcode used for set elements.
func hashcode(v interface{}) interface{} {
	if h, ok := v.(Hashable); ok {
		return h.Hashcode()
	}
	return v
}

// Worker interface for task processing
type Worker[T Hashable] interface {
	Run(ctx context.Context, data T) error
}

// DeadTask struct to hold failed task information
type DeadTask[T Hashable] struct {
	Data          T
	Retries       int32
	TotalDuration time.Duration
	Errors        []error
}

// TaskWrapper includes scheduledTime, triedWorkers, errors, durations, and panicOnTimeout
type TaskWrapper[T Hashable] struct {
	data           T
	retries        atomic.Int32
	totalDuration  atomic.Int64 // Store duration as int64 nanoseconds
	timeLimit      atomic.Int64
	maxDuration    atomic.Int64
	scheduledTime  atomic.Value // Store time.Time
	triedWorkers   *SafeMap[int, bool]
	errors         *SafeSlice[error]
	durations      *SafeSlice[time.Duration]
	ctx            context.Context
	cancel         context.CancelFunc
	immediateRetry atomic.Bool
	panicOnTimeout atomic.Bool
	mu             sync.Mutex // For fields that can't use atomic operations
}

func (t *TaskWrapper[T]) Data() T {
	return t.data
}

func (t *TaskWrapper[T]) Retries() int32 {
	return t.retries.Load()
}

func (t *TaskWrapper[T]) TotalDuration() time.Duration {
	return time.Duration(t.totalDuration.Load())
}

func (t *TaskWrapper[T]) TimeLimit() time.Duration {
	return time.Duration(t.timeLimit.Load())
}

func (t *TaskWrapper[T]) ScheduledTime() time.Time {
	return t.scheduledTime.Load().(time.Time)
}

func (t *TaskWrapper[T]) TriedWorkers() *SafeMap[int, bool] {
	return t.triedWorkers
}

func (t *TaskWrapper[T]) Errors() []error {
	return t.errors.Items()
}

func (t *TaskWrapper[T]) Durations() []time.Duration {
	return t.durations.Items()
}

func (t *TaskWrapper[T]) IncrementRetries() {
	t.retries.Add(1)
}

func (t *TaskWrapper[T]) AddDuration(d time.Duration) {
	t.totalDuration.Add(int64(d))
}

func (t *TaskWrapper[T]) SetScheduledTime(tm time.Time) {
	t.scheduledTime.Store(tm)
}

// taskQueue stores pointers to TaskWrapper
type taskQueue[T Hashable] struct {
	tasks *SafeSlice[*TaskWrapper[T]]
}

// Option type for configuring the Pool
type Option[T Hashable] func(*Pool[T])

// TaskOption type for configuring individual tasks
type TaskOption[T Hashable] func(*TaskWrapper[T])

// Config struct to hold retry configurations
type Config[T Hashable] struct {
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
type workerState[T Hashable] struct {
	worker         Worker[T]
	stopChan       chan struct{}
	doneChan       chan struct{}
	cancel         context.CancelFunc
	ctx            context.Context
	forcePanicFlag *atomic.Bool
	currentTask    *atomic.Value // Holds *TaskWrapper[T]
	interrupted    *atomic.Bool  // Indicates if the worker has been interrupted
	forceRemove    *atomic.Bool  // Indicates if the worker should be removed
}

// Pool struct updated to include Config and support dynamic worker management
type Pool[T Hashable] struct {
	workers         *SafeMap[int, *workerState[T]] // Map of workers with unique worker IDs
	nextWorkerID    *atomic.Int64                  // Counter for assigning unique worker IDs
	workersToRemove *SafeMap[int, bool]
	taskQueues      *SafeMap[int, *taskQueue[T]]
	processing      *atomic.Int64
	cond            *sync.Cond
	wg              sync.WaitGroup
	stopped         *atomic.Bool
	closed          *atomic.Bool
	ctx             context.Context

	tasks          *SafeMap[interface{}, *TaskWrapper[T]] // Map of hashKey to TaskWrapper
	taskProcessing *SafeMap[interface{}, int]             // Map of hashKey to workerID

	deadTasks   *SafeMap[interface{}, *DeadTask[T]] // Map of hashKey to DeadTask
	config      Config[T]
	timer       Timer
	compareFunc func(a, b T) bool // Function to compare tasks
}

// New initializes the Pool with given workers and options
func New[T Hashable](ctx context.Context, workers []Worker[T], options ...Option[T]) *Pool[T] {
	pool := &Pool[T]{
		workers:         NewSafeMap[int, *workerState[T]](),
		nextWorkerID:    &atomic.Int64{},
		workersToRemove: NewSafeMap[int, bool](),
		taskQueues:      NewSafeMap[int, *taskQueue[T]](),
		config:          newDefaultConfig[T](),
		timer:           &timerImpl{},
		ctx:             ctx,
		tasks:           NewSafeMap[interface{}, *TaskWrapper[T]](),
		taskProcessing:  NewSafeMap[interface{}, int](),
		deadTasks:       NewSafeMap[interface{}, *DeadTask[T]](),
		processing:      &atomic.Int64{},
		stopped:         &atomic.Bool{},
		closed:          &atomic.Bool{},
	}
	for _, option := range options {
		option(pool)
	}

	if pool.compareFunc == nil {
		pool.compareFunc = func(a, b T) bool {
			return hashcode(a) == hashcode(b)
		}
	}

	pool.cond = sync.NewCond(&sync.Mutex{})

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
		p.cond.L.Lock()
		defer p.cond.L.Unlock()

		workerID := int(p.nextWorkerID.Add(1)) - 1

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
			currentTask:    &atomic.Value{},
			interrupted:    &atomic.Bool{},
			forceRemove:    &atomic.Bool{},
		}

		p.workers.Set(workerID, state)
		p.taskQueues.Set(workerID, &taskQueue[T]{tasks: NewSafeSlice[*TaskWrapper[T]]()})

		p.wg.Add(1)
		go p.workerLoop(workerID)
		done <- workerID
	}()

	return <-done
}

// Method to mark worker for removal
func (p *Pool[T]) RemovalWorker(workerID int) {
	p.workersToRemove.Set(workerID, true)
}

// WorkerController interface provides methods to control workers
type WorkerController[T Hashable] interface {
	AddWorker(worker Worker[T]) int
	RemovalWorker(workerID int)
	RestartWorker(workerID int) error
	InterruptWorker(workerID int, options ...WorkerInterruptOption) error
	PutTaskAsDead(data T) error
	DispatchDeadTask(data T, options ...TaskOption[T]) error
	RemoveDeadTask(data T) error
}

func (p *Pool[T]) RestartWorker(workerID int) error {
	state, exists := p.workers.Get(workerID)
	if !exists {
		return fmt.Errorf("worker %d does not exist", workerID)
	}

	if !state.interrupted.Load() {
		return fmt.Errorf("worker %d is not interrupted and cannot be restarted", workerID)
	}

	// Create a new context for the worker
	workerCtx, workerCancel := context.WithCancel(p.ctx)
	state.ctx = workerCtx
	state.cancel = workerCancel
	state.forcePanicFlag.Store(false)
	state.interrupted.Store(false)

	// Create new stopChan and doneChan
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
	state, exists := p.workers.Get(workerID)
	if !exists {
		return fmt.Errorf("worker %d does not exist", workerID)
	}

	// Cancel the worker's context
	state.cancel()

	// Close the worker's stop channel
	close(state.stopChan)

	// Signal the worker to wake up if it's waiting
	p.cond.Broadcast()

	doneChan := state.doneChan

	const workerStopTimeout = 5 * time.Second
	const workerForceStopTimeout = 5 * time.Second

	// Wait for the worker to finish, with a timeout
	select {
	case <-doneChan:
		// Worker has exited
	case <-time.After(workerStopTimeout):
		// Worker did not exit in time, force panic
		state.forcePanicFlag.Store(true)

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
	p.workers.Delete(workerID)
	p.taskQueues.Delete(workerID)

	// Requeue any tasks assigned to this worker
	p.requeueTasksFromWorker(workerID)

	return nil
}

// requeueTasksFromWorker reassigns tasks from the removed worker to other workers
func (p *Pool[T]) requeueTasksFromWorker(workerID int) {
	p.taskQueues.Range(func(key int, queue *taskQueue[T]) bool {
		queue.tasks.Range(func(idx int, task *TaskWrapper[T]) bool {
			if task.triedWorkers != nil {
				task.triedWorkers.Delete(workerID)
			}
			return true
		})
		return true
	})

	// Signal workers that the task queues have changed
	p.cond.Broadcast()
}

// workerLoop updated to handle scheduledTime, triedWorkers, and worker interruption
func (p *Pool[T]) workerLoop(workerID int) {
	defer p.wg.Done()

	state, exists := p.workers.Get(workerID)
	if !exists {
		return
	}
	stopChan := state.stopChan
	doneChan := state.doneChan
	ctx := state.ctx

	defer func() {
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

		p.cond.L.Lock()
		for p.isAllQueuesEmpty() && !p.stopped.Load() {
			p.cond.Wait()

			// Check if context is canceled
			if ctx.Err() != nil {
				p.cond.L.Unlock()
				return
			}
		}

		if p.stopped.Load() && p.isAllQueuesEmpty() {
			p.cond.L.Unlock()
			return
		}

		// Check if context is canceled before proceeding
		if ctx.Err() != nil {
			p.cond.L.Unlock()
			return
		}

		_, idx, task, ok := p.getNextTask(workerID)
		if !ok {
			p.cond.L.Unlock()
			continue
		}

		// Remove the task from the queue
		q, _ := p.taskQueues.Get(workerID)
		q.tasks.Delete(idx)

		// Mark the task as tried by this worker
		if task.triedWorkers == nil {
			task.triedWorkers = NewSafeMap[int, bool]()
		}
		task.triedWorkers.Set(workerID, true)

		// Set the current task
		state.currentTask.Store(task)

		// Add to taskProcessing map
		hashKey := hashcode(task.data)
		p.taskProcessing.Set(hashKey, workerID)

		p.processing.Add(1)
		p.cond.L.Unlock()

		// Check if context is canceled before processing the task
		if ctx.Err() != nil {
			p.processing.Add(-1)
			p.taskProcessing.Delete(hashKey)
			state.currentTask.Store((*TaskWrapper[T])(nil))
			return
		}

		p.runWorkerWithFailsafe(workerID, task)

		if p.workersToRemove.Exists(workerID) {
			p.workersToRemove.Delete(workerID)
			go p.RemoveWorker(workerID)
			return
		}

		// Check if the task was forcibly removed
		if state.forceRemove.Load() {
			state.forceRemove.Store(false)
			p.processing.Add(-1)
			p.cond.Broadcast()
			continue
		}

		// Unset the current task
		state.currentTask.Store((*TaskWrapper[T])(nil))

		// Remove from taskProcessing map
		p.taskProcessing.Delete(hashKey)

		if state.interrupted.Load() {
			p.processing.Add(-1)
			return // Exit the loop if the worker was interrupted
		}

		p.processing.Add(-1)
		p.cond.Broadcast()
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
	empty := true
	p.taskQueues.Range(func(key int, q *taskQueue[T]) bool {
		if q.tasks.Len() > 0 {
			empty = false
			return false
		}
		return true
	})
	return empty
}

// getNextTask returns the next task that the worker hasn't tried
func (p *Pool[T]) getNextTask(workerID int) (int, int, *TaskWrapper[T], bool) {
	q, _ := p.taskQueues.Get(workerID)
	var selectedTask *TaskWrapper[T]
	var selectedIndex int

	q.tasks.Range(func(idx int, task *TaskWrapper[T]) bool {
		if tried, _ := task.triedWorkers.Get(workerID); !tried {
			selectedTask = task
			selectedIndex = idx
			return false // Stop iteration
		}
		return true
	})

	if selectedTask != nil {
		return workerID, selectedIndex, selectedTask, true
	}

	return 0, 0, nil, false
}

// RangeTasks iterates over all tasks in the pool, including those currently being processed.
// The callback function receives the task data and the worker ID (-1 if the task is in the queue).
// If the callback returns false, the iteration stops.
type TaskStatus int

const (
	TaskStatusNotFound TaskStatus = iota
	TaskStatusQueued
	TaskStatusProcessing
	TaskStatusDead
)

// String provides a string representation of the TaskStatus
func (s TaskStatus) String() string {
	switch s {
	case TaskStatusNotFound:
		return "Not Found"
	case TaskStatusQueued:
		return "Queued"
	case TaskStatusProcessing:
		return "Processing"
	case TaskStatusDead:
		return "Dead"
	default:
		return "Unknown"
	}
}

// GetTaskStatus checks the current status of a task
func (p *Pool[T]) GetTaskStatus(data T) TaskStatus {
	hashKey := hashcode(data)

	// Check if the task is in dead tasks
	if _, exists := p.deadTasks.Get(hashKey); exists {
		return TaskStatusDead
	}

	// Check if the task is being processed
	if _, processing := p.taskProcessing.Get(hashKey); processing {
		return TaskStatusProcessing
	}

	// Check if the task is in the queue
	if _, exists := p.tasks.Get(hashKey); exists {
		return TaskStatusQueued
	}

	// If we haven't found the task anywhere, it's not in the pool
	return TaskStatusNotFound
}

func (p *Pool[T]) RangeTasks(cb func(data T, workerID int, status TaskStatus) bool) bool {
	// Iterate over tasks currently being processed
	p.workers.Range(func(workerID int, state *workerState[T]) bool {
		task, ok := state.currentTask.Load().(*TaskWrapper[T])
		if ok && task != nil {
			if !cb(task.data, workerID, TaskStatusProcessing) {
				return false
			}
		}
		return true
	})

	// Iterate over tasks in the queues
	p.taskQueues.Range(func(workerID int, queue *taskQueue[T]) bool {
		queue.tasks.Range(func(idx int, task *TaskWrapper[T]) bool {
			if !cb(task.data, workerID, TaskStatusQueued) {
				return false
			}
			return true
		})
		return true
	})

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
	state, exists := p.workers.Get(workerID)
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
	task, ok := state.currentTask.Load().(*TaskWrapper[T])
	if ok && task != nil {
		task.cancel()
	}

	// Close the stopChan to signal the worker to stop
	close(state.stopChan)

	// Handle task options
	if ok && task != nil {
		hashKey := hashcode(task.data)
		if cfg.RemoveTask {
			// Task is already canceled and will not be retried
			// Remove from tasks and taskProcessing maps
			p.tasks.Delete(hashKey)
			p.taskProcessing.Delete(hashKey)
		} else if cfg.ReassignTask {
			// Reset the task's context
			taskCtx, cancel := context.WithCancel(p.ctx)
			task.ctx = taskCtx
			task.cancel = cancel
			task.triedWorkers = NewSafeMap[int, bool]()
			task.SetScheduledTime(time.Now())
			// Add the task back to the taskQueues for the worker
			q, _ := p.taskQueues.Get(workerID)
			q.tasks.Append(task)
		}
	}

	if cfg.RemoveWorker {
		err := p.RemoveWorker(workerID)
		if err != nil {
			return fmt.Errorf("failed to remove worker %d: %v", workerID, err)
		}
	} else {
		state.interrupted.Store(true)

		if cfg.Restart {
			err := p.RestartWorker(workerID)
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
	if task == nil {
		log.Printf("Worker %d received nil task", workerID)
		return
	}

	// Create attempt-specific context
	attemptCtx, attemptCancel := context.WithCancel(task.ctx)
	defer attemptCancel() // Ensure the context is canceled when done

	// Wrap context with panicContext by default
	attemptCtx = &panicContext{Context: attemptCtx}

	// Wrap context with panicOnTimeoutContext if enabled
	if task.panicOnTimeout.Load() {
		attemptCtx = &panicOnTimeoutContext{Context: attemptCtx}
	}

	// Reset attempt-specific duration tracking
	start := time.Now()

	// If maxDuration is set, enforce it by cancelling the attempt's context when exceeded
	if task.maxDuration.Load() > 0 {
		go func() {
			select {
			case <-p.timer.After(time.Duration(task.maxDuration.Load())):
				attemptCancel() // Cancel the attempt's context when maxDuration is exceeded
			case <-attemptCtx.Done():
			}
		}()
	}

	// Enforce time limit for the overall task duration
	if task.timeLimit.Load() > 0 {
		currentTotalDuration := task.TotalDuration()

		// Pass necessary data to avoid accessing shared fields concurrently
		go func() {
			p.enforceTimeLimit(attemptCancel, time.Duration(task.timeLimit.Load()), currentTotalDuration, task.ctx)
		}()
	}

	var err error

	// Attempt to run the worker within a panic-catching function
	func() {
		defer func() {
			if r := recover(); r != nil {
				if p.config.panicHandler != nil {
					p.config.panicHandler(task.Data(), r)
				}
				// Also set err to indicate that a panic occurred
				err = fmt.Errorf("panic occurred in worker %d: %v", workerID, r)
			}
		}()

		// Attempt to run the worker
		state, exists := p.workers.Get(workerID)
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
	task.AddDuration(duration)
	task.durations.Append(duration)

	if err != nil {
		task.errors.Append(err)

		if IsUnrecoverable(err) {
			p.addToDeadTasks(task, err)
			return
		}
		// Check if the error is due to time limit or max duration exceeded
		if errors.Is(err, context.DeadlineExceeded) || (errors.Is(err, context.Canceled) && duration >= time.Duration(task.maxDuration.Load())) {
			// Exceeded maxDuration for this attempt
			p.requeueTask(task, fmt.Errorf("task exceeded max duration of %v for attempt", task.maxDuration.Load()))
			return
		}
		if err != context.Canceled && p.config.retryIf(err) {
			p.config.onRetry(int(task.Retries()), err, task)
			if p.config.onTaskFailure != nil {
				state, exists := p.workers.Get(workerID)
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
			state, exists := p.workers.Get(workerID)
			if exists {
				p.config.onTaskSuccess(p, workerID, state.worker, task)
			}
		}
		// Task succeeded, remove from tasks map
		hashKey := hashcode(task.data)
		p.tasks.Delete(hashKey)
		p.taskProcessing.Delete(hashKey)
	}
}

func IsUnrecoverable(err error) bool {
	var unrecoverableErr unrecoverableError
	return errors.As(err, &unrecoverableErr)
}

// requeueTask updated to handle delays and keep triedWorkers intact
func (p *Pool[T]) requeueTask(task *TaskWrapper[T], err error) {
	task.IncrementRetries()

	// Check if task has exceeded time limit
	if task.timeLimit.Load() > 0 && task.TotalDuration() >= time.Duration(task.timeLimit.Load()) {
		p.addToDeadTasks(task, err)
		return
	}

	// Reset the per-attempt duration for the next attempt
	task.durations.Clear()

	// Check if max attempts reached (unless unlimited retries)
	if p.config.attempts != UnlimitedAttempts && task.Retries() >= int32(p.config.attempts) {
		p.addToDeadTasks(task, err)
		return
	}

	// Calculate delay before next retry
	delay := p.calculateDelay(int(task.Retries()), err)
	task.SetScheduledTime(time.Now().Add(delay))

	// Reset triedWorkers
	task.triedWorkers = NewSafeMap[int, bool]()

	// Add the task back to the taskQueues
	workerIDs := p.workers.Keys()
	selectedWorkerID := workerIDs[rand.Intn(len(workerIDs))]

	q, _ := p.taskQueues.Get(selectedWorkerID)
	q.tasks.Append(task)

	p.cond.Broadcast()
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
	totalDuration := task.TotalDuration()
	errors := task.Errors()
	if finalError != nil && (len(errors) == 0 || finalError.Error() != errors[len(errors)-1].Error()) {
		errors = append(errors, finalError)
	}
	hashKey := hashcode(task.data)
	p.deadTasks.Set(hashKey, &DeadTask[T]{
		Data:          task.data,
		Retries:       task.Retries(),
		TotalDuration: totalDuration,
		Errors:        errors,
	})

	// Remove from tasks and taskProcessing maps
	p.tasks.Delete(hashKey)
	p.taskProcessing.Delete(hashKey)
}

// Dispatch updated to accept TaskOptions
func (p *Pool[T]) Dispatch(data T, options ...TaskOption[T]) error {
	if p.stopped.Load() {
		return errors.New("pool is closed")
	}

	taskCtx, cancel := context.WithCancel(p.ctx)
	task := &TaskWrapper[T]{
		data:         data,
		triedWorkers: NewSafeMap[int, bool](),
		errors:       NewSafeSlice[error](),
		durations:    NewSafeSlice[time.Duration](),
		ctx:          taskCtx,
		cancel:       cancel,
	}
	for _, opt := range options {
		opt(task)
	}
	task.SetScheduledTime(time.Now())

	// Add task to the tasks map
	hashKey := hashcode(data)
	p.tasks.Set(hashKey, task)

	// Add the task to a worker's queue (simple round-robin for this example)
	workerIDs := p.workers.Keys()

	if len(workerIDs) == 0 {
		return errors.New("no workers available")
	}

	selectedWorkerID := workerIDs[rand.Intn(len(workerIDs))]

	q, _ := p.taskQueues.Get(selectedWorkerID)
	q.tasks.Append(task)

	// Signal all waiting workers that there's a new task
	p.cond.Broadcast()

	return nil
}

// DeadTasks returns a copy of the dead tasks list
func (p *Pool[T]) DeadTasks() []DeadTask[T] {
	result := make([]DeadTask[T], 0)
	p.deadTasks.Range(func(key interface{}, deadTask *DeadTask[T]) bool {
		result = append(result, *deadTask)
		return true
	})
	return result
}

// QueueSize returns the total number of tasks in the queue
func (p *Pool[T]) QueueSize() int {
	total := 0
	p.taskQueues.Range(func(key int, q *taskQueue[T]) bool {
		total += q.tasks.Len()
		return true
	})
	return total
}

// ProcessingCount returns the number of tasks currently being processed
func (p *Pool[T]) ProcessingCount() int {
	return int(p.processing.Load())
}

// Close stops the pool and waits for all tasks to complete
func (p *Pool[T]) Close() {
	if p.closed.Load() {
		return
	}
	p.closed.Store(true)
	p.stopped.Store(true)
	p.cond.Broadcast()
	p.wg.Wait()
}

// ForceClose stops the pool without waiting for tasks to complete
func (p *Pool[T]) ForceClose() {
	if p.closed.Load() {
		return
	}
	p.closed.Store(true)
	p.stopped.Store(true)
	p.taskQueues.Range(func(key int, q *taskQueue[T]) bool {
		q.tasks.Clear()
		return true
	})
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
func newDefaultConfig[T Hashable]() Config[T] {
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
func WithPanicHandler[T Hashable](handler PanicHandlerFunc[T]) Option[T] {
	return func(p *Pool[T]) {
		p.config.panicHandler = handler
	}
}

// WithAttempts sets the maximum number of attempts
func WithAttempts[T Hashable](attempts int) Option[T] {
	return func(p *Pool[T]) {
		p.config.attempts = attempts
	}
}

// WithWorkerContext creates a specific context for each worker
func WithWorkerContext[T Hashable](fn ContextFunc) Option[T] {
	return func(p *Pool[T]) {
		p.config.contextFunc = fn
	}
}

// WithDelay sets the delay between retries
func WithDelay[T Hashable](delay time.Duration) Option[T] {
	return func(p *Pool[T]) {
		p.config.delay = delay
	}
}

// WithMaxDelay sets the maximum delay between retries
func WithMaxDelay[T Hashable](maxDelay time.Duration) Option[T] {
	return func(p *Pool[T]) {
		p.config.maxDelay = maxDelay
	}
}

// WithMaxJitter sets the maximum random jitter between retries
func WithMaxJitter[T Hashable](maxJitter time.Duration) Option[T] {
	return func(p *Pool[T]) {
		p.config.maxJitter = maxJitter
	}
}

// WithDelayType sets the delay type function
func WithDelayType[T Hashable](delayType DelayTypeFunc[T]) Option[T] {
	return func(p *Pool[T]) {
		p.config.delayType = delayType
	}
}

// WithOnRetry sets the OnRetry callback function
func WithOnRetry[T Hashable](onRetry OnRetryFunc[T]) Option[T] {
	return func(p *Pool[T]) {
		p.config.onRetry = onRetry
	}
}

// WithRetryIf sets the RetryIf function
func WithRetryIf[T Hashable](retryIf RetryIfFunc) Option[T] {
	return func(p *Pool[T]) {
		p.config.retryIf = retryIf
	}
}

// WithContext sets the context for the Pool
func WithContext[T Hashable](ctx context.Context) Option[T] {
	return func(p *Pool[T]) {
		p.config.context = ctx
	}
}

// WithTimer allows setting a custom timer
func WithTimer[T Hashable](timer Timer) Option[T] {
	return func(p *Pool[T]) {
		p.timer = timer
	}
}

// WithOnTaskSuccess sets the OnTaskSuccess callback function
func WithOnTaskSuccess[T Hashable](onTaskSuccess OnTaskSuccessFunc[T]) Option[T] {
	return func(p *Pool[T]) {
		p.config.onTaskSuccess = onTaskSuccess
	}
}

// WithOnTaskFailure sets the OnTaskFailure callback function
func WithOnTaskFailure[T Hashable](onTaskFailure OnTaskFailureFunc[T]) Option[T] {
	return func(p *Pool[T]) {
		p.config.onTaskFailure = onTaskFailure
	}
}

// WithCompareFunc sets a custom function to compare tasks
func WithCompareFunc[T Hashable](fn func(a, b T) bool) Option[T] {
	return func(p *Pool[T]) {
		p.compareFunc = fn
	}
}

// TaskOption functions for configuring individual tasks

// WithMaxDuration TaskOption to set per-attempt max duration
func WithMaxDuration[T Hashable](maxDuration time.Duration) TaskOption[T] {
	return func(t *TaskWrapper[T]) {
		t.maxDuration.Store(int64(maxDuration))
	}
}

// WithTimeLimit sets a time limit for a task, considering all retries
func WithTimeLimit[T Hashable](limit time.Duration) TaskOption[T] {
	return func(t *TaskWrapper[T]) {
		t.timeLimit.Store(int64(limit))
	}
}

// WithImmediateRetry enables immediate retry for a task
func WithImmediateRetry[T Hashable]() TaskOption[T] {
	return func(t *TaskWrapper[T]) {
		t.immediateRetry.Store(true)
	}
}

// WithPanicOnTimeout enables panic on timeout for a task
func WithPanicOnTimeout[T Hashable]() TaskOption[T] {
	return func(t *TaskWrapper[T]) {
		t.panicOnTimeout.Store(true)
	}
}

// WithRetries sets the retries for a task
func WithRetries[T Hashable](retries int) TaskOption[T] {
	return func(t *TaskWrapper[T]) {
		t.retries.Store(int32(retries))
	}
}

// DelayTypeFunc signature
type DelayTypeFunc[T Hashable] func(n int, err error, config *Config[T]) time.Duration

// OnRetryFunc signature
type OnRetryFunc[T Hashable] func(attempt int, err error, task *TaskWrapper[T])

// OnTaskSuccessFunc is the type of function called when a task succeeds
type OnTaskSuccessFunc[T Hashable] func(controller WorkerController[T], workerID int, worker Worker[T], task *TaskWrapper[T])

// OnTaskFailureFunc is the type of function called when a task fails
type OnTaskFailureFunc[T Hashable] func(controller WorkerController[T], workerID int, worker Worker[T], task *TaskWrapper[T], err error)

// RetryIfFunc signature
type RetryIfFunc func(error) bool

// ContextFunc signature
type ContextFunc func() context.Context

// DelayType functions

// BackOffDelay increases delay exponentially
func BackOffDelay[T Hashable](n int, _ error, config *Config[T]) time.Duration {
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
func FixedDelay[T Hashable](_ int, _ error, config *Config[T]) time.Duration {
	return config.delay
}

// RandomDelay adds random jitter
func RandomDelay[T Hashable](_ int, _ error, config *Config[T]) time.Duration {
	return time.Duration(rand.Int63n(int64(config.maxJitter)))
}

// CombineDelay combines multiple DelayType functions
func CombineDelay[T Hashable](delays ...DelayTypeFunc[T]) DelayTypeFunc[T] {
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

// panicOnTimeoutError indicates that the context was canceled due to timeout
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
type PanicHandlerFunc[T Hashable] func(task T, v interface{})

// New methods added for handling dead tasks

// PutTaskAsDead moves a task to the dead tasks list
func (p *Pool[T]) PutTaskAsDead(data T) error {
	hashKey := hashcode(data)

	// Check if the task is already in dead tasks
	if _, exists := p.deadTasks.Get(hashKey); exists {
		return fmt.Errorf("task is already in dead tasks")
	}

	task, exists := p.tasks.Get(hashKey)
	if !exists {
		// Task not found in tasks map, it might be currently processing
		workerID, processing := p.taskProcessing.Get(hashKey)
		if processing {
			state, _ := p.workers.Get(workerID)
			currentTask, ok := state.currentTask.Load().(*TaskWrapper[T])
			if ok && currentTask != nil && p.compareFunc(currentTask.data, data) {
				// Mark the task for removal
				state.forceRemove.Store(true)
				// Add to dead tasks
				p.addToDeadTasks(currentTask, errors.New("task manually moved to dead tasks while processing"))
				return nil
			}
		}
		// If we reach here, the task is not in the tasks map and not being processed
		return fmt.Errorf("task not found in pool")
	}

	// Task found in tasks map
	// Check if task is being processed
	workerID, processing := p.taskProcessing.Get(hashKey)
	if processing {
		state, _ := p.workers.Get(workerID)
		currentTask, ok := state.currentTask.Load().(*TaskWrapper[T])
		if ok && currentTask != nil && p.compareFunc(currentTask.data, data) {
			// Mark the task for removal
			state.forceRemove.Store(true)
			// Remove from tasks map
			p.tasks.Delete(hashKey)
			// Add to dead tasks
			p.addToDeadTasks(currentTask, errors.New("task manually moved to dead tasks while processing"))
			return nil
		}
	}

	// Task is in queue, remove it
	removed := false
	p.taskQueues.Range(func(workerID int, q *taskQueue[T]) bool {
		q.tasks.Range(func(idx int, t *TaskWrapper[T]) bool {
			if p.compareFunc(t.data, data) {
				// Remove the task from the queue
				q.tasks.Delete(idx)
				// Remove from tasks map
				p.tasks.Delete(hashKey)
				// Add to dead tasks
				p.addToDeadTasks(task, errors.New("task manually moved to dead tasks from queue"))
				removed = true
				return false
			}
			return true
		})
		return !removed
	})

	if removed {
		return nil
	}

	return fmt.Errorf("task found in pool but could not be removed")
}

// DispatchDeadTask removes a task from dead tasks and dispatches it again
func (p *Pool[T]) DispatchDeadTask(data T, options ...TaskOption[T]) error {
	hashKey := hashcode(data)

	deadTask, exists := p.deadTasks.Get(hashKey)
	if !exists {
		return fmt.Errorf("task not found in dead tasks")
	}

	// Remove the task from deadTasks
	p.deadTasks.Delete(hashKey)

	// Use Dispatch to add the task back
	options = append(options, WithRetries[T](int(deadTask.Retries)))
	return p.Dispatch(deadTask.Data, options...)
}

// RemoveDeadTask removes a task from the dead tasks list
func (p *Pool[T]) RemoveDeadTask(data T) error {
	hashKey := hashcode(data)

	if !p.deadTasks.Exists(hashKey) {
		return fmt.Errorf("task not found in dead tasks")
	}

	p.deadTasks.Delete(hashKey)
	return nil
}

// SafeMap is a thread-safe generic map
type SafeMap[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

// NewSafeMap creates a new SafeMap
func NewSafeMap[K comparable, V any]() *SafeMap[K, V] {
	return &SafeMap[K, V]{
		m: make(map[K]V),
	}
}

// Set adds a key-value pair to the map
func (sm *SafeMap[K, V]) Set(key K, value V) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.m[key] = value
}

// Get retrieves a value from the map by key
func (sm *SafeMap[K, V]) Get(key K) (V, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	value, ok := sm.m[key]
	return value, ok
}

// Delete removes a key-value pair from the map
func (sm *SafeMap[K, V]) Delete(key K) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.m, key)
}

// Exists checks if a key exists in the map
func (sm *SafeMap[K, V]) Exists(key K) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	_, exists := sm.m[key]
	return exists
}

// Len returns the number of items in the map
func (sm *SafeMap[K, V]) Len() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.m)
}

// Range calls the given function for each key-value pair in the map
func (sm *SafeMap[K, V]) Range(f func(key K, value V) bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for k, v := range sm.m {
		if !f(k, v) {
			break
		}
	}
}

// Keys returns a slice of keys in the map
func (sm *SafeMap[K, V]) Keys() []K {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	keys := make([]K, 0, len(sm.m))
	for k := range sm.m {
		keys = append(keys, k)
	}
	return keys
}

// SafeSlice is a thread-safe slice
type SafeSlice[T any] struct {
	mu    sync.RWMutex
	items []T
}

// NewSafeSlice creates a new SafeSlice
func NewSafeSlice[T any]() *SafeSlice[T] {
	return &SafeSlice[T]{
		items: make([]T, 0),
	}
}

// Append adds an item to the slice
func (ss *SafeSlice[T]) Append(item T) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.items = append(ss.items, item)
}

// Get retrieves an item by index
func (ss *SafeSlice[T]) Get(index int) (T, bool) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	if index < 0 || index >= len(ss.items) {
		var zero T
		return zero, false
	}
	return ss.items[index], true
}

// Delete removes an item by index
func (ss *SafeSlice[T]) Delete(index int) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if index < 0 || index >= len(ss.items) {
		return
	}
	ss.items = append(ss.items[:index], ss.items[index+1:]...)
}

// Len returns the length of the slice
func (ss *SafeSlice[T]) Len() int {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return len(ss.items)
}

// Clear removes all items from the slice
func (ss *SafeSlice[T]) Clear() {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.items = ss.items[:0]
}

// Range iterates over the slice and calls the provided function for each item
func (ss *SafeSlice[T]) Range(f func(index int, item T) bool) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	for i, item := range ss.items {
		if !f(i, item) {
			break
		}
	}
}

// Items returns a copy of the slice
func (ss *SafeSlice[T]) Items() []T {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	items := make([]T, len(ss.items))
	copy(items, ss.items)
	return items
}
