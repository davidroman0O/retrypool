package retrypool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/automaxprocs/maxprocs"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/davidroman0O/retrypool/logs"
	"github.com/sasha-s/go-deadlock"
)

func init() {
	maxprocs.Set()

	deadlock.Opts.DeadlockTimeout = time.Second * 2 // Time to wait before reporting a potential deadlock
	deadlock.Opts.OnPotentialDeadlock = func() {
		// You can customize the behavior when a potential deadlock is detected
		log.Println("POTENTIAL DEADLOCK DETECTED!")
		buf := make([]byte, 1<<16)
		runtime.Stack(buf, true)
		log.Printf("Goroutine stack dump:\n%s", buf)
	}
}

// Define a public context key for the worker ID
type workerIDKeyType struct{}

var WorkerIDKey = workerIDKeyType{}

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
	mu            sync.Mutex
	data          T
	retries       int
	totalDuration time.Duration
	timeLimit     time.Duration   // Zero means no overall limit
	maxDuration   time.Duration   // Max duration per attempt
	scheduledTime time.Time       // For delay between retries
	triedWorkers  map[int]bool    // Track workers that have tried this task
	errors        []error         // Track errors for each attempt
	durations     []time.Duration // Track duration for each attempt

	immediateRetry bool

	beingProcessed processedNotification // optional
	beingQueued    queuedNotification    // optional

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

// TaskQueue interface defines the methods for task queue operations
type TaskQueue[T any] interface {
	Enqueue(task *TaskWrapper[T])
	InsertAt(index int, task *TaskWrapper[T]) error
	RemoveAt(index int) (*TaskWrapper[T], error)
	Length() int
	Tasks() []*TaskWrapper[T]
	Clear()
}

func (q *taskQueue[T]) Clear() {
	q.tasks = nil
}

// taskQueue implements TaskQueue interface
type taskQueue[T any] struct {
	tasks []*TaskWrapper[T]
}

func (q *taskQueue[T]) Enqueue(task *TaskWrapper[T]) {
	q.tasks = append(q.tasks, task)
}

func (q *taskQueue[T]) InsertAt(index int, task *TaskWrapper[T]) error {
	if index < 0 || index > len(q.tasks) {
		return fmt.Errorf("index out of range")
	}
	q.tasks = append(q.tasks[:index], append([]*TaskWrapper[T]{task}, q.tasks[index:]...)...)
	return nil
}

func (q *taskQueue[T]) RemoveAt(index int) (*TaskWrapper[T], error) {
	if index < 0 || index >= len(q.tasks) {
		return nil, fmt.Errorf("index out of range")
	}
	task := q.tasks[index]
	q.tasks = append(q.tasks[:index], q.tasks[index+1:]...)
	return task, nil
}

func (q *taskQueue[T]) Length() int {
	return len(q.tasks)
}

func (q *taskQueue[T]) Tasks() []*TaskWrapper[T] {
	return q.tasks
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

	panicHandler PanicHandlerFunc[T]
	panicWorker  PanicWorker

	logLevel logs.Level

	roundRobinDistribution bool // Distribute tasks round-robin among workers
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
	taskQueues      map[int]TaskQueue[T] // Updated to use TaskQueue interface
	processing      int
	mu              deadlock.Mutex
	cond            *sync.Cond
	stopped         bool
	closed          bool
	ctx             context.Context
	cancel          context.CancelFunc
	deadTasks       []DeadTask[T]
	deadTasksMutex  deadlock.RWMutex

	config Config[T]

	metrics Metrics

	limiter *rate.Limiter

	errGroup *errgroup.Group

	taskWrapperPool sync.Pool

	nextRoundRobinWorkerIndex int
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
		taskQueues:      make(map[int]TaskQueue[T]),
		config:          newDefaultConfig[T](),
		ctx:             ctx,
		cancel:          cancel,
		errGroup:        errGroup,

		taskWrapperPool: sync.Pool{
			New: func() interface{} {
				return &TaskWrapper[T]{}
			},
		},
	}

	for _, option := range options {
		option(pool)
	}

	pool.cond = sync.NewCond(&pool.mu) // Use the pool's mutex directly

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

// Knowing the amount of workers minus the amount of tasks being processed, we can determine the amount of available slots for new tasks.
func (p *Pool[T]) AvailableWorkers() int {
	workerIDs := p.GetWorkerIDs()
	availableSlots := len(workerIDs) - p.ProcessingCount()
	if availableSlots <= 0 {
		return 0
	}
	return availableSlots
}

// Range on workers through callback
func (p *Pool[T]) RangeWorkers(callback func(workerID int, worker Worker[T]) bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for workerID, workerState := range p.workers {
		if !callback(workerID, workerState.worker) {
			break
		}
	}
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
	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		logs.Error(context.Background(), "Error while waiting for workers to finish", "error", err)
	}

	// Add unprocessed tasks to the dead tasks list
	p.mu.Lock()
	for workerID, queue := range p.taskQueues {
		tasks := queue.Tasks()
		for _, task := range tasks {
			p.addToDeadTasks(task, context.Canceled)
		}
		delete(p.taskQueues, workerID) // Clear the queue after moving tasks
	}
	p.mu.Unlock()

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
func (p *Pool[T]) AddWorker(worker Worker[T], options ...WorkerOption[T]) int {
	p.mu.Lock()
	workerID := p.nextWorkerID
	p.nextWorkerID++

	// Create worker state
	var workerCtx context.Context
	var workerCancel context.CancelFunc

	// Create base cancellable context from pool
	workerCtx, workerCancel = context.WithCancel(p.ctx)

	state := &workerState[T]{
		worker:   worker,
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),
		cancel:   workerCancel,
		ctx:      workerCtx,
	}

	for _, opt := range options {
		opt(state)
	}

	p.workers[workerID] = state

	// Redistribute existing tasks among all workers
	if len(p.taskQueues) > 0 {
		allTasks := make([]*TaskWrapper[T], 0)
		// Collect all tasks
		for _, queue := range p.taskQueues {
			allTasks = append(allTasks, queue.Tasks()...)
		}
		// Clear existing queues
		p.taskQueues = make(map[int]TaskQueue[T])

		// Redistribute tasks round-robin
		for i, task := range allTasks {
			targetWorkerID := i % len(p.workers)
			q := p.taskQueues[targetWorkerID]
			if q == nil {
				q = &taskQueue[T]{}
				p.taskQueues[targetWorkerID] = q
			}
			q.Enqueue(task)
		}
	}

	// Start worker
	p.errGroup.Go(func() error {
		return p.workerLoop(workerID)
	})

	p.cond.Broadcast()
	p.mu.Unlock()

	logs.Info(context.Background(), "Added new worker", "workerID", workerID)
	return workerID
}

func (p *Pool[T]) RedistributeTasks() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Get all worker IDs
	workerIDs := make([]int, 0, len(p.workers))
	for workerID := range p.workers {
		workerIDs = append(workerIDs, workerID)
	}

	// Collect worker data
	workerProcessingTasks := make(map[int]int) // 1 if processing a task, 0 otherwise
	workerQueueLengths := make(map[int]int)
	totalTasks := 0

	for _, workerID := range workerIDs {
		queue, exists := p.taskQueues[workerID]
		if !exists {
			// Ensure every worker has a queue
			queue = &taskQueue[T]{}
			p.taskQueues[workerID] = queue
		}
		length := queue.Length()
		workerQueueLengths[workerID] = length

		// Check if the worker is processing a task
		if state, exists := p.workers[workerID]; exists && state.currentTask != nil {
			workerProcessingTasks[workerID] = 1
		} else {
			workerProcessingTasks[workerID] = 0
		}

		totalTasks += length + workerProcessingTasks[workerID]
	}

	numWorkers := len(workerIDs)
	if numWorkers == 0 || totalTasks == 0 {
		return
	}

	// Calculate desired total tasks per worker
	baseTasksPerWorker := totalTasks / numWorkers
	extraTasks := totalTasks % numWorkers

	desiredTotalTasksPerWorker := make(map[int]int)
	for _, workerID := range workerIDs {
		desiredTotalTasksPerWorker[workerID] = baseTasksPerWorker
		if extraTasks > 0 {
			desiredTotalTasksPerWorker[workerID]++
			extraTasks--
		}
	}

	// Calculate desired queue lengths per worker
	desiredQueueLengths := make(map[int]int)
	for _, workerID := range workerIDs {
		desiredQueueLengths[workerID] = desiredTotalTasksPerWorker[workerID] - workerProcessingTasks[workerID]
		if desiredQueueLengths[workerID] < 0 {
			desiredQueueLengths[workerID] = 0
		}
	}

	// Initialize overfull and underfull workers lists
	overfullWorkers := []int{}
	underfullWorkers := []int{}

	for _, workerID := range workerIDs {
		length := workerQueueLengths[workerID]
		desiredLength := desiredQueueLengths[workerID]
		if length > desiredLength {
			overfullWorkers = append(overfullWorkers, workerID)
		} else if length < desiredLength {
			underfullWorkers = append(underfullWorkers, workerID)
		}
	}

	// Continue moving tasks until overfullWorkers and underfullWorkers are balanced
	for len(overfullWorkers) > 0 && len(underfullWorkers) > 0 {
		fromWorkerID := overfullWorkers[0]
		fromQueue := p.taskQueues[fromWorkerID]
		desiredFromLength := desiredQueueLengths[fromWorkerID]

		toWorkerID := underfullWorkers[0]
		toQueue := p.taskQueues[toWorkerID]
		desiredToLength := desiredQueueLengths[toWorkerID]

		// Move a task from fromWorkerID to toWorkerID
		task, err := fromQueue.RemoveAt(0)
		if err == nil {
			toQueue.Enqueue(task)
			workerQueueLengths[fromWorkerID]--
			workerQueueLengths[toWorkerID]++

			// Update overfull and underfull worker counts
			if workerQueueLengths[fromWorkerID] <= desiredFromLength {
				overfullWorkers = overfullWorkers[1:]
			}

			if workerQueueLengths[toWorkerID] >= desiredToLength {
				underfullWorkers = underfullWorkers[1:]
			}
		} else {
			// Cannot remove task, skip to next overfull worker
			overfullWorkers = overfullWorkers[1:]
		}
	}

	// Notify all workers in case they're waiting for tasks
	p.cond.Broadcast()
}

func (p *Pool[T]) GetRandomWorkerID() int {

	workerIDs := make([]int, 0, len(p.workers))
	p.mu.Lock()
	for workerID := range p.workers {
		workerIDs = append(workerIDs, workerID)
	}
	p.mu.Unlock()
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
	AddWorker(worker Worker[T], options ...WorkerOption[T]) int
	RemovalWorker(workerID int)
	RestartWorker(workerID int) error
	InterruptWorker(workerID int, options ...WorkerInterruptOption) error
}

func (p *Pool[T]) RestartWorker(workerID int) error {
	p.mu.Lock()
	state, exists := p.workers[workerID]
	if !exists {
		p.mu.Unlock()
		return fmt.Errorf("%w: worker %d does not exist", ErrInvalidWorkerID, workerID)
	}
	p.mu.Unlock()

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

	p.mu.Lock()
	// Start a new goroutine for the worker
	p.errGroup.Go(func() error {
		return p.workerLoop(workerID)
	})
	p.mu.Unlock()

	logs.Info(context.Background(), "Worker has been restarted", "workerID", workerID)
	return nil
}

var ErrAlreadyRemovingWorker = errors.New("worker is already being removed")

// RemoveWorker removes a worker from the pool
func (p *Pool[T]) RemoveWorker(workerID int) error {
	p.mu.Lock()

	state, exists := p.workers[workerID]
	if !exists {
		p.mu.Unlock()
		return fmt.Errorf("%w: worker %d does not exist", ErrInvalidWorkerID, workerID)
	}

	// Check if the worker is already removed
	if state.removed {
		p.mu.Unlock()
		return ErrAlreadyRemovingWorker
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
	q := p.taskQueues[workerID]
	if q == nil {
		p.mu.Unlock()
		return
	}

	// Get all tasks and find other workers
	tasks := q.Tasks()
	availableWorkers := make([]int, 0)
	for wID, wState := range p.workers {
		if wID != workerID && !wState.interrupted {
			availableWorkers = append(availableWorkers, wID)
		}
	}

	if len(availableWorkers) > 0 {
		// Distribute tasks among available workers
		for i, task := range tasks {
			selectedWorkerID := availableWorkers[i%len(availableWorkers)]
			task.mu.Lock()
			task.triedWorkers = make(map[int]bool)
			task.queuedAt = append(task.queuedAt, time.Now())
			task.mu.Unlock()

			dstQ := p.taskQueues[selectedWorkerID]
			if dstQ == nil {
				dstQ = &taskQueue[T]{}
				p.taskQueues[selectedWorkerID] = dstQ
			}
			dstQ.Enqueue(task)

			logs.Debug(context.Background(), "Task requeued to worker",
				"fromWorkerID", workerID,
				"toWorkerID", selectedWorkerID,
				"taskData", task.data)
		}
	}

	// Delete original queue
	delete(p.taskQueues, workerID)
	p.cond.Broadcast()
	p.mu.Unlock()
}

// InterruptWorker cancels a worker's current task and optionally removes the worker.
// It can also force the worker to panic.
func (p *Pool[T]) InterruptWorker(workerID int, options ...WorkerInterruptOption) error {
	p.mu.Lock()
	state, exists := p.workers[workerID]
	if !exists {
		p.mu.Unlock()
		return fmt.Errorf("%w: worker %d does not exist", ErrInvalidWorkerID, workerID)
	}

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

	if cfg.ForcePanic {
		state.forcePanic = true
	}

	// Mark as interrupted and get task info while holding lock
	state.interrupted = true
	currentTask := state.currentTask
	currentQueue := p.taskQueues[workerID]
	delete(p.taskQueues, workerID) // Remove queue first
	p.mu.Unlock()

	// Cancel the worker's context
	state.cancel()

	if cfg.ReassignTask {
		p.mu.Lock()
		// Find available worker
		availableWorkerID := -1
		for wID, wState := range p.workers {
			if wID != workerID && !wState.interrupted {
				availableWorkerID = wID
				break
			}
		}
		p.mu.Unlock()

		if availableWorkerID != -1 {
			// Requeue all tasks including current
			tasks := make([]*TaskWrapper[T], 0)
			if currentTask != nil {
				tasks = append(tasks, currentTask)
			}
			if currentQueue != nil {
				tasks = append(tasks, currentQueue.Tasks()...)
			}

			p.mu.Lock()
			for _, task := range tasks {
				task.mu.Lock()
				// Reset task state
				task.triedWorkers = make(map[int]bool)
				task.scheduledTime = time.Now()
				task.mu.Unlock()

				// Add to available worker's queue
				q := p.taskQueues[availableWorkerID]
				if q == nil {
					q = &taskQueue[T]{}
					p.taskQueues[availableWorkerID] = q
				}
				q.Enqueue(task)
				logs.Debug(context.Background(), "Task reassigned to worker",
					"fromWorkerID", workerID,
					"toWorkerID", availableWorkerID,
					"taskData", task.data)
			}
			p.cond.Broadcast()
			p.mu.Unlock()
		}
	}

	if cfg.RemoveWorker {
		err := p.RemoveWorker(workerID)
		if err != nil {
			return fmt.Errorf("failed to remove worker %d: %w", workerID, err)
		}
	} else if cfg.Restart {
		err := p.RestartWorker(workerID)
		if err != nil {
			return fmt.Errorf("failed to restart worker %d: %w", workerID, err)
		}
	}

	return nil
}

func setWorkerIDFieldIfExists[T any](worker Worker[T], id int) {
	rv := reflect.ValueOf(worker)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		// Not a struct; cannot set field
		return
	}
	field := rv.FieldByName("ID")
	if !field.IsValid() || !field.CanSet() {
		// Field does not exist or cannot be set
		return
	}
	if field.Kind() == reflect.Int {
		field.SetInt(int64(id))
	}
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
		if state.removed {
			// Remove worker from the pool when it exits
			delete(p.workers, workerID)
			logs.Debug(context.Background(), "Worker loop exited and removed", "workerID", workerID)
		} else {
			logs.Debug(context.Background(), "Worker loop exited but not removed", "workerID", workerID)
		}
		p.mu.Unlock()
	}()

	if !exists {
		return fmt.Errorf("%w: worker %d does not exist", ErrInvalidWorkerID, workerID)
	}

	// Set the worker's ID field if it exists
	setWorkerIDFieldIfExists(state.worker, workerID)

	for {
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		case <-state.ctx.Done():
			return state.ctx.Err()
		default:
		}

		logs.Debug(context.Background(), "Worker is waiting for tasks", "workerID", workerID)

		p.mu.Lock()
		now := time.Now()
		_, idx, task, ok := p.getNextTask(workerID)
		if !ok {
			logs.Debug(context.Background(), "Worker hasn't found any tasks", "workerID", workerID)
			// No immediate tasks; find the next scheduled task time
			q := p.taskQueues[workerID]
			if q == nil {
				if p.stopped {
					p.mu.Unlock()
					return nil
				}
				// Wait for new tasks
				logs.Debug(context.Background(), "Worker is waiting for tasks, waiting", "workerID", workerID)
				p.cond.Wait()
				p.mu.Unlock()
				continue
			}
			tasks := q.Tasks()
			if len(tasks) == 0 {
				if p.stopped {
					p.mu.Unlock()
					return nil
				}
				// Wait for new tasks
				logs.Debug(context.Background(), "Worker is waiting for tasks, waiting", "workerID", workerID)
				p.cond.Wait()
				p.mu.Unlock()
				continue
			}
			// Wait until the next task is scheduled
			nextScheduledTime := tasks[0].scheduledTime
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
			p.reinsertTask(workerID, idx, task)
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

func (p *Pool[T]) reinsertTask(workerID, idx int, task *TaskWrapper[T]) {
	q := p.taskQueues[workerID]
	if q == nil {
		q = &taskQueue[T]{}
		p.taskQueues[workerID] = q
	}
	err := q.InsertAt(idx, task)
	if err != nil {
		logs.Error(context.Background(), "Failed to reinsert task", "error", err)
	}
}

// isAllQueuesEmpty checks if all task queues are empty
func (p *Pool[T]) isAllQueuesEmpty() bool {
	for _, q := range p.taskQueues {
		if q.Length() > 0 {
			return false
		}
	}
	return true
}

// getNextTask returns the next task that the worker hasn't tried
func (p *Pool[T]) getNextTask(workerID int) (int, int, *TaskWrapper[T], bool) {
	now := time.Now()
	q := p.taskQueues[workerID]
	if q == nil {
		return 0, 0, nil, false
	}
	tasks := q.Tasks()

	// First, check for immediate retry tasks
	for idx, task := range tasks {
		if task.immediateRetry && !task.triedWorkers[workerID] && !task.scheduledTime.After(now) {
			// Remove the task from the queue
			_, err := q.RemoveAt(idx)
			if err != nil {
				logs.Error(context.Background(), "Failed to remove task", "error", err)
				continue
			}
			if q.Length() == 0 {
				delete(p.taskQueues, workerID)
			}
			return workerID, idx, task, true
		}
	}

	// Then, check for any task this worker hasn't tried and are ready to process
	for idx, task := range tasks {
		if task.triedWorkers == nil {
			task.triedWorkers = make(map[int]bool)
		}
		if !task.triedWorkers[workerID] && !task.scheduledTime.After(now) {
			// Remove the task from the queue
			_, err := q.RemoveAt(idx)
			if err != nil {
				logs.Error(context.Background(), "Failed to remove task", "error", err)
				continue
			}
			if q.Length() == 0 {
				delete(p.taskQueues, workerID)
			}
			return workerID, idx, task, true
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

func (s TaskStatus) String() string {
	switch s {
	case TaskStatusQueued:
		return "queued"
	case TaskStatusProcessing:
		return "processing"
	}
	return "unknown"
}

func (p *Pool[T]) RangeTasks(cb func(data *TaskWrapper[T], workerID int, status TaskStatus) bool) bool {
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
			// TODO we need to fix that
			if !cb(state.currentTask, workerID, TaskStatusProcessing) {
				return false
			}
		}
	}

	// Iterate over tasks in the queues
	for workerID, queue := range p.taskQueues {
		tasks := queue.Tasks()
		for _, task := range tasks {
			// TODO we need to fix that
			if !cb(task, workerID, TaskStatusQueued) {
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

// runWorkerWithFailsafe handles the execution of a task, including panic recovery and retries
func (p *Pool[T]) runWorkerWithFailsafe(workerID int, task *TaskWrapper[T]) {
	defer func() {
		p.taskWrapperPool.Put(task)
	}()

	p.mu.Lock()
	state := p.workers[workerID]
	p.mu.Unlock()

	// Create attempt-specific context, but use worker's context as parent
	attemptCtx, attemptCancel := p.createAttemptContext(task, state.ctx)
	if attemptCtx == nil {
		err := fmt.Errorf("%w: %v", ErrTaskExceedsTimeLimit, task.timeLimit)
		p.addToDeadTasks(task, err)
		return
	}
	defer attemptCancel()

	// Set context ID key/value
	attemptCtx = context.WithValue(attemptCtx, WorkerIDKey, workerID)

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
			if task.beingQueued != nil {
				task.beingQueued.Close()
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
				if task.beingQueued != nil {
					task.beingQueued.Close()
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
		if task.beingQueued != nil {
			task.beingQueued.Close()
		}
	}
}

// createAttemptContext creates the context for an attempt, considering maxDuration and timeLimit
func (p *Pool[T]) createAttemptContext(task *TaskWrapper[T], workerCtx context.Context) (context.Context, context.CancelFunc) {
	task.mu.Lock()
	defer task.mu.Unlock()

	var attemptCtx context.Context
	var attemptCancel context.CancelFunc

	// Use worker's context as base instead of task.ctx
	baseCtx := workerCtx
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	// Calculate remaining time if time limit is set
	var remainingTime time.Duration
	if task.timeLimit > 0 {
		remainingTime = task.timeLimit - task.totalDuration
		if remainingTime <= 0 {
			return nil, func() {}
		}
	}

	// Create the attempt context with appropriate timeout
	if task.maxDuration > 0 {
		if remainingTime > 0 {
			minDuration := task.maxDuration
			if remainingTime < minDuration {
				minDuration = remainingTime
			}
			attemptCtx, attemptCancel = context.WithTimeout(baseCtx, minDuration)
		} else {
			attemptCtx, attemptCancel = context.WithTimeout(baseCtx, task.maxDuration)
		}
	} else if remainingTime > 0 {
		attemptCtx, attemptCancel = context.WithTimeout(baseCtx, remainingTime)
	} else {
		attemptCtx, attemptCancel = context.WithCancel(baseCtx)
	}

	return attemptCtx, attemptCancel
}

// requeueTask updated to handle delays and keep triedWorkers intact
func (p *Pool[T]) requeueTask(task *TaskWrapper[T], err error, forceRetry bool) {
	// First take task lock
	task.mu.Lock()
	task.retries++
	totalDuration := task.totalDuration
	maxAttempts := task.retries >= p.config.attempts
	immediateRetry := task.immediateRetry
	task.mu.Unlock()

	// Check time limit first
	if task.timeLimit > 0 && totalDuration >= task.timeLimit {
		p.addToDeadTasks(task, err)
		return
	}

	// Now take pool lock for queue operations
	p.mu.Lock()
	defer p.mu.Unlock()

	if !forceRetry && p.config.attempts != UnlimitedAttempts && maxAttempts {
		p.addToDeadTasks(task, err)
		return
	}

	// Reset the per-attempt duration for the next attempt
	task.mu.Lock()
	task.durations = nil
	task.queuedAt = append(task.queuedAt, time.Now())

	// Calculate delay before next retry
	delay := p.calculateDelay(task.retries, err)
	task.scheduledTime = time.Now().Add(delay)
	task.mu.Unlock()

	logs.Debug(context.Background(), "Requeuing task", "taskData", task.data, "retries", task.retries, "delay", delay)

	if !immediateRetry {
		// Randomly select a worker that hasn't tried this task
		availableWorkers := make([]int, 0)
		task.mu.Lock()
		for workerID := range p.workers {
			if !task.triedWorkers[workerID] {
				availableWorkers = append(availableWorkers, workerID)
			}
		}
		task.mu.Unlock()

		var selectedWorkerID int
		if len(availableWorkers) > 0 {
			selectedWorkerID = availableWorkers[rand.Intn(len(availableWorkers))]
		} else {
			// If all workers have tried, reset triedWorkers and select a random worker
			task.mu.Lock()
			task.triedWorkers = make(map[int]bool)
			task.mu.Unlock()

			workerIDs := make([]int, 0, len(p.workers))
			for workerID := range p.workers {
				workerIDs = append(workerIDs, workerID)
			}
			selectedWorkerID = workerIDs[rand.Intn(len(workerIDs))]
		}

		q := p.taskQueues[selectedWorkerID]
		if q == nil {
			q = &taskQueue[T]{}
			p.taskQueues[selectedWorkerID] = q
		}
		q.Enqueue(task)
	} else {
		// Immediate retry
		task.mu.Lock()
		triedWorkersCount := len(task.triedWorkers)
		task.mu.Unlock()

		if triedWorkersCount < len(p.workers) {
			// Find a worker that hasn't tried this task
			for workerID := range p.workers {
				task.mu.Lock()
				hasTriedWorker := task.triedWorkers[workerID]
				task.mu.Unlock()

				if !hasTriedWorker {
					q := p.taskQueues[workerID]
					if q == nil {
						q = &taskQueue[T]{}
						p.taskQueues[workerID] = q
					}
					err := q.InsertAt(0, task)
					if err != nil {
						logs.Error(context.Background(), "Failed to insert task", "error", err)
					}
					break
				}
			}
		} else {
			// All workers have tried, reset triedWorkers and put at the back of a random worker's queue
			task.mu.Lock()
			task.triedWorkers = make(map[int]bool)
			task.mu.Unlock()

			workerIDs := make([]int, 0, len(p.workers))
			for workerID := range p.workers {
				workerIDs = append(workerIDs, workerID)
			}
			randomWorkerID := workerIDs[rand.Intn(len(workerIDs))]
			q := p.taskQueues[randomWorkerID]
			if q == nil {
				q = &taskQueue[T]{}
				p.taskQueues[randomWorkerID] = q
			}
			q.Enqueue(task)
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

	idx := len(p.deadTasks)
	p.deadTasks = append(p.deadTasks, deadTask)

	logs.Warn(context.Background(), "Task added to dead tasks", "taskData", task.data, "retries", task.retries, "errors", errors)

	// Increment dead tasks metric
	atomic.AddInt64(&p.metrics.DeadTasks, 1)

	p.deadTasksMutex.Unlock()

	if p.config.onNewDeadTask != nil {
		p.config.onNewDeadTask(&deadTask, idx)
	}
}

func (task *TaskWrapper[T]) reset() {
	task.retries = 0
	task.triedWorkers = make(map[int]bool)
	task.errors = make([]error, 0)
	task.durations = make([]time.Duration, 0)
	task.immediateRetry = false
	task.processedAt = []time.Time{}
	task.queuedAt = []time.Time{}
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

	task := p.taskWrapperPool.Get().(*TaskWrapper[T])
	task.reset()
	task.data = data

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

	var selectedWorkerID int
	if p.config.roundRobinDistribution {
		// Ensure nextWorkerIndex is within bounds
		safeIndex := p.nextRoundRobinWorkerIndex + 1
		safeIndex = safeIndex % len(p.workers)
		selectedWorkerID = safeIndex
		safeIndex = (safeIndex + 1) % len(p.workers)
		p.nextRoundRobinWorkerIndex = safeIndex - 1
	} else {
		// Find the worker with the smallest queue
		minQueueSize := int(^uint(0) >> 1) // Max int
		for _, workerID := range workerIDs {
			q := p.taskQueues[workerID]
			queueSize := 0
			if q != nil {
				queueSize = q.Length()
			}
			if queueSize < minQueueSize {
				minQueueSize = queueSize
				selectedWorkerID = workerID
			}
		}
	}

	task.queuedAt = append(task.queuedAt, time.Now())

	q := p.taskQueues[selectedWorkerID]
	if q == nil {
		q = &taskQueue[T]{}
		p.taskQueues[selectedWorkerID] = q
	}
	q.Enqueue(task)

	if task.beingQueued != nil {
		task.beingQueued <- struct{}{}
	}

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
	p.deadTasksMutex.RLock()
	defer p.deadTasksMutex.RUnlock()
	return append([]DeadTask[T](nil), p.deadTasks...)
}

// QueueSize returns the total number of tasks in the queue
func (p *Pool[T]) QueueSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	total := 0
	for _, q := range p.taskQueues {
		total += q.Length()
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
		delete(p.taskQueues, k)
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
		attempts:  1,
		delay:     1 * time.Nanosecond,
		maxJitter: 1 * time.Nanosecond,
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

func WithRoundRobinAssignment[T any]() Option[T] {
	return func(p *Pool[T]) {
		p.config.roundRobinDistribution = true
	}
}

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

// Worker context option
type WorkerOption[T any] func(*workerState[T])

// WithWorkerContext allows setting custom context values for a specific worker
func WithWorkerContext[T any](fn ContextFunc) WorkerOption[T] {
	return func(ws *workerState[T]) {
		if fn != nil {
			ws.ctx = fn(ws.ctx)
		}
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

type queuedNotification chan struct{}

func NewQueuedNotification() queuedNotification {
	return make(chan struct{}, 1)
}

func (p queuedNotification) Close() {
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

// WithQueued sets a channel to indicate that a task is being processed after dispatch
func WithQueued[T any](chn queuedNotification) TaskOption[T] {
	return func(t *TaskWrapper[T]) {
		t.beingQueued = chn
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
type OnNewDeadTaskFunc[T any] func(task *DeadTask[T], idx int)

// RetryIfFunc signature
type RetryIfFunc func(error) bool

// ContextFunc signature
type ContextFunc func(context.Context) context.Context

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
			return zero, rr.err
		} else {
			return rr.response, nil
		}
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
