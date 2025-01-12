package retrypool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sasha-s/go-deadlock"
	"golang.org/x/time/rate"
)

/// Pool
/// - 3 core components: Runner (run() or runWorker()), TaskQueue, Worker
/// - each doesn't know about the other, the pool manages the interactions
/// Worker
/// - Start: initial state, accept new tasks, queue is open, processing tasks
/// - Pause: won't accept new tasks, queue tasks are redistributed to other workers (if zero workers then to deadtasks), finish current task (might be retried or go to deadtasks), out of any operations, paused while triggering OnPause
/// - Resume: accept new tasks, queue is open again, continue processing, resumed while triggering OnResume
/// - Remove: no more new tasks, queue tasks are redistribute to other workers (if zero workers then to deadtasks), finish current task (might be retried or go to deadtasks), out of any operations, removed from the Pool while triggering OnRemove

// Initialize deadlock detection
func init() {
	// ATTENTION TO MYSELF FROM THE FUTURE
	// Also see that we commented all the logs, it's on purpose for the performances (for now we mogged the old retrypool).
	// DO NOT REMOVE THAT
	// When you're development and debugging, you MUST replace all `sync.` with their `deadlock.` counterparts to allow you to detect deadlocks!!
	deadlock.Opts.DeadlockTimeout = time.Second * 2
	deadlock.Opts.OnPotentialDeadlock = func() {
		fmt.Println("Potential deadlock detected")
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		fmt.Printf("Stack trace:\n%s\n", string(buf[:n]))
	}
}

// Worker interface for task processing
type Worker[T any] interface {
	Run(ctx context.Context, data T) error
}

type WorkerFactory[T any] func() Worker[T]

// TaskState represents the state of a task
type TaskState int

const (
	TaskStateCreated TaskState = iota
	TaskStatePending
	TaskStateQueued
	TaskStateRunning
	TaskStateCompleted
	TaskStateFailed
	TaskStateDead
)

func (s TaskState) String() string {
	switch s {
	case TaskStateCreated:
		return "Created"
	case TaskStatePending:
		return "Pending"
	case TaskStateQueued:
		return "Queued"
	case TaskStateRunning:
		return "Running"
	case TaskStateCompleted:
		return "Completed"
	case TaskStateFailed:
		return "Failed"
	case TaskStateDead:
		return "Dead"
	default:
		return "Unknown"
	}
}

// TaskAction represents the action to take for a failed task
type TaskAction int

const (
	TaskActionRetry          TaskAction = iota + 1 // Task will retry using its own state
	TaskActionForceRetry                           // Force a retry, ignoring retry limits
	TaskActionRemove                               // Remove the task and recycle resources
	TaskActionAddToDeadTasks                       // Add the task to dead tasks
)

// NoWorkerPolicy defines the behavior when no workers are available
type NoWorkerPolicy int

const (
	NoWorkerPolicyReject         NoWorkerPolicy = iota // Default behavior: Reject the task submission with an error
	NoWorkerPolicyAddToDeadTasks                       // Add the task to dead tasks
)

// Predefined errors
var (
	ErrPoolClosed           = errors.New("pool is closed")
	ErrRateLimitExceeded    = errors.New("rate limit exceeded")
	ErrNoWorkersAvailable   = errors.New("no workers available")
	ErrInvalidWorkerID      = errors.New("invalid worker ID")
	ErrMaxQueueSizeExceeded = errors.New("max queue size exceeded")
	ErrTaskTimeout          = errors.New("task timeout")
)

// TaskStateTransition represents a state change
type TaskStateTransition[T any] struct {
	FromState TaskState
	ToState   TaskState
	Task      *Task[T]
	Reason    string
	Timestamp time.Time
}

// stateMetrics tracks counts for each state
type stateMetrics struct {
	counts    map[TaskState]*atomic.Int64
	mu        sync.RWMutex
	callbacks map[TaskState][]func(interface{})
}

var allTaskStates = []TaskState{
	TaskStateCreated,
	TaskStatePending,
	TaskStateQueued,
	TaskStateRunning,
	TaskStateCompleted,
	TaskStateFailed,
	TaskStateDead,
}

func newStateMetrics() *stateMetrics {
	sm := &stateMetrics{
		counts:    make(map[TaskState]*atomic.Int64),
		callbacks: make(map[TaskState][]func(interface{})),
	}

	for _, state := range allTaskStates {
		sm.counts[state] = &atomic.Int64{}
	}
	return sm
}

type taskQueues[T any] struct {
	mu     deadlock.Mutex
	queues map[int]TaskQueue[T]
}

func newTaskQueues[T any]() *taskQueues[T] {
	return &taskQueues[T]{
		queues: make(map[int]TaskQueue[T]),
	}
}

// Methods for the taskQueues type
func (tq *taskQueues[T]) get(id int) (TaskQueue[T], bool) {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	q, ok := tq.queues[id]
	return q, ok
}

func (tq *taskQueues[T]) set(id int, queue TaskQueue[T]) {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	tq.queues[id] = queue
}

func (tq *taskQueues[T]) delete(id int) {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	delete(tq.queues, id)
}

func (tq *taskQueues[T]) getOrCreate(id int, creator func() TaskQueue[T]) TaskQueue[T] {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	if q, ok := tq.queues[id]; ok {
		return q
	}
	q := creator()
	tq.queues[id] = q
	return q
}

func (tq *taskQueues[T]) range_(f func(id int, q TaskQueue[T]) bool) {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	for id, q := range tq.queues {
		if !f(id, q) {
			break
		}
	}
}

func (tq *taskQueues[T]) metrics() map[int]int {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	m := make(map[int]int, len(tq.queues))
	for id, q := range tq.queues {
		m[id] = q.Length()
	}
	return m
}

// Pool manages a set of workers and tasks
type Pool[T any] struct {
	workers               map[int]*workerState[T]
	nextWorkerID          int
	taskQueues            *taskQueues[T]
	taskQueueType         TaskQueueType
	mu                    deadlock.Mutex
	cond                  *sync.Cond
	stopped               bool
	ctx                   context.Context
	cancel                context.CancelFunc
	config                Config[T]
	metrics               Metrics
	limiter               *rate.Limiter
	taskPool              sync.Pool
	deadTasks             []*DeadTask[T]
	deadMu                deadlock.Mutex
	roundRobinIndex       atomic.Int64
	totalProcessing       atomic.Int64
	totalQueueSize        atomic.Int64
	availableWorkers      atomic.Int64
	deadTaskNotifications chan int
	logger                Logger
	stateMetrics          *stateMetrics
	workerQueues          map[int]*atomic.Int64
	queueMu               sync.RWMutex
	getData               func(T) interface{}
}

// New initializes the Pool with given workers and options
func New[T any](ctx context.Context, workers []Worker[T], options ...Option[T]) *Pool[T] {
	ctx, cancel := context.WithCancel(ctx)

	pool := &Pool[T]{
		workers:               make(map[int]*workerState[T]),
		nextWorkerID:          0,
		taskQueues:            newTaskQueues[T](),
		taskQueueType:         TaskQueueTypeSlice, // default one // TODO: make this configurable
		config:                newDefaultConfig[T](),
		ctx:                   ctx,
		cancel:                cancel,
		logger:                NewLogger(slog.LevelDebug),
		deadTaskNotifications: make(chan int, 1000), // todo: make this configurable
		stateMetrics:          newStateMetrics(),
		workerQueues:          make(map[int]*atomic.Int64),
	}

	pool.logger.Disable()

	pool.logger.Info(ctx, "Creating new Pool", "workers_count", len(workers))

	for _, option := range options {
		option(pool)
	}

	pool.logger.Debug(ctx, "Pool configuration", "config", pool.config)

	pool.cond = sync.NewCond(&pool.mu)

	pool.taskPool = sync.Pool{
		New: func() interface{} {
			return &Task[T]{}
		},
	}

	// Start dead task handler if callback is set
	if pool.config.onDeadTask != nil {
		go pool.handleDeadTaskNotifications(ctx)
	}

	for _, worker := range workers {
		// In asynchronous mode, that function will start the worker in a new goroutine
		err := pool.Add(worker, nil)
		if err != nil {
			pool.logger.Error(ctx, "Failed to add worker", "error", err)
		}
	}

	// In synchronous mode, we want the Pool to have ONE goroutine but it manage ALL the workers and ALL the same rules
	if !pool.config.async {
		pool.logger.Debug(ctx, "Starting pool in synchronous mode")
		go pool.run()
	}

	return pool
}

func (p *Pool[T]) IsAsync() bool {
	return p.config.async
}

func (p *Pool[T]) IsRoundRobin() bool {
	return p.config.roundRobin
}

// If you're using round robin, it might comes handy to know the current index
func (p *Pool[T]) RoundRobinIndex() int {
	return int(p.roundRobinIndex.Load())
}

// SetOnTaskSuccess allows setting the onTaskSuccess handler after pool creation
func (p *Pool[T]) SetOnTaskSuccess(handler func(data T)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.config.onTaskSuccess = handler
}

// SetOnTaskFailure allows setting the onTaskFailure handler after pool creation
func (p *Pool[T]) SetOnTaskFailure(handler func(data T, err error) TaskAction) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.config.onTaskFailure = handler
}

// SetOnTaskAttempt allows setting the onTaskAttempt handler after pool creation
func (p *Pool[T]) SetOnTaskAttempt(handler func(task *Task[T], workerID int)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.config.onTaskAttempt = handler
}

type MetricsSnapshot[T any] struct {
	TasksSubmitted int64
	TasksProcessed int64
	TasksSucceeded int64
	TasksFailed    int64
	DeadTasks      int64
	TaskQueues     map[int]int
	Workers        map[int]State[T]
}

func (p *Pool[T]) GetMetricsSnapshot() MetricsSnapshot[T] {
	p.mu.Lock()
	defer p.mu.Unlock()

	metrics := p.taskQueues.metrics()

	workers := map[int]State[T]{}
	p.rangeWorkers(func(workerID int, state State[T]) bool {
		workers[workerID] = state
		return true
	})

	return MetricsSnapshot[T]{
		TasksSubmitted: p.metrics.TasksSubmitted.Load(),
		TasksProcessed: p.metrics.TasksProcessed.Load(),
		TasksSucceeded: p.metrics.TasksSucceeded.Load(),
		TasksFailed:    p.metrics.TasksFailed.Load(),
		DeadTasks:      p.metrics.DeadTasks.Load(),
		TaskQueues:     metrics,
		Workers:        workers,
	}
}

// newWorkerID generates a new worker ID
func (p *Pool[T]) newWorkerID() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	workerID := p.nextWorkerID
	p.nextWorkerID++
	return workerID
}

// Option type for configuring the Pool
type Option[T any] func(*Pool[T])

// WithLogger sets the logger for the pool and all its components
func WithLogger[T any](logger Logger) Option[T] {
	return func(p *Pool[T]) {
		p.logger = logger
	}
}

func WithGetData[T any](getData func(T) interface{}) Option[T] {
	return func(p *Pool[T]) {
		p.getData = getData
	}
}

// WithAttempts sets the maximum number of attempts
func WithAttempts[T any](attempts int) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting attempts", "attempts", attempts)
		p.config.attempts = attempts
	}
}

// WithOnPanic sets a custom panic handler for the pool
func WithOnPanic[T any](handler func(recovery interface{}, stackTrace string)) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting OnPanic handler")
		p.config.onPanic = handler
	}
}

// WithOnWorkerPanic sets a custom panic handler for workers
func WithOnWorkerPanic[T any](handler func(workerID int, recovery interface{}, stackTrace string)) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting OnWorkerPanic handler")
		p.config.onWorkerPanic = handler
	}
}

// WithDelay sets the delay between retries
func WithDelay[T any](delay time.Duration) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting delay", "delay", delay)
		p.config.delay = delay
	}
}

// WithMaxDelay sets the maximum delay between retries
func WithMaxDelay[T any](maxDelay time.Duration) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting max delay", "max_delay", maxDelay)
		p.config.maxDelay = maxDelay
	}
}

// WithMaxJitter sets the maximum jitter for delay between retries
func WithMaxJitter[T any](maxJitter time.Duration) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting max jitter", "max_jitter", maxJitter)
		p.config.maxJitter = maxJitter
	}
}

// WithDelayFunc sets a custom function to determine the delay between retries
func WithDelayFunc[T any](delayFunc DelayFunc[T]) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting DelayFunc")
		p.config.delayFunc = delayFunc
	}
}

// WithRetryPolicy sets a custom retry policy for the pool
func WithRetryPolicy[T any](policy RetryPolicy[T]) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting RetryPolicy")
		p.config.retryPolicy = policy
	}
}

// WithMaxQueueSize sets the maximum queue size for the pool
func WithMaxQueueSize[T any](size int) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting max queue size", "size", size)
		p.config.maxQueueSize = size
	}
}

// WithOnDeadTask sets a callback when a task is added to dead tasks
func WithOnDeadTask[T any](handler func(deadTaskIndex int)) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting OnDeadTask handler")
		p.config.onDeadTask = handler
	}
}

// WithDeadTasksLimit sets the limit for dead tasks
func WithDeadTasksLimit[T any](limit int) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting dead tasks limit", "limit", limit)
		p.config.deadTasksLimit = limit
	}
}

// WithIfRetry sets the function to determine if an error is retryable
func WithIfRetry[T any](retryIf func(err error) bool) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting retry condition function")
		p.config.retryIf = retryIf
	}
}

// WithNoWorkerPolicy sets the policy when no workers are available
func WithNoWorkerPolicy[T any](policy NoWorkerPolicy) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting NoWorkerPolicy", "policy", policy)
		p.config.noWorkerPolicy = policy
	}
}

// WithRateLimit sets the rate limit for task submissions. In async mode,
// the first burst of tasks (up to 2 * number of workers) may be processed
// immediately before the rate limit takes full effect. This provides faster
// startup while maintaining the desired steady-state rate.
func WithRateLimit[T any](rps float64) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting rate limit", "requests_per_second", rps)
		// Allow burst size to be the number of workers * 2, minimum of 1
		burstSize := len(p.workers) * 2
		if burstSize < 1 {
			burstSize = 1
		}
		p.limiter = rate.NewLimiter(rate.Limit(rps), burstSize)
	}
}

// WithOnTaskSuccess sets a callback for successful task completion
func WithOnTaskSuccess[T any](handler func(data T)) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting OnTaskSuccess handler")
		p.config.onTaskSuccess = handler
	}
}

// WithOnTaskFailure sets a callback for task failure
func WithOnTaskFailure[T any](handler func(data T, err error) TaskAction) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting OnTaskFailure handler")
		p.config.onTaskFailure = handler
	}
}

// WithSynchronousMode sets the pool to synchronous mode
func WithSynchronousMode[T any]() Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting pool to synchronous mode")
		p.config.async = false
	}
}

// WithRoundRobinDistribution sets the task distribution to round-robin
func WithRoundRobinDistribution[T any]() Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting round-robin task distribution")
		p.config.roundRobin = true
	}
}

func WithLoopTicker[T any](d time.Duration) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting loop ticker", "duration", d)
		p.config.loopTicker = d
	}
}

// WithTaskQueueType sets the type of task queue
func WithTaskQueueType[T any](queueType TaskQueueType) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting task queue type", "queue_type", queueType)
		p.taskQueueType = queueType
	}
}

// WithOnTaskAttempt sets a callback that is called whenever a worker attempts a task
func WithOnTaskAttempt[T any](handler func(task *Task[T], workerID int)) Option[T] {
	return func(p *Pool[T]) {
		p.logger.Debug(p.ctx, "Setting OnTaskAttempt handler")
		p.config.onTaskAttempt = handler
	}
}

// updateRateLimiterBurst updates the rate limiter's burst size based on active workers
func (p *Pool[T]) updateRateLimiterBurst() {
	if p.limiter == nil {
		return
	}

	// Calculate active workers
	activeWorkers := int(p.availableWorkers.Load())
	burstSize := activeWorkers * 2
	if burstSize < 1 {
		burstSize = 1
	}

	currentRate := p.limiter.Limit()
	p.limiter = rate.NewLimiter(currentRate, burstSize)

	p.logger.Debug(p.ctx, "Updated rate limiter burst size", "active_workers", activeWorkers, "burst_size", burstSize)
}

// Config holds configurations for the pool
type Config[T any] struct {
	attempts       int
	delay          time.Duration
	maxDelay       time.Duration
	maxJitter      time.Duration
	delayFunc      DelayFunc[T]
	retryPolicy    RetryPolicy[T]
	maxQueueSize   int
	onPanic        func(recovery interface{}, stackTrace string)
	onWorkerPanic  func(workerID int, recovery interface{}, stackTrace string)
	onTaskSuccess  func(data T)
	onTaskFailure  func(data T, err error) TaskAction
	onDeadTask     func(deadTaskIndex int)
	onTaskAttempt  func(task *Task[T], workerID int)
	deadTasksLimit int
	retryIf        func(err error) bool
	roundRobin     bool
	noWorkerPolicy NoWorkerPolicy
	loopTicker     time.Duration
	async          bool
}

// newDefaultConfig initializes default configurations
func newDefaultConfig[T any]() Config[T] {
	return Config[T]{
		attempts:    -1, // Unlimited attempts by default
		delay:       time.Millisecond,
		retryPolicy: nil,
		retryIf: func(err error) bool {
			return err != nil
		},
		async:          true,
		noWorkerPolicy: NoWorkerPolicyReject,
	}
}

// DelayFunc defines a function to calculate delay before retrying a task
type DelayFunc[T any] func(retries int, err error, config *Config[T]) time.Duration

// RetryPolicy defines an interface for retry policies
type RetryPolicy[T any] interface {
	ComputeDelay(retries int, err error, config *Config[T]) time.Duration
}

// ExponentialBackoffRetryPolicy implements exponential backoff with jitter
type ExponentialBackoffRetryPolicy[T any] struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	MaxJitter time.Duration
}

// ComputeDelay computes the delay using exponential backoff with jitter
func (p ExponentialBackoffRetryPolicy[T]) ComputeDelay(retries int, err error, config *Config[T]) time.Duration {
	delay := p.BaseDelay * (1 << retries)
	if delay > p.MaxDelay {
		delay = p.MaxDelay
	}
	jitter := time.Duration(rand.Int63n(int64(p.MaxJitter)))
	return delay + jitter
}

// FixedDelayRetryPolicy implements a fixed delay between retries
type FixedDelayRetryPolicy[T any] struct {
	Delay     time.Duration
	MaxJitter time.Duration
}

// ComputeDelay computes the delay using fixed delay with jitter
func (p FixedDelayRetryPolicy[T]) ComputeDelay(retries int, err error, config *Config[T]) time.Duration {
	if p.MaxJitter <= 0 {
		return p.Delay
	}
	jitter := time.Duration(rand.Int63n(int64(p.MaxJitter)))
	return p.Delay + jitter
}

// Metrics holds metrics for the pool
type Metrics struct {
	TasksSubmitted atomic.Int64
	TasksProcessed atomic.Int64
	TasksSucceeded atomic.Int64
	TasksFailed    atomic.Int64
	DeadTasks      atomic.Int64
}

// Task represents a task in the pool
type Task[T any] struct {
	mu                deadlock.Mutex
	data              T
	retries           int
	totalDuration     time.Duration
	attemptStartTime  time.Time
	durations         []time.Duration
	errors            []error
	deadReason        string
	state             TaskState
	totalTimeout      time.Duration
	attemptTimeout    time.Duration
	notifiedProcessed *ProcessedNotification
	notifiedQueued    *QueuedNotification
	queuedCb          func()
	runningCb         func()
	processedCb       func()
	immediateRetry    bool
	bounceRetry       bool
	attemptedWorkers  map[int]struct{} // Track workers that attempted this task
	lastWorkerID      int              // Last worker that processed this task
	queuedAt          []time.Time      // When task entered different queues
	processedAt       []time.Time      // When task started processing
	stateHistory      []*TaskStateTransition[T]
}

func (t *Task[T]) GetData() T {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.data
}

func (t *Task[T]) GetAttemptedWorkers() []int {
	t.mu.Lock()
	defer t.mu.Unlock()

	ids := make([]int, 0, len(t.attemptedWorkers))
	for id := range t.attemptedWorkers {
		ids = append(ids, id)
	}
	return ids
}

// GetFreeWorkers returns a list of worker IDs that have no tasks in their queue
func (p *Pool[T]) GetFreeWorkers() []int {
	p.mu.Lock()
	defer p.mu.Unlock()

	freeWorkers := []int{}
	for id, state := range p.workers {
		state.mu.Lock()
		isPaused := state.paused.Load()
		isRemoved := state.removed.Load()
		hasCurrentTask := state.currentTask != nil
		state.mu.Unlock()

		if !isPaused && !isRemoved {
			if queue, ok := p.taskQueues.get(id); ok && queue.Length() == 0 && !hasCurrentTask {
				freeWorkers = append(freeWorkers, id)
			}
		}
	}

	return freeWorkers
}

// SubmitToFreeWorker attempts to submit a task to a free worker
func (p *Pool[T]) SubmitToFreeWorker(taskData T, options ...TaskOption[T]) error {

	// Get the list of free workers without holding the lock
	freeWorkers := p.GetFreeWorkers()

	p.mu.Lock()
	defer p.mu.Unlock()

	if len(freeWorkers) == 0 {
		p.logger.Warn(p.ctx, "No free workers available for task submission", "task_data", taskData)
		return ErrNoWorkersAvailable
	}

	// Select a random free worker
	selectedWorkerID := freeWorkers[rand.Intn(len(freeWorkers))]
	q := p.taskQueues.getOrCreate(selectedWorkerID, func() TaskQueue[T] {
		return p.NewTaskQueue(p.taskQueueType)
	})

	task := p.taskPool.Get().(*Task[T])
	*task = Task[T]{data: taskData, state: TaskStateCreated}

	for _, option := range options {
		option(task)
	}

	if err := p.TransitionTaskState(task, TaskStatePending, "SubmittedToFreeWorker"); err != nil {
		return err
	}

	p.logger.Debug(p.ctx, "Submitting task to free worker", "worker_id", selectedWorkerID, "task_data", taskData)
	p.metrics.TasksSubmitted.Add(1)
	p.UpdateTotalQueueSize(1)
	q.Enqueue(task)
	p.UpdateWorkerQueueSize(selectedWorkerID, 1)

	if task.notifiedQueued != nil {
		task.notifiedQueued.Notify()
	}
	if task.queuedCb != nil {
		task.queuedCb()
	}

	if err := p.TransitionTaskState(task, TaskStateQueued, "Task enqueued to free worker"); err != nil {
		return err
	}

	// Signal differently based on mode
	if p.config.async {
		p.cond.Broadcast()
	} else {
		p.cond.Signal()
	}

	return nil
}

// TransitionTaskState handles state changes and maintains counts
func (p *Pool[T]) TransitionTaskState(task *Task[T], to TaskState, reason string) error {
	task.mu.Lock()
	from := task.state

	if !isValidTransition(from, to) {
		task.mu.Unlock()
		return fmt.Errorf("invalid state transition from %s to %s", from, to)
	}

	transition := &TaskStateTransition[T]{
		FromState: from,
		ToState:   to,
		Task:      task,
		Reason:    reason,
		Timestamp: time.Now(),
	}

	task.state = to
	task.stateHistory = append(task.stateHistory, transition)
	task.mu.Unlock()

	fromCounter, fromExists := p.stateMetrics.counts[from]
	if !fromExists {
		return fmt.Errorf("stateMetrics.counts does not have from state: %v", from)
	}
	fromCounter.Add(-1)

	toCounter, toExists := p.stateMetrics.counts[to]
	if !toExists {
		return fmt.Errorf("stateMetrics.counts does not have to state: %v", to)
	}
	toCounter.Add(1)

	p.stateMetrics.mu.RLock()
	if callbacks, exists := p.stateMetrics.callbacks[to]; exists {
		for _, cb := range callbacks {
			go cb(transition)
		}
	}
	p.stateMetrics.mu.RUnlock()

	return nil
}

func isValidTransition(from, to TaskState) bool {
	switch from {
	case TaskStateCreated:
		return to == TaskStatePending || to == TaskStateDead
	case TaskStatePending:
		return to == TaskStateQueued || to == TaskStateDead
	case TaskStateQueued:
		return to == TaskStateRunning || to == TaskStateDead
	case TaskStateRunning:
		return to == TaskStateCompleted || to == TaskStateFailed || to == TaskStateDead
	case TaskStateFailed:
		return to == TaskStateQueued || to == TaskStateDead
	case TaskStateCompleted, TaskStateDead:
		return false
	default:
		return false
	}
}

// DeadTask represents a task that has failed permanently
type DeadTask[T any] struct {
	Data          T
	Retries       int
	TotalDuration time.Duration
	Errors        []error
	Reason        string
	StateHistory  []*TaskStateTransition[T]
}

// Worker interface for task processing
// Already defined earlier

// TaskOption functions for configuring individual tasks
type TaskOption[T any] func(*Task[T])

// WithImmediateRetry allows the submitted task to retry immediately
func WithImmediateRetry[T any]() TaskOption[T] {
	return func(t *Task[T]) {
		t.immediateRetry = true
	}
}

// WithBounceRetry enables retry on different workers
func WithBounceRetry[T any]() TaskOption[T] {
	return func(t *Task[T]) {
		t.bounceRetry = true
		t.attemptedWorkers = make(map[int]struct{})
	}
}

// WithDuration sets a per-attempt time limit for the task
func WithDuration[T any](d time.Duration) TaskOption[T] {
	return func(t *Task[T]) {
		t.attemptTimeout = d
	}
}

// WithTimeout sets a total time limit for the task
func WithTimeout[T any](d time.Duration) TaskOption[T] {
	return func(t *Task[T]) {
		t.totalTimeout = d
	}
}

// WithProcessedNotification sets a notification for when a task is processed
func WithProcessedNotification[T any](n *ProcessedNotification) TaskOption[T] {
	return func(t *Task[T]) {
		t.notifiedProcessed = n
	}
}

// WithQueuedNotification sets a notification for when a task is queued
func WithQueuedNotification[T any](n *QueuedNotification) TaskOption[T] {
	return func(t *Task[T]) {
		t.notifiedQueued = n
	}
}

// WithQueuedCb sets a callback for when a task is queued
func WithQueuedCb[T any](cb func()) TaskOption[T] {
	return func(t *Task[T]) {
		t.queuedCb = cb
	}
}

// WithProcessedCb sets a callback for when a task is processed
func WithProcessedCb[T any](cb func()) TaskOption[T] {
	return func(t *Task[T]) {
		t.processedCb = cb
	}
}

// WithRunningCb sets a callback for when a task is running
func WithRunningCb[T any](cb func()) TaskOption[T] {
	return func(t *Task[T]) {
		t.runningCb = cb
	}
}

// Submit allows the developer to send data directly without pre-allocation.
func (p *Pool[T]) Submit(data T, options ...TaskOption[T]) error {

	if p.availableWorkers.Load() == 0 {
		p.logger.Error(p.ctx, "Attempted to submit task with no workers available")

		switch p.config.noWorkerPolicy {
		case NoWorkerPolicyAddToDeadTasks:
			p.logger.Warn(p.ctx, "No workers available, adding task to dead tasks")
			t := p.taskPool.Get().(*Task[T])
			*t = Task[T]{data: data}
			t.deadReason = "No workers available"
			p.addDeadTask(t)
			return nil
		default:
			return ErrNoWorkersAvailable
		}
	}

	if p.limiter != nil {
		if err := p.limiter.Wait(p.ctx); err != nil {
			p.logger.Warn(p.ctx, "Rate limit wait failed", "error", err)
			return ErrRateLimitExceeded
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.config.maxQueueSize > 0 && p.metrics.TasksSubmitted.Load()-p.metrics.TasksSucceeded.Load()-p.DeadTaskCount() >= int64(p.config.maxQueueSize) {
		p.logger.Warn(p.ctx, "Max queue size exceeded")
		return ErrMaxQueueSizeExceeded
	}

	t := p.taskPool.Get().(*Task[T])
	*t = Task[T]{data: data, state: TaskStateCreated}

	for _, option := range options {
		option(t)
	}

	if err := p.TransitionTaskState(t, TaskStatePending, "Submitted"); err != nil {
		return err
	}

	p.metrics.TasksSubmitted.Add(1)
	p.UpdateTotalQueueSize(1)
	err := p.submitTask(t)
	if err != nil {
		p.logger.Warn(p.ctx, "Submit failed", "error", err)
		return err
	}

	return nil
}

// submitTask submits a task to the pool
func (p *Pool[T]) submitTask(task *Task[T]) error {
	if p.stopped {
		p.logger.Error(p.ctx, "Attempted to submit task to stopped pool", "task_data", task.data)
		return ErrPoolClosed
	}

	if len(p.workers) == 0 {
		p.logger.Error(p.ctx, "Attempted to submit task with no workers available", "task_data", task.data)
		return ErrNoWorkersAvailable
	}

	p.logger.Debug(p.ctx, "Submitting task", "task_data", task.data, "immediate_retry", task.immediateRetry, "bounce_retry", task.bounceRetry)

	selectedWorkerID := p.selectWorkerForTask(task)
	if selectedWorkerID == -1 {
		// No suitable worker found
		if task.bounceRetry {
			p.logger.Debug(p.ctx, "All workers attempted, resetting attempted workers list", "task_data", task.data)
			task.attemptedWorkers = make(map[int]struct{})
			selectedWorkerID = p.selectWorkerForTask(task)
		}
		if selectedWorkerID == -1 {
			p.logger.Error(p.ctx, "No available workers for task", "task_data", task.data)
			return ErrNoWorkersAvailable
		}
	}

	p.logger.Debug(p.ctx, "Selected worker for task", "worker_id", selectedWorkerID)
	q := p.taskQueues.getOrCreate(selectedWorkerID, func() TaskQueue[T] {
		return p.NewTaskQueue(p.taskQueueType)
	})

	// Record when the task is queued
	task.mu.Lock()
	task.queuedAt = append(task.queuedAt, time.Now())
	task.mu.Unlock()

	p.UpdateWorkerQueueSize(selectedWorkerID, 1)

	if task.immediateRetry {
		p.logger.Debug(p.ctx, "Enqueueing task with immediate retry", "task_data", task.data, "worker_id", selectedWorkerID)
		err := q.PriorityEnqueue(task)
		if err != nil {
			p.logger.Error(p.ctx, "Failed to priority enqueue task", "task_data", task.data, "worker_id", selectedWorkerID, "error", err)
			p.UpdateWorkerQueueSize(selectedWorkerID, -1)
			return err
		}
	} else {
		p.logger.Debug(p.ctx, "Enqueueing task normally", "task_data", task.data, "worker_id", selectedWorkerID)
		q.Enqueue(task)
	}

	if task.notifiedQueued != nil {
		task.notifiedQueued.Notify()
	}
	if task.queuedCb != nil {
		task.queuedCb()
	}

	if err := p.TransitionTaskState(task, TaskStateQueued, "Task enqueued"); err != nil {
		p.logger.Warn(p.ctx, "Failed to transition task state", "error", err)
	}

	// Let's add more detailed logging here
	p.logger.Debug(p.ctx, "Task enqueued and signaling", "async_mode", p.config.async, "worker_id", selectedWorkerID, "queue_length", q.Length(), "immediate_retry", task.immediateRetry, "bounce_retry", task.bounceRetry)

	// Signal differently based on mode
	if p.config.async {
		p.logger.Debug(p.ctx, "Signaling workers that a new task is available")
		p.cond.Broadcast()
	} else {
		p.logger.Debug(p.ctx, "Signaling sync mode that a new task is available")
		p.cond.Signal()
	}

	return nil
}

// selectWorkerForTask selects an appropriate worker for the task
// p.mu.Lock is already held by caller
func (p *Pool[T]) selectWorkerForTask(task *Task[T]) int {
	p.logger.Debug(p.ctx, "Selecting worker for task", "task_data", task.data, "immediate_retry", task.immediateRetry, "bounce_retry", task.bounceRetry)

	availableWorkers := make([]int, 0, len(p.workers))
	for id, state := range p.workers {
		if !state.paused.Load() && !state.removed.Load() {
			if task.bounceRetry {
				// Skip workers that have already attempted this task
				if _, attempted := task.attemptedWorkers[id]; !attempted {
					availableWorkers = append(availableWorkers, id)
					p.logger.Debug(p.ctx, "Found available worker for bounce retry", "worker_id", id)
				}
			} else {
				availableWorkers = append(availableWorkers, id)
			}
		}
	}

	if len(availableWorkers) == 0 {
		p.logger.Debug(p.ctx, "No available workers found for task", "task_data", task.data)
		return -1
	}

	sort.Ints(availableWorkers)

	if task.immediateRetry && !task.bounceRetry {
		// Try to use the last worker if available
		for _, id := range availableWorkers {
			if id == task.lastWorkerID {
				p.logger.Debug(p.ctx, "Selected same worker for immediate retry", "worker_id", id)
				return id
			}
		}
	}

	// Use round-robin or least-loaded selection for other cases
	if p.config.roundRobin {
		currentIndex := p.roundRobinIndex.Add(1) - 1 // Get current index and increment
		selectedID := availableWorkers[int(currentIndex)%len(availableWorkers)]
		p.logger.Debug(p.ctx, "Selected worker using round-robin", "worker_id", selectedID)
		return selectedID
	}

	// Select least loaded worker
	minTasks := int(^uint(0) >> 1)
	selectedWorkerID := availableWorkers[0]
	for _, id := range availableWorkers {
		if q, ok := p.taskQueues.get(id); ok {
			if qLen := q.Length(); qLen < minTasks {
				minTasks = qLen
				selectedWorkerID = id
				p.logger.Debug(p.ctx, "Found worker with fewer tasks", "worker_id", id, "queue_length", qLen)
			}
		}
	}

	p.logger.Debug(p.ctx, "Selected worker using least-loaded strategy", "worker_id", selectedWorkerID, "queue_length", minTasks)
	return selectedWorkerID
}

// getWorkerIDs returns the list of worker IDs
func (p *Pool[T]) getWorkerIDs() []int {
	ids := make([]int, 0, len(p.workers))
	for id := range p.workers {
		ids = append(ids, id)
	}
	return ids
}

func (p *Pool[T]) UpdateWorkerQueueSize(workerID int, delta int64) {
	p.queueMu.RLock()
	if counter, exists := p.workerQueues[workerID]; exists {
		newSize := counter.Add(delta)
		if newSize < 0 {
			p.logger.Warn(p.ctx, "Queue size for worker became negative", "worker_id", workerID, "queue_size", newSize)
		}
	} else {
		p.logger.Error(p.ctx, "Invalid worker ID in UpdateWorkerQueueSize", "worker_id", workerID)
	}
	p.queueMu.RUnlock()
}

func (p *Pool[T]) GetWorkerQueueSize(workerID int) int64 {
	p.queueMu.RLock()
	defer p.queueMu.RUnlock()
	if counter, exists := p.workerQueues[workerID]; exists {
		return counter.Load()
	}
	return -1
}

func (p *Pool[T]) RangeWorkerQueues(f func(workerID int, queueSize int64) bool) {
	p.queueMu.RLock()
	defer p.queueMu.RUnlock()
	for workerID, counter := range p.workerQueues {
		if !f(workerID, counter.Load()) {
			break
		}
	}
}

func (p *Pool[T]) RangeTaskQueues(f func(workerID int, queue TaskQueue[T]) bool) {
	p.taskQueues.range_(f)
}

func (p *Pool[T]) RangeWorkers(f func(workerID int, state State[T]) bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rangeWorkers(f)
}

func (p *Pool[T]) rangeWorkers(f func(workerID int, state State[T]) bool) {
	for id, worker := range p.workers {

		hasCurrentTask := false
		if p.getData != nil {
			worker.mu.Lock()
			if worker.currentTask != nil {
				hasCurrentTask = true
			}
			worker.mu.Unlock()
		}

		var data interface{}

		if p.getData != nil {
			if hasCurrentTask {
				worker.mu.Lock()
				worker.currentTask.mu.Lock()
				if worker.currentTask != nil {
					data = p.getData(worker.currentTask.data)
				}
				worker.currentTask.mu.Unlock()
				worker.mu.Unlock()
			}
		}

		worker.mu.Lock()
		state := State[T]{
			ID:      id,
			Paused:  worker.paused.Load(),
			Removed: worker.removed.Load(),
			HasTask: worker.currentTask != nil,
		}

		if p.getData != nil {
			if hasCurrentTask {
				state.Data = data
			}
		}

		worker.mu.Unlock()
		if !f(id, state) {
			break
		}
	}
}

func (p *Pool[T]) UpdateTotalQueueSize(delta int64) {
	newSize := p.totalQueueSize.Add(delta)
	if newSize < 0 {
		p.logger.Warn(p.ctx, "Total queue size became negative", "queue_size", newSize)
	}
}

// TaskQueueType represents the type of task queue
type TaskQueueType int

const (
	TaskQueueTypeSlice TaskQueueType = iota
	TaskQueueTypeRingBuffer
	TaskQueueTypeCircularQueue
	TaskQueueTypeLinkedList
	TaskQueueTypeGrowingRingBuffer
	TaskQueueTypeGrowingCircularQueue
)

// TaskQueue defines the interface for task queues
type TaskQueue[T any] interface {
	Enqueue(task *Task[T])
	PriorityEnqueue(task *Task[T]) error // For immediate retry
	Dequeue() (*Task[T], bool)
	Length() int
	Clear()
	Drain() []*Task[T]
}

// NewTaskQueue creates a new task queue of the specified type
func (p *Pool[T]) NewTaskQueue(queueType TaskQueueType) TaskQueue[T] {
	switch queueType {
	case TaskQueueTypeSlice:
		return newSliceTaskQueue[T](p.logger)
	case TaskQueueTypeRingBuffer:
		return newRingBufferQueue[T](1024, p.logger)
	case TaskQueueTypeCircularQueue:
		return newCircularQueue[T](1024, p.logger)
	case TaskQueueTypeLinkedList:
		return &linkedListQueue[T]{logger: p.logger}
	case TaskQueueTypeGrowingRingBuffer:
		return newGrowingRingBufferQueue[T](1024, p.logger)
	case TaskQueueTypeGrowingCircularQueue:
		return newGrowingCircularQueue[T](1024, p.logger)
	default:
		return &sliceTaskQueue[T]{logger: p.logger}
	}
}

// sliceTaskQueue is a simple slice-based queue
type sliceTaskQueue[T any] struct {
	mu     deadlock.Mutex
	tasks  []*Task[T]
	logger Logger
}

func newSliceTaskQueue[T any](logger Logger) *sliceTaskQueue[T] {
	return &sliceTaskQueue[T]{logger: logger}
}

func (q *sliceTaskQueue[T]) Enqueue(task *Task[T]) {
	q.mu.Lock()
	q.tasks = append(q.tasks, task)
	// q.logger.Debug(context.Background(), "Task enqueued in sliceTaskQueue", "queue_length", len(q.tasks))
	q.mu.Unlock()
}

func (q *sliceTaskQueue[T]) PriorityEnqueue(task *Task[T]) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Insert the task after any currently processing immediate retry tasks
	insertIdx := 0
	for i, t := range q.tasks {
		if t.immediateRetry {
			insertIdx = i + 1
		} else {
			break
		}
	}

	// Insert at the determined position
	q.tasks = append(q.tasks, nil)
	copy(q.tasks[insertIdx+1:], q.tasks[insertIdx:])
	q.tasks[insertIdx] = task

	// q.logger.Debug(context.Background(), "Priority task enqueued", "queue_length", len(q.tasks), "insert_position", insertIdx)

	return nil
}

func (q *sliceTaskQueue[T]) Dequeue() (*Task[T], bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// q.logger.Debug(context.Background(), "Attempting to dequeue from sliceTaskQueue", "queue_length", len(q.tasks))

	if len(q.tasks) == 0 {
		// q.logger.Debug(context.Background(), "Dequeue called on empty sliceTaskQueue")
		return nil, false
	}

	// Log first task in queue
	// firstTask := q.tasks[0]
	// firstTask.mu.Lock()
	// q.logger.Debug(context.Background(), "First task in queue", "task_data", firstTask.data, "task_state", firstTask.state, "retries", firstTask.retries)
	// firstTask.mu.Unlock()

	task := q.tasks[0]
	q.tasks = q.tasks[1:]

	// q.logger.Debug(context.Background(), "Task dequeued from sliceTaskQueue", "queue_length", len(q.tasks))
	return task, true
}
func (q *sliceTaskQueue[T]) Length() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	length := len(q.tasks)
	// q.logger.Debug(context.Background(), "sliceTaskQueue length fetched", "queue_length", length)
	return length
}

func (q *sliceTaskQueue[T]) Clear() {
	q.mu.Lock()
	q.tasks = nil
	// q.logger.Debug(context.Background(), "sliceTaskQueue cleared")
	q.mu.Unlock()
}

func (q *sliceTaskQueue[T]) Drain() []*Task[T] {
	q.mu.Lock()
	tasks := q.tasks
	q.tasks = nil
	// q.logger.Debug(context.Background(), "sliceTaskQueue drained", "tasks_drained", len(tasks))
	q.mu.Unlock()
	return tasks
}

// linkedListQueue is a linked list-based queue
type linkedListQueue[T any] struct {
	mu     deadlock.Mutex
	head   *listNode[T]
	tail   *listNode[T]
	size   int
	logger Logger
}

type listNode[T any] struct {
	value *Task[T]
	next  *listNode[T]
}

func (q *linkedListQueue[T]) Enqueue(task *Task[T]) {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := &listNode[T]{value: task}
	if q.tail != nil {
		q.tail.next = n
		q.tail = n
	} else {
		q.head = n
		q.tail = n
	}
	q.size++
	// q.logger.Debug(context.Background(), "Task enqueued in linkedListQueue", "queue_length", q.size)
}

func (q *linkedListQueue[T]) PriorityEnqueue(task *Task[T]) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Create new node
	n := &listNode[T]{value: task}

	if q.head == nil {
		// Empty queue
		q.head = n
		q.tail = n
		// q.logger.Debug(context.Background(), "Priority task enqueued in empty linkedListQueue", "queue_length", q.size+1)
	} else {
		// Insert at head
		n.next = q.head
		q.head = n
		// q.logger.Debug(context.Background(), "Priority task enqueued at head in linkedListQueue", "queue_length", q.size+1)
	}
	q.size++
	return nil
}

func (q *linkedListQueue[T]) Dequeue() (*Task[T], bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.head == nil {
		// q.logger.Debug(context.Background(), "Dequeue called on empty linkedListQueue")
		return nil, false
	}
	task := q.head.value
	q.head = q.head.next
	if q.head == nil {
		q.tail = nil
	}
	q.size--
	// q.logger.Debug(context.Background(), "Task dequeued from linkedListQueue", "queue_length", q.size)
	return task, true
}

func (q *linkedListQueue[T]) Length() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	length := q.size
	// q.logger.Debug(context.Background(), "linkedListQueue length fetched", "queue_length", length)
	return length
}

func (q *linkedListQueue[T]) Clear() {
	q.mu.Lock()
	q.head = nil
	q.tail = nil
	q.size = 0
	// q.logger.Debug(context.Background(), "linkedListQueue cleared")
	q.mu.Unlock()
}

func (q *linkedListQueue[T]) Drain() []*Task[T] {
	q.mu.Lock()
	defer q.mu.Unlock()
	var tasks []*Task[T]
	current := q.head
	for current != nil {
		tasks = append(tasks, current.value)
		current = current.next
	}
	q.head = nil
	q.tail = nil
	q.size = 0
	// q.logger.Debug(context.Background(), "linkedListQueue drained", "tasks_drained", len(tasks))
	return tasks
}

// RingBufferQueue is a ring buffer-based queue
type RingBufferQueue[T any] struct {
	buffer []*Task[T]
	head   int
	tail   int
	size   int
	cap    int
	mu     deadlock.Mutex
	logger Logger
}

// newRingBufferQueue creates a new RingBufferQueue with the given capacity
func newRingBufferQueue[T any](capacity int, logger Logger) *RingBufferQueue[T] {
	return &RingBufferQueue[T]{buffer: make([]*Task[T], capacity), cap: capacity, logger: logger}
}

func (q *RingBufferQueue[T]) Enqueue(task *Task[T]) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.size == q.cap {
		// q.logger.Error(context.Background(), "RingBufferQueue is full")
		panic("RingBufferQueue is full")
	}
	q.buffer[q.tail] = task
	q.tail = (q.tail + 1) % q.cap
	q.size++
	// q.logger.Debug(context.Background(), "Task enqueued in RingBufferQueue", "queue_length", q.size)
}

func (q *RingBufferQueue[T]) PriorityEnqueue(task *Task[T]) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.size == q.cap {
		// q.logger.Error(context.Background(), "RingBufferQueue is full during PriorityEnqueue")
		return errors.New("RingBufferQueue is full")
	}

	// Move existing elements backward to make space at head
	if q.size > 0 {
		newHead := (q.head - 1 + q.cap) % q.cap
		q.buffer[newHead] = task
		q.head = newHead
		// q.logger.Debug(context.Background(), "Priority task enqueued in RingBufferQueue", "queue_length", q.size+1)
	} else {
		// If queue is empty, just add normally
		q.buffer[q.head] = task
		// q.logger.Debug(context.Background(), "Priority task enqueued in empty RingBufferQueue", "queue_length", 1)
	}
	q.size++
	return nil
}

func (q *RingBufferQueue[T]) Dequeue() (*Task[T], bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.size == 0 {
		// q.logger.Debug(context.Background(), "Dequeue called on empty RingBufferQueue")
		return nil, false
	}
	task := q.buffer[q.head]
	q.buffer[q.head] = nil
	q.head = (q.head + 1) % q.cap
	q.size--
	// q.logger.Debug(context.Background(), "Task dequeued from RingBufferQueue", "queue_length", q.size)
	return task, true
}

func (q *RingBufferQueue[T]) Length() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	length := q.size
	// q.logger.Debug(context.Background(), "RingBufferQueue length fetched", "queue_length", length)
	return length
}

func (q *RingBufferQueue[T]) Clear() {
	q.mu.Lock()
	for i := 0; i < q.cap; i++ {
		q.buffer[i] = nil
	}
	q.head = 0
	q.tail = 0
	q.size = 0
	// q.logger.Debug(context.Background(), "RingBufferQueue cleared")
	q.mu.Unlock()
}

func (q *RingBufferQueue[T]) Drain() []*Task[T] {
	q.mu.Lock()
	defer q.mu.Unlock()
	tasks := make([]*Task[T], 0, q.size)
	for q.size > 0 {
		task := q.buffer[q.head]
		q.buffer[q.head] = nil
		q.head = (q.head + 1) % q.cap
		q.size--
		tasks = append(tasks, task)
	}
	q.head = 0
	q.tail = 0
	// q.logger.Debug(context.Background(), "RingBufferQueue drained", "tasks_drained", len(tasks))
	return tasks
}

// GrowingRingBufferQueue is a ring buffer-based queue that grows when full
type GrowingRingBufferQueue[T any] struct {
	buffer []*Task[T]
	head   int
	tail   int
	size   int
	cap    int
	mu     deadlock.Mutex
	logger Logger
}

// newGrowingRingBufferQueue creates a new GrowingRingBufferQueue with the given initial capacity
func newGrowingRingBufferQueue[T any](initialCapacity int, logger Logger) *GrowingRingBufferQueue[T] {
	return &GrowingRingBufferQueue[T]{
		buffer: make([]*Task[T], initialCapacity),
		cap:    initialCapacity,
		logger: logger,
	}
}

func (q *GrowingRingBufferQueue[T]) Enqueue(task *Task[T]) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.size == q.cap {
		// q.logger.Debug(context.Background(), "GrowingRingBufferQueue is full, growing capacity", "current_capacity", q.cap)
		q.grow()
	}
	q.buffer[q.tail] = task
	q.tail = (q.tail + 1) % q.cap
	q.size++
	// q.logger.Debug(context.Background(), "Task enqueued in GrowingRingBufferQueue", "queue_length", q.size, "capacity", q.cap)
}

func (q *GrowingRingBufferQueue[T]) grow() {
	newCap := q.cap * 2
	// q.logger.Debug(context.Background(), "GrowingRingBufferQueue increasing capacity", "old_capacity", q.cap, "new_capacity", newCap)
	newBuffer := make([]*Task[T], newCap)
	if q.head < q.tail {
		copy(newBuffer, q.buffer[q.head:q.tail])
	} else {
		n := copy(newBuffer, q.buffer[q.head:])
		copy(newBuffer[n:], q.buffer[:q.tail])
	}
	q.buffer = newBuffer
	q.head = 0
	q.tail = q.size
	q.cap = newCap
}

func (q *GrowingRingBufferQueue[T]) PriorityEnqueue(task *Task[T]) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// If queue is full, grow it
	if q.size == q.cap {
		// q.logger.Debug(context.Background(), "GrowingRingBufferQueue is full during PriorityEnqueue, growing capacity", "current_capacity", q.cap)
		q.grow()
	}

	// Move existing elements backward to make space at head
	if q.size > 0 {
		newHead := (q.head - 1 + q.cap) % q.cap
		q.buffer[newHead] = task
		q.head = newHead
		// q.logger.Debug(context.Background(), "Priority task enqueued in GrowingRingBufferQueue", "queue_length", q.size+1, "capacity", q.cap)
	} else {
		// If queue is empty, just add normally
		q.buffer[q.head] = task
		// q.logger.Debug(context.Background(), "Priority task enqueued in empty GrowingRingBufferQueue", "queue_length", 1, "capacity", q.cap)
	}
	q.size++
	return nil
}

func (q *GrowingRingBufferQueue[T]) Dequeue() (*Task[T], bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.size == 0 {
		// q.logger.Debug(context.Background(), "Dequeue called on empty GrowingRingBufferQueue")
		return nil, false
	}
	task := q.buffer[q.head]
	q.buffer[q.head] = nil
	q.head = (q.head + 1) % q.cap
	q.size--
	// q.logger.Debug(context.Background(), "Task dequeued from GrowingRingBufferQueue", "queue_length", q.size)
	return task, true
}

func (q *GrowingRingBufferQueue[T]) Length() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	length := q.size
	// q.logger.Debug(context.Background(), "GrowingRingBufferQueue length fetched", "queue_length", length)
	return length
}

func (q *GrowingRingBufferQueue[T]) Clear() {
	q.mu.Lock()
	for i := 0; i < q.cap; i++ {
		q.buffer[i] = nil
	}
	q.head = 0
	q.tail = 0
	q.size = 0
	// q.logger.Debug(context.Background(), "GrowingRingBufferQueue cleared")
	q.mu.Unlock()
}

func (q *GrowingRingBufferQueue[T]) Drain() []*Task[T] {
	q.mu.Lock()
	defer q.mu.Unlock()
	tasks := make([]*Task[T], 0, q.size)
	for q.size > 0 {
		task := q.buffer[q.head]
		q.buffer[q.head] = nil
		q.head = (q.head + 1) % q.cap
		q.size--
		tasks = append(tasks, task)
	}
	q.head = 0
	q.tail = 0
	// q.logger.Debug(context.Background(), "GrowingRingBufferQueue drained", "tasks_drained", len(tasks))
	return tasks
}

// CircularQueue is a fixed-size circular queue
type CircularQueue[T any] struct {
	buffer []*Task[T]
	head   int
	tail   int
	size   int
	cap    int
	mu     deadlock.Mutex
	logger Logger
}

// newCircularQueue creates a new CircularQueue with the given capacity
func newCircularQueue[T any](capacity int, logger Logger) *CircularQueue[T] {
	return &CircularQueue[T]{
		buffer: make([]*Task[T], capacity),
		cap:    capacity,
		logger: logger,
	}
}

func (q *CircularQueue[T]) Enqueue(task *Task[T]) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.size == q.cap {
		// q.logger.Error(context.Background(), "CircularQueue is full")
		panic("CircularQueue is full")
	}
	q.buffer[q.tail] = task
	q.tail = (q.tail + 1) % q.cap
	q.size++
	// q.logger.Debug(context.Background(), "Task enqueued in CircularQueue", "queue_length", q.size)
}

func (q *CircularQueue[T]) PriorityEnqueue(task *Task[T]) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.size == q.cap {
		// q.logger.Error(context.Background(), "CircularQueue is full")
		return errors.New("CircularQueue is full")
	}

	// Move existing elements one position forward to make space at head
	if q.size > 0 {
		newHead := (q.head - 1 + q.cap) % q.cap
		q.buffer[newHead] = task
		q.head = newHead
	} else {
		// If queue is empty, just add normally
		q.buffer[q.head] = task
	}
	q.size++
	// q.logger.Debug(context.Background(), "Priority task enqueued in CircularQueue", "queue_length", q.size)
	return nil
}

func (q *CircularQueue[T]) Dequeue() (*Task[T], bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.size == 0 {
		// q.logger.Debug(context.Background(), "Dequeue called on empty CircularQueue")
		return nil, false
	}
	task := q.buffer[q.head]
	q.buffer[q.head] = nil
	q.head = (q.head + 1) % q.cap
	q.size--
	// q.logger.Debug(context.Background(), "Task dequeued from CircularQueue", "queue_length", q.size)
	return task, true
}

func (q *CircularQueue[T]) Length() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	length := q.size
	// q.logger.Debug(context.Background(), "CircularQueue length fetched", "queue_length", length)
	return length
}

func (q *CircularQueue[T]) Clear() {
	q.mu.Lock()
	for i := 0; i < q.cap; i++ {
		q.buffer[i] = nil
	}
	q.head = 0
	q.tail = 0
	q.size = 0
	// q.logger.Debug(context.Background(), "CircularQueue cleared")
	q.mu.Unlock()
}

func (q *CircularQueue[T]) Drain() []*Task[T] {
	q.mu.Lock()
	defer q.mu.Unlock()
	tasks := make([]*Task[T], 0, q.size)
	for q.size > 0 {
		task := q.buffer[q.head]
		q.buffer[q.head] = nil
		q.head = (q.head + 1) % q.cap
		q.size--
		tasks = append(tasks, task)
	}
	q.head = 0
	q.tail = 0
	// q.logger.Debug(context.Background(), "CircularQueue drained", "tasks_drained", len(tasks))
	return tasks
}

// GrowingCircularQueue is a circular queue that grows when full
type GrowingCircularQueue[T any] struct {
	buffer []*Task[T]
	head   int
	tail   int
	size   int
	cap    int
	mu     deadlock.Mutex
	logger Logger
}

// newGrowingCircularQueue creates a new GrowingCircularQueue with the given initial capacity
func newGrowingCircularQueue[T any](initialCapacity int, logger Logger) *GrowingCircularQueue[T] {
	return &GrowingCircularQueue[T]{
		buffer: make([]*Task[T], initialCapacity),
		cap:    initialCapacity,
		logger: logger,
	}
}

func (q *GrowingCircularQueue[T]) Enqueue(task *Task[T]) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.size == q.cap {
		// q.logger.Debug(context.Background(), "GrowingCircularQueue is full, growing capacity", "current_capacity", q.cap)
		q.grow()
	}
	q.buffer[q.tail] = task
	q.tail = (q.tail + 1) % q.cap
	q.size++
	// q.logger.Debug(context.Background(), "Task enqueued in GrowingCircularQueue", "queue_length", q.size, "capacity", q.cap)
}

func (q *GrowingCircularQueue[T]) PriorityEnqueue(task *Task[T]) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// If queue is full, grow it
	if q.size == q.cap {
		// q.logger.Debug(context.Background(), "GrowingCircularQueue is full during PriorityEnqueue, growing capacity", "current_capacity", q.cap)
		q.grow()
	}

	// Move existing elements backward to make space at head
	if q.size > 0 {
		newHead := (q.head - 1 + q.cap) % q.cap
		q.buffer[newHead] = task
		q.head = newHead
		// q.logger.Debug(context.Background(), "Priority task enqueued in GrowingCircularQueue", "queue_length", q.size+1, "capacity", q.cap)
	} else {
		// If queue is empty, just add normally
		q.buffer[q.head] = task
		// q.logger.Debug(context.Background(), "Priority task enqueued in empty GrowingCircularQueue", "queue_length", 1, "capacity", q.cap)
	}
	q.size++
	return nil
}

func (q *GrowingCircularQueue[T]) grow() {
	newCap := q.cap * 2
	// q.logger.Debug(context.Background(), "GrowingCircularQueue increasing capacity", "old_capacity", q.cap, "new_capacity", newCap)
	newBuffer := make([]*Task[T], newCap)
	if q.head < q.tail {
		copy(newBuffer, q.buffer[q.head:q.tail])
	} else if q.size > 0 {
		n := copy(newBuffer, q.buffer[q.head:])
		copy(newBuffer[n:], q.buffer[:q.tail])
	}
	q.buffer = newBuffer
	q.head = 0
	q.tail = q.size
	q.cap = newCap
}

func (q *GrowingCircularQueue[T]) Dequeue() (*Task[T], bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.size == 0 {
		// q.logger.Debug(context.Background(), "Dequeue called on empty GrowingCircularQueue")
		return nil, false
	}
	task := q.buffer[q.head]
	q.buffer[q.head] = nil
	q.head = (q.head + 1) % q.cap
	q.size--
	// q.logger.Debug(context.Background(), "Task dequeued from GrowingCircularQueue", "queue_length", q.size)
	return task, true
}

func (q *GrowingCircularQueue[T]) Length() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	length := q.size
	// q.logger.Debug(context.Background(), "GrowingCircularQueue length fetched", "queue_length", length)
	return length
}

func (q *GrowingCircularQueue[T]) Clear() {
	q.mu.Lock()
	for i := 0; i < q.cap; i++ {
		q.buffer[i] = nil
	}
	q.head = 0
	q.tail = 0
	q.size = 0
	// q.logger.Debug(context.Background(), "GrowingCircularQueue cleared")
	q.mu.Unlock()
}

func (q *GrowingCircularQueue[T]) Drain() []*Task[T] {
	q.mu.Lock()
	defer q.mu.Unlock()
	tasks := make([]*Task[T], 0, q.size)
	for q.size > 0 {
		task := q.buffer[q.head]
		q.buffer[q.head] = nil
		q.head = (q.head + 1) % q.cap
		q.size--
		tasks = append(tasks, task)
	}
	q.head = 0
	q.tail = 0
	// q.logger.Debug(context.Background(), "GrowingCircularQueue drained", "tasks_drained", len(tasks))
	return tasks
}

// workerState holds the state of a worker
type workerState[T any] struct {
	worker       Worker[T]
	id           int
	ctx          context.Context
	cancel       context.CancelFunc
	paused       atomic.Bool
	currentTask  *Task[T]
	taskQueue    TaskQueue[T]
	removed      atomic.Bool
	mu           deadlock.Mutex
	cond         *sync.Cond
	async        bool
	methodsCache map[string]*reflect.Value
	done         chan struct{}
	logger       Logger
}

type State[T any] struct {
	ID      int
	Paused  bool
	Removed bool
	HasTask bool
	Data    interface{}
}

// Add adds a new worker to the pool
func (p *Pool[T]) Add(worker Worker[T], queue TaskQueue[T]) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if worker == nil {
		p.logger.Error(p.ctx, "Cannot add nil worker")
		return errors.New("worker cannot be nil")
	}

	workerID := p.nextWorkerID
	p.nextWorkerID++

	if queue == nil {
		queue = p.NewTaskQueue(p.taskQueueType)
	}

	workerCtx, cancel := context.WithCancel(p.ctx)

	state := &workerState[T]{
		worker:       worker,
		id:           workerID,
		ctx:          workerCtx,
		cancel:       cancel,
		taskQueue:    queue,
		async:        p.config.async,
		done:         make(chan struct{}),
		methodsCache: getWorkerMethods(worker),
		// logger:       p.logger.WithFields(map[string]any{"worker_id": workerID}),
	}

	state.cond = sync.NewCond(&state.mu)
	p.workers[workerID] = state
	p.taskQueues.set(workerID, queue)
	p.workerQueues[workerID] = &atomic.Int64{}

	p.logger.Info(p.ctx, "Added new worker", "worker_id", workerID)

	// Set worker ID if the struct has an ID field
	setWorkerID(worker, workerID)

	// Trigger OnStart if exists
	state.callMethod("OnStart", state.ctx)

	// Before returning, update availableWorkers
	if !state.paused.Load() && !state.removed.Load() {
		p.availableWorkers.Add(1)
	}

	// Redistribute tasks to include the new worker
	p.redistributeAllTasks()

	p.updateRateLimiterBurst()

	if p.config.async {
		go p.runWorker(state)
	}

	return nil
}

func setWorkerID[T any](worker Worker[T], id int) {
	val := reflect.ValueOf(worker)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() == reflect.Struct {
		field := val.FieldByName("ID")
		if field.IsValid() && field.CanSet() && field.Kind() == reflect.Int {
			field.SetInt(int64(id))
		}
	}
}

func getWorkerMethods[T any](worker Worker[T]) map[string]*reflect.Value {
	methods := make(map[string]*reflect.Value)
	rv := reflect.ValueOf(worker)
	methodNames := []string{"OnStart", "OnStop", "OnRemove", "OnPause", "OnResume"}
	for _, name := range methodNames {
		method := rv.MethodByName(name)
		if method.IsValid() {
			methods[name] = &method
		}
	}
	return methods
}

// Remove removes a worker from the pool
func (p *Pool[T]) Remove(id int) error {
	p.mu.Lock()
	state, exists := p.workers[id]
	if !exists {
		p.mu.Unlock()
		p.logger.Error(p.ctx, "Attempted to remove invalid worker", "worker_id", id)
		return ErrInvalidWorkerID
	}

	// Check if worker is available
	wasAvailable := !state.paused.Load() && !state.removed.Load()

	p.logger.Info(p.ctx, "Removing worker", "worker_id", id)

	// Stop the flow of tasks
	state.mu.Lock()
	if state.removed.Load() {
		state.mu.Unlock()
		p.mu.Unlock()
		p.logger.Warn(p.ctx, "Worker already removed", "worker_id", id)
		return nil
	}
	state.removed.Store(true)
	state.paused.Store(true)
	state.mu.Unlock()

	// Signal the worker's condition variable to wake it up if it's waiting
	state.cond.Broadcast()

	// Signal the pool's condition variable to wake up any workers waiting on tasks
	p.cond.Broadcast()

	// Remove the worker's queue and redistribute tasks
	q, ok := p.taskQueues.get(id)
	if !ok {
		p.mu.Unlock()
		p.logger.Error(p.ctx, "Task queue not found for worker", "worker_id", id)
		return errors.New("task queue not found for worker")
	}
	p.taskQueues.delete(id)
	delete(p.workerQueues, id)

	p.mu.Unlock()

	// Drain the task queue and redistribute tasks
	tasks := q.Drain()
	if len(tasks) > 0 {
		p.logger.Debug(p.ctx, "Redistributing tasks from removed worker", "worker_id", id, "tasks_count", len(tasks))
		p.redistributeTasks(tasks)
	}

	// Cancel the worker's context to signal task cancellation
	state.cancel()

	// Wait for the worker to finish, with timeout
	timeout := time.Second * 10 // Configurable timeout duration
	workerFinished := make(chan struct{})

	go func() {
		if state.async {
			// In async mode, wait for the worker goroutine to finish
			<-state.done
		} else {
			// TODO: i'm not sure sure about that but we will see
			// In synchronous mode, we need to wait for the worker to complete the current task
			state.mu.Lock()
			for state.currentTask != nil {
				state.logger.Debug(state.ctx, "Waiting for worker to finish current task", "worker_id", id)
				state.cond.Wait()
			}
			state.mu.Unlock()
		}
		close(workerFinished)
	}()

	select {
	case <-workerFinished:
		// Worker finished gracefully
		p.logger.Debug(p.ctx, "Worker exited gracefully", "worker_id", id)
	case <-time.After(timeout):
		// Worker didn't finish within timeout
		p.logger.Warn(p.ctx, "Worker did not finish within timeout", "worker_id", id)
		// Handle the current task
		state.mu.Lock()
		if state.currentTask != nil {
			task := state.currentTask
			task.mu.Lock()
			task.deadReason = "worker removed due to timeout"
			task.mu.Unlock()
			p.addDeadTask(task)
			state.currentTask = nil
			state.cond.Broadcast()
		}
		state.mu.Unlock()
	}

	// After removing, update availableWorkers
	if wasAvailable {
		p.availableWorkers.Add(-1)
	}

	p.logger.Debug(p.ctx, "Calling methods", "worker_id", id)
	// Trigger OnStop and OnRemove
	state.callMethod("OnStop", state.ctx)
	state.callMethod("OnRemove", state.ctx)

	p.logger.Debug(p.ctx, "Removing worker from pool", "worker_id", id)
	p.mu.Lock()
	delete(p.workers, id)
	p.updateRateLimiterBurst()
	p.mu.Unlock()

	p.logger.Info(p.ctx, "Worker removed successfully", "worker_id", id)

	return nil
}

// Pause pauses a worker
func (p *Pool[T]) Pause(id int) error {
	p.mu.Lock()
	state, exists := p.workers[id]
	if !exists {
		p.mu.Unlock()
		p.logger.Error(p.ctx, "Attempted to pause invalid worker", "worker_id", id)
		return ErrInvalidWorkerID
	}

	// Check if worker is available
	wasAvailable := !state.paused.Load() && !state.removed.Load()

	p.logger.Info(p.ctx, "Pausing worker", "worker_id", id)

	// Stop the flow of tasks
	state.mu.Lock()
	if state.paused.Load() {
		state.mu.Unlock()
		p.mu.Unlock()
		p.logger.Warn(p.ctx, "Worker already paused", "worker_id", id)
		return nil
	}
	state.paused.Store(true)
	state.mu.Unlock()

	// Remove the worker's queue and redistribute tasks
	q, _ := p.taskQueues.get(id)
	p.taskQueues.delete(id)
	delete(p.workerQueues, id)
	p.mu.Unlock()

	// Drain the task queue and redistribute tasks
	tasks := q.Drain()
	if len(tasks) > 0 {
		p.logger.Debug(p.ctx, "Redistributing tasks from paused worker", "worker_id", id, "tasks_count", len(tasks))
		p.redistributeTasks(tasks)
	}

	// Trigger OnPause
	state.callMethod("OnPause", state.ctx)

	// After pausing, update availableWorkers
	if wasAvailable {
		p.availableWorkers.Add(-1)
	}

	p.updateRateLimiterBurst()

	return nil
}

// Redistribute all tasks among workers
func (p *Pool[T]) redistributeAllTasks() {
	p.logger.Debug(p.ctx, "Redistributing all tasks among workers")

	// Collect all tasks from all queues
	allTasks := []*Task[T]{}
	p.taskQueues.range_(func(id int, q TaskQueue[T]) bool {
		tasks := q.Drain()
		allTasks = append(allTasks, tasks...)
		return true
	})

	// Get available (non-paused) workers
	availableWorkers := []int{}
	for id, state := range p.workers {
		if !state.paused.Load() && !state.removed.Load() {
			availableWorkers = append(availableWorkers, id)
		}
	}

	workerCount := len(availableWorkers)
	if workerCount == 0 {
		for _, task := range allTasks {
			task.deadReason = "no workers available"
			p.addDeadTask(task)
			p.UpdateTotalQueueSize(-1)
		}
		p.logger.Warn(p.ctx, "No workers available during redistribution, tasks added to dead tasks", "dead_tasks_count", len(allTasks))
		return
	}

	// Redistribute tasks among available workers
	for i, task := range allTasks {
		selectedWorkerID := availableWorkers[i%workerCount]
		q := p.taskQueues.getOrCreate(selectedWorkerID, func() TaskQueue[T] {
			return p.NewTaskQueue(p.taskQueueType)
		})
		q.Enqueue(task)
		p.UpdateWorkerQueueSize(selectedWorkerID, 1)
		p.logger.Debug(p.ctx, "Task redistributed", "worker_id", selectedWorkerID)
	}

	p.logger.Debug(p.ctx, "Tasks redistributed among workers", "tasks_count", len(allTasks))
}

// Resume resumes a worker
func (p *Pool[T]) Resume(id int) error {
	p.mu.Lock()
	state, exists := p.workers[id]
	if !exists {
		p.mu.Unlock()
		p.logger.Error(p.ctx, "Attempted to resume invalid worker", "worker_id", id)
		return ErrInvalidWorkerID
	}

	p.logger.Info(p.ctx, "Resuming worker", "worker_id", id)

	state.mu.Lock()
	if !state.paused.Load() {
		state.mu.Unlock()
		p.mu.Unlock()
		p.logger.Warn(p.ctx, "Worker not paused, cannot resume", "worker_id", id)
		return nil
	}

	// Create a new task queue for the worker if it doesn't exist
	if _, ok := p.taskQueues.get(id); !ok {
		p.taskQueues.set(id, p.NewTaskQueue(p.taskQueueType))
		p.workerQueues[id] = &atomic.Int64{}
	}

	// Mark the worker as not paused before redistribution
	state.paused.Store(false)

	// Redistribute tasks while holding both locks
	p.redistributeAllTasks()

	// Signal the worker to continue
	state.cond.Signal()
	p.cond.Broadcast()

	state.mu.Unlock()
	p.mu.Unlock()

	p.updateRateLimiterBurst()

	state.callMethod("OnResume", state.ctx)

	// After resuming, update availableWorkers
	p.availableWorkers.Add(1)

	p.logger.Info(p.ctx, "Worker resumed", "worker_id", id)
	return nil
}

// Workers returns the list of worker IDs
func (p *Pool[T]) Workers() ([]int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ids := make([]int, 0, len(p.workers))
	for id := range p.workers {
		ids = append(ids, id)
	}
	p.logger.Debug(p.ctx, "Fetched all worker IDs", "worker_ids", ids)
	return ids, nil
}

func (p *Pool[T]) getAvailableWorkerIDs() []int {
	ids := make([]int, 0)
	for id, state := range p.workers {
		if !state.paused.Load() && !state.removed.Load() {
			ids = append(ids, id)
		}
	}
	return ids
}

// redistributeTasks redistributes tasks to other workers
func (p *Pool[T]) redistributeTasks(tasks []*Task[T]) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Get available (non-paused, non-removed) workers
	availableWorkers := p.getAvailableWorkerIDs()
	workerCount := len(availableWorkers)
	if workerCount == 0 {
		for _, task := range tasks {
			task.deadReason = "no workers available"
			p.addDeadTask(task)
			p.UpdateTotalQueueSize(-1)
		}
		p.logger.Warn(p.ctx, "No workers available, tasks added to dead tasks", "dead_tasks_count", len(tasks))
		return
	}

	for i, task := range tasks {
		selectedWorkerID := availableWorkers[i%workerCount]
		q := p.taskQueues.getOrCreate(selectedWorkerID, func() TaskQueue[T] {
			return p.NewTaskQueue(p.taskQueueType)
		})
		q.Enqueue(task)
		p.UpdateWorkerQueueSize(selectedWorkerID, 1)
		p.logger.Debug(p.ctx, "Task redistributed", "worker_id", selectedWorkerID)
	}

	p.logger.Debug(p.ctx, "Tasks redistributed among workers", "tasks_count", len(tasks))

	// Notify all workers that tasks are available
	p.cond.Broadcast()
}

// runWorker runs the worker's loop
func (p *Pool[T]) runWorker(state *workerState[T]) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 1<<16)
			n := runtime.Stack(buf, false)
			stackTrace := string(buf[:n])
			if p.config.onWorkerPanic != nil {
				p.config.onWorkerPanic(state.id, r, stackTrace)
			}
			p.logger.Error(p.ctx, "Panic in worker", "worker_id", state.id, "error", r, "stack_trace", stackTrace)
		}
		close(state.done)
		p.logger.Debug(p.ctx, "Worker goroutine exited", "worker_id", state.id)
	}()

	for {
		// First check if we're removed without holding any locks
		if state.removed.Load() {
			p.logger.Debug(p.ctx, "Worker detected removal, exiting", "worker_id", state.id)
			break
		}

		// Then check if we're paused
		if state.paused.Load() {
			state.mu.Lock()
			for state.paused.Load() && !state.removed.Load() {
				p.logger.Debug(p.ctx, "Worker is paused, waiting", "worker_id", state.id)
				state.cond.Wait()
			}
			state.mu.Unlock()
			continue
		}

		// Try to get a task with proper lock ordering
		var task *Task[T]
		var ok bool

		// First get pool lock to check queue
		p.mu.Lock()
		q, hasQueue := p.taskQueues.get(state.id)
		if !hasQueue {
			p.mu.Unlock()
			p.logger.Error(p.ctx, "No task queue found for worker", "worker_id", state.id)
			continue
		}

		qLen := q.Length()
		p.logger.Debug(p.ctx, "Checking queue", "worker_id", state.id, "queue_length", qLen)

		if qLen > 0 {
			task, ok = q.Dequeue()
			if ok {
				p.UpdateWorkerQueueSize(state.id, -1)
				p.logger.Debug(p.ctx, "Dequeued task", "worker_id", state.id, "task_data", task.data, "remaining_queue", q.Length())
			}
		}

		if !ok {
			if state.removed.Load() {
				p.mu.Unlock()
				break
			}
			p.logger.Debug(p.ctx, "No task available, waiting", "worker_id", state.id, "queue_size", p.QueueSize(), "total_processing", p.totalProcessing.Load())
			p.cond.Wait()
			p.mu.Unlock()
			continue
		}
		p.mu.Unlock()

		// Now process the task
		if task != nil {
			state.mu.Lock()
			state.currentTask = task
			state.mu.Unlock()

			p.logger.Debug(p.ctx, "Processing task", "worker_id", state.id, "task_data", task.data)

			p.processTask(state, task)

			state.mu.Lock()
			state.currentTask = nil
			state.mu.Unlock()

			p.logger.Debug(p.ctx, "Completed task processing", "worker_id", state.id, "queue_size", p.QueueSize(), "total_processing", p.totalProcessing.Load())
		}
	}

	// Trigger OnStop and OnRemove if not already called
	state.callMethod("OnStop", state.ctx)
	state.callMethod("OnRemove", state.ctx)
}

// run processes tasks in synchronous mode
func (p *Pool[T]) run() {
	p.logger.Debug(p.ctx, "Running pool in synchronous mode")
	duration := time.Millisecond * 10
	if p.config.loopTicker > 0 {
		duration = p.config.loopTicker
	}
	ticker := time.NewTicker(duration) // Configurable interval
	defer ticker.Stop()

	iterations := 0
	for {
		iterations++

		select {
		case <-p.ctx.Done():
			p.logger.Info(p.ctx, "Pool context done, exiting run loop")
			return
		case <-ticker.C:
			p.mu.Lock()
			if p.stopped {
				p.mu.Unlock()
				p.logger.Info(p.ctx, "Pool stopped, exiting run loop")
				return
			}

			// Get available workers
			workerIDs := p.getWorkerIDs()

			if len(workerIDs) == 0 {
				p.mu.Unlock()
				p.logger.Warn(p.ctx, "No workers available")
				continue
			}

			hasWork := false
			totalTasks := int64(0)
			for _, workerID := range workerIDs {
				state := p.workers[workerID]
				if state == nil {
					continue
				}

				state.mu.Lock()
				if state.paused.Load() || state.removed.Load() {
					state.mu.Unlock()
					continue
				}
				state.mu.Unlock()

				// Check if there are tasks available
				if q, ok := p.taskQueues.get(workerID); ok {
					qLen := q.Length()
					totalTasks += int64(qLen)
					if qLen > 0 {
						hasWork = true
					}
				}
			}

			if !hasWork {
				p.logger.Debug(p.ctx, "No tasks available, waiting")
				p.cond.Wait()
				p.mu.Unlock()
				continue
			}

			p.mu.Unlock()

			if !hasWork {
				continue
			}

			// Process one task per worker
			for _, workerID := range workerIDs {
				state := p.workers[workerID]
				if state == nil {
					continue
				}

				state.mu.Lock()
				if state.paused.Load() || state.removed.Load() {
					state.mu.Unlock()
					continue
				}
				state.mu.Unlock()

				task, ok := p.getTaskForWorker(state.id)
				if !ok {
					continue
				}

				state.mu.Lock()
				state.currentTask = task
				state.mu.Unlock()

				p.processTask(state, task)

				state.mu.Lock()
				state.currentTask = nil
				state.mu.Unlock()
			}
		}
	}
}

// getTaskForWorker gets the next task for the worker
func (p *Pool[T]) getTaskForWorker(workerID int) (*Task[T], bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.logger.Debug(p.ctx, "Getting task for worker", "worker_id", workerID, "total_queue_size", p.QueueSize())

	q, ok := p.taskQueues.get(workerID)
	if !ok {
		return nil, false
	}

	qLen := q.Length()
	p.logger.Debug(p.ctx, "Found queue", "worker_id", workerID, "queue_length", qLen)

	if qLen == 0 {
		return nil, false
	}

	task, ok := q.Dequeue()
	if ok {
		p.UpdateWorkerQueueSize(workerID, -1)
		p.logger.Debug(p.ctx, "Got task for worker", "worker_id", workerID, "task_data", task.data, "remaining_queue", q.Length())
	}

	return task, ok
}

// processTask processes a task with proper state management
func (p *Pool[T]) processTask(state *workerState[T], task *Task[T]) {
	p.logger.Debug(p.ctx, "Starting task processing", "worker_id", state.id, "task_data", task.data, "task_retries", task.retries, "immediate_retry", task.immediateRetry)

	// Update task state with proper locking
	task.mu.Lock()
	task.lastWorkerID = state.id
	if task.bounceRetry {
		task.attemptedWorkers[state.id] = struct{}{}
	}
	task.attemptStartTime = time.Now()
	task.processedAt = append(task.processedAt, time.Now())
	task.mu.Unlock()

	// Update metrics atomically
	p.metrics.TasksProcessed.Add(1)
	p.totalProcessing.Add(1)
	defer p.totalProcessing.Add(-1)

	p.logger.Debug(p.ctx, "Processing task", "worker_id", state.id, "task_data", task.data)

	if p.config.onTaskAttempt != nil {
		p.config.onTaskAttempt(task, state.id)
	}

	// Create a context for this task attempt, considering both per-attempt and total task timeouts.
	// If the total task timeout has already been exceeded, createTaskContext returns nil,
	// and we handle the task failure without starting a new attempt.
	ctx, cancel := p.createTaskContext(state.ctx, task)
	if ctx == nil {
		// Total task timeout has been exceeded before starting this attempt.
		// Handle task failure without starting a new attempt.
		p.handleTaskFailure(task, context.DeadlineExceeded)
		return
	}
	defer cancel()

	// Before processing the task
	if err := p.TransitionTaskState(task, TaskStateRunning, "Task processing started"); err != nil {
		p.logger.Warn(p.ctx, "Failed to transition task state", "error", err, "worker_id", state.id, "task_data", task.data)
		p.handleTaskFailure(task, err)
		return
	}

	// Execute task with proper error handling
	err := p.executeTask(ctx, state, task)

	p.logger.Debug(p.ctx, "Task execution completed", "worker_id", state.id, "task_data", task.data, "error", err, "task_retries", task.retries)

	// Handle task completion
	p.handleTaskCompletion(state, task, err)
}

// createTaskContext creates a new context for a task attempt, ensuring that both per-attempt and total task timeouts are enforced.
// It calculates the appropriate timeout for the current attempt based on the per-attempt timeout and the remaining total task timeout.
// - If the total task timeout has been exceeded, it returns nil, indicating that no further attempts should be made.
// - If only the per-attempt timeout is set, it uses that for the context.
// - If both timeouts are set, it uses the smaller of the per-attempt timeout and the remaining total task timeout.
func (p *Pool[T]) createTaskContext(parentCtx context.Context, task *Task[T]) (context.Context, context.CancelFunc) {
	task.mu.Lock()
	totalTimeout := task.totalTimeout
	attemptTimeout := task.attemptTimeout
	totalDuration := task.totalDuration
	task.mu.Unlock()

	if totalTimeout > 0 {
		// Calculate the remaining total task timeout.
		remainingTotalTimeout := totalTimeout - totalDuration
		if remainingTotalTimeout <= 0 {
			// Total task timeout has been exceeded; no further attempts should be made.
			return nil, nil
		}

		// Determine the timeout for this attempt.
		// Use the smaller of the per-attempt timeout and the remaining total task timeout.
		var timeout time.Duration
		if attemptTimeout > 0 && attemptTimeout < remainingTotalTimeout {
			timeout = attemptTimeout
		} else {
			timeout = remainingTotalTimeout
		}

		// Create a context with the calculated timeout.
		ctx, cancel := context.WithTimeout(parentCtx, timeout)
		return ctx, cancel
	} else if attemptTimeout > 0 {
		// Only per-attempt timeout is set; use it for the context.
		ctx, cancel := context.WithTimeout(parentCtx, attemptTimeout)
		return ctx, cancel
	}

	// No timeouts are set; create a cancellable context without a deadline.
	return context.WithCancel(parentCtx)
}

// executeTask executes the task with proper panic recovery
func (p *Pool[T]) executeTask(ctx context.Context, state *workerState[T], task *Task[T]) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = p.handlePanic(state, r)
			p.logger.Error(p.ctx, "Recovered from panic in worker", "worker_id", state.id, "error", err)
		}
	}()

	p.logger.Debug(p.ctx, "Executing task", "worker_id", state.id, "task_data", task.data)

	if task.runningCb != nil {
		task.runningCb()
	}

	return state.worker.Run(ctx, task.data)
}

// handlePanic processes panic recovery
func (p *Pool[T]) handlePanic(state *workerState[T], r interface{}) error {
	const stackSize = 1 << 16
	buf := make([]byte, stackSize)
	n := runtime.Stack(buf, false)
	stackTrace := string(buf[:n])

	err := fmt.Errorf("panic in worker %d: %v\n%s", state.id, r, stackTrace)

	if p.config.onWorkerPanic != nil {
		p.config.onWorkerPanic(state.id, r, stackTrace)
	}

	p.logger.Error(p.ctx, "Panic in worker during task execution", "worker_id", state.id, "error", r, "stack_trace", stackTrace)

	return err
}

// handleTaskCompletion processes task completion or failure
func (p *Pool[T]) handleTaskCompletion(state *workerState[T], task *Task[T], err error) {
	taskDuration := time.Since(task.attemptStartTime)

	task.mu.Lock()
	task.totalDuration += taskDuration
	task.durations = append(task.durations, taskDuration)

	if err != nil {
		task.errors = append(task.errors, err)
		task.retries++
		task.mu.Unlock()

		p.logger.Warn(p.ctx, "Task failed", "worker_id", state.id, "task_data", task.data, "error", err)

		if err := p.TransitionTaskState(task, TaskStateFailed, "Task failed"); err != nil {
			p.logger.Error(p.ctx, "Failed to transition task state", "error", err)
		}

		p.handleTaskFailure(task, err)
		return
	}

	task.mu.Unlock()

	p.metrics.TasksSucceeded.Add(1)
	p.UpdateTotalQueueSize(-1)

	p.logger.Debug(p.ctx, "Task succeeded", "worker_id", state.id, "task_data", task.data)

	if err := p.TransitionTaskState(task, TaskStateCompleted, "Task succeeded"); err != nil {
		p.logger.Error(p.ctx, "Failed to transition task state", "error", err)
	}

	state.mu.Lock()
	state.currentTask = nil
	state.mu.Unlock()

	p.logger.Debug(p.ctx, "Checking task success callback", "has_callback", p.config.onTaskSuccess != nil)

	if p.config.onTaskSuccess != nil {
		p.config.onTaskSuccess(task.data)
	}

	if task.notifiedProcessed != nil {
		task.notifiedProcessed.Notify()
	}
	if task.processedCb != nil {
		task.processedCb()
	}
	p.taskPool.Put(task)
}

// handleTaskFailure handles a task failure
func (p *Pool[T]) handleTaskFailure(task *Task[T], err error) {
	// p.metrics.TasksFailed.Add(1)
	p.metrics.TasksFailed.Add(1)

	task.mu.Lock()
	totalTimeout := task.totalTimeout
	totalDuration := task.totalDuration
	task.mu.Unlock()

	// Check if total task timeout has been exceeded
	if totalTimeout > 0 && totalDuration >= totalTimeout {
		p.UpdateTotalQueueSize(-1)
		task.deadReason = "failed; total task time limit exceeded"
		p.addDeadTask(task)
		return
	}

	action := TaskActionRetry
	if p.config.onTaskFailure != nil {
		action = p.config.onTaskFailure(task.data, err)
	}

	p.logger.Debug(p.ctx, "Handling task failure", "task_data", task.data, "action", action, "bounce_retry", task.bounceRetry)

	switch action {
	case TaskActionRetry:
		if p.config.attempts > 0 && task.retries >= p.config.attempts {
			p.UpdateTotalQueueSize(-1)
			task.deadReason = "failed; max retries exceeded"
			p.addDeadTask(task)
			return
		}
		if p.config.retryIf != nil && !p.config.retryIf(err) {
			p.UpdateTotalQueueSize(-1)
			task.deadReason = "failed; retryIf condition failed"
			p.addDeadTask(task)
			return
		}

		// Handle bounce retry worker tracking
		if task.bounceRetry {
			if len(task.attemptedWorkers) >= len(p.workers) {
				// Reset if we've tried all workers
				task.attemptedWorkers = make(map[int]struct{})
			}
		}

		// Handle immediate retry
		if task.immediateRetry {
			p.submitTask(task)
			return
		}

		// Calculate retry delay
		delay := p.config.delay
		if p.config.delayFunc != nil {
			delay = p.config.delayFunc(task.retries, err, &p.config)
		} else if p.config.retryPolicy != nil {
			delay = p.config.retryPolicy.ComputeDelay(task.retries, err, &p.config)
		}

		// Simple blocking delay - this will still respect context cancellation
		select {
		case <-p.ctx.Done():
			p.UpdateTotalQueueSize(-1)
			task.deadReason = "failed; context cancelled during retry delay"
			p.addDeadTask(task)
			return
		case <-time.After(delay):
			if err := p.submitTask(task); err != nil {
				p.UpdateTotalQueueSize(-1)
				task.deadReason = "failed; retry submission failed"
				p.addDeadTask(task)
			}
		}

	case TaskActionForceRetry:
		if task.bounceRetry {
			if len(task.attemptedWorkers) >= len(p.workers) {
				task.attemptedWorkers = make(map[int]struct{})
			}
		}
		task.deadReason = "failed; force retry"
		p.submitTask(task)

	case TaskActionAddToDeadTasks:
		p.UpdateTotalQueueSize(-1)
		task.deadReason = "failed; add to dead tasks"
		p.addDeadTask(task)

	case TaskActionRemove:
		p.UpdateTotalQueueSize(-1)
		p.taskPool.Put(task)
	}
}

// addDeadTask adds a task to dead tasks
func (p *Pool[T]) addDeadTask(task *Task[T]) {

	// First transition to dead state
	if err := p.TransitionTaskState(task, TaskStateDead, "Task added to dead tasks"); err != nil {
		p.logger.Error(p.ctx, "Failed to transition task state", "error", err)
	}

	// THEN copy it
	p.deadMu.Lock()

	deadTaskIndex := len(p.deadTasks)
	if p.config.deadTasksLimit > 0 && deadTaskIndex >= p.config.deadTasksLimit {
		p.deadTasks = p.deadTasks[1:]
		deadTaskIndex--
	}

	deadTask := &DeadTask[T]{
		Data:          task.data,
		Retries:       task.retries,
		TotalDuration: task.totalDuration,
		Errors:        task.errors,
		Reason:        task.deadReason,
		StateHistory:  task.stateHistory,
	}

	p.deadTasks = append(p.deadTasks, deadTask)
	p.metrics.DeadTasks.Add(1)

	p.deadMu.Unlock()

	// Send notification after releasing the lock
	if p.config.onDeadTask != nil {
		select {
		case p.deadTaskNotifications <- deadTaskIndex:
		default:
			p.logger.Warn(p.ctx, "Dead task notification channel full, notification dropped", "dead_task_index", deadTaskIndex)
		}
	}

	p.taskPool.Put(task)
}

// WaitWithCallback waits for the pool to complete while calling a callback function
func (p *Pool[T]) WaitWithCallback(ctx context.Context, callback func(queueSize, processingCount, deadTaskCount int) bool, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			p.logger.Warn(p.ctx, "WaitWithCallback context done", "error", ctx.Err())
			return ctx.Err()
		case <-ticker.C:
			queueSize := int(p.QueueSize())
			processingCount := int(p.totalProcessing.Load())
			deadTaskCount := int(p.metrics.DeadTasks.Load())
			p.logger.Debug(p.ctx, "WaitWithCallback tick", "queue_size", queueSize, "processing_count", processingCount, "dead_task_count", deadTaskCount)
			if !callback(queueSize, processingCount, deadTaskCount) {
				p.logger.Info(p.ctx, "WaitWithCallback stopping as callback returned false")
				return nil
			}
		}
	}
}

// Close gracefully shuts down the pool
func (p *Pool[T]) Close() error {
	p.mu.Lock()
	p.stopped = true
	p.mu.Unlock()

	p.logger.Info(p.ctx, "Closing pool")

	p.cond.Broadcast()

	p.mu.Lock()
	for _, state := range p.workers {
		state.mu.Lock()
		if !state.removed.Load() {
			state.removed.Store(true)
			state.paused.Store(true)
			state.cancel()
			state.mu.Unlock()
			state.callMethod("OnStop", state.ctx)
			state.callMethod("OnRemove", state.ctx)
			p.logger.Info(p.ctx, "Worker closed", "worker_id", state.id)
		} else {
			state.mu.Unlock()
		}
	}
	p.mu.Unlock()

	p.cancel()
	return nil
}

// QueueSize returns the total number of tasks in the queue
func (p *Pool[T]) QueueSize() int64 {
	total := p.totalQueueSize.Load()
	p.logger.Debug(p.ctx, "Fetching total queue size", "queue_size", total)
	return total
}

// ProcessingCount returns the number of tasks currently being processed
func (p *Pool[T]) ProcessingCount() int64 {
	processingCount := p.totalProcessing.Load()
	p.logger.Debug(p.ctx, "Fetching processing count", "processing_count", processingCount)
	return processingCount
}

// DeadTaskCount returns the number of dead tasks
func (p *Pool[T]) DeadTaskCount() int64 {
	deadTaskCount := p.metrics.DeadTasks.Load()
	p.logger.Debug(p.ctx, "Fetching dead task count", "dead_task_count", deadTaskCount)
	return deadTaskCount
}

// RangeDeadTasks iterates over all dead tasks
func (p *Pool[T]) RangeDeadTasks(fn func(*DeadTask[T]) bool) {
	p.deadMu.Lock()
	defer p.deadMu.Unlock()
	for _, dt := range p.deadTasks {
		p.logger.Debug(p.ctx, "Iterating dead tasks", "dead_task_data", dt.Data)
		if !fn(dt) {
			break
		}
	}
}

// PullDeadTask removes and returns a dead task from the pool
func (p *Pool[T]) PullDeadTask(idx int) (*DeadTask[T], error) {
	p.deadMu.Lock()
	defer p.deadMu.Unlock()
	if idx < 0 || idx >= len(p.deadTasks) {
		p.logger.Error(p.ctx, "Invalid index for PullDeadTask", "index", idx)
		return nil, errors.New("invalid index")
	}
	dt := p.deadTasks[idx]
	p.deadTasks = append(p.deadTasks[:idx], p.deadTasks[idx+1:]...)
	p.metrics.DeadTasks.Add(-1)
	p.logger.Debug(p.ctx, "Pulled dead task", "dead_task_data", dt.Data)
	return dt, nil
}

// PullRangeDeadTasks removes and returns a range of dead tasks from the pool
func (p *Pool[T]) PullRangeDeadTasks(from int, to int) ([]*DeadTask[T], error) {
	p.deadMu.Lock()
	defer p.deadMu.Unlock()
	if from < 0 || to > len(p.deadTasks) || from > to {
		p.logger.Error(p.ctx, "Invalid range for PullRangeDeadTasks", "from", from, "to", to)
		return nil, errors.New("invalid range")
	}
	dts := make([]*DeadTask[T], to-from)
	copy(dts, p.deadTasks[from:to])
	p.deadTasks = append(p.deadTasks[:from], p.deadTasks[to:]...)
	p.metrics.DeadTasks.Add(int64(from - to))
	p.logger.Debug(p.ctx, "Pulled range of dead tasks", "count", len(dts))
	return dts, nil
}

// ProcessedNotification is used to notify when a task is processed
type ProcessedNotification struct {
	ch     chan struct{}
	once   sync.Once
	closed bool
}

// NewProcessedNotification creates a new ProcessedNotification
func NewProcessedNotification() *ProcessedNotification {
	return &ProcessedNotification{ch: make(chan struct{})}
}

// Notify notifies that the task has been processed
func (n *ProcessedNotification) Notify() {
	n.once.Do(func() {
		close(n.ch)
		n.closed = true
	})
}

// Wait waits for the notification
func (n *ProcessedNotification) Wait() {
	<-n.ch
}

// Done returns the channel that's closed when the task is processed
func (n *ProcessedNotification) Done() <-chan struct{} {
	return n.ch
}

// QueuedNotification is used to notify when a task is queued
type QueuedNotification struct {
	ch     chan struct{}
	once   sync.Once
	closed bool
}

// NewQueuedNotification creates a new QueuedNotification
func NewQueuedNotification() *QueuedNotification {
	return &QueuedNotification{ch: make(chan struct{})}
}

// Notify notifies that the task has been queued
func (n *QueuedNotification) Notify() {
	n.once.Do(func() {
		close(n.ch)
		n.closed = true
	})
}

// Wait waits for the notification
func (n *QueuedNotification) Wait() {
	<-n.ch
}

// Done returns the channel that's closed when the task is queued
func (n *QueuedNotification) Done() <-chan struct{} {
	return n.ch
}

// RequestResponse manages the lifecycle of a task request and its response
type RequestResponse[T any, R any] struct {
	request     T                // The request data
	done        chan struct{}    // Channel to signal completion
	response    R                // Stores the successful response
	err         error            // Stores any error that occurred
	mu          deadlock.RWMutex // Protects response and err
	isCompleted bool             // Indicates if request is completed
}

// NewRequestResponse creates a new RequestResponse instance
func NewRequestResponse[T any, R any](request T) *RequestResponse[T, R] {
	return &RequestResponse[T, R]{
		request: request,
		done:    make(chan struct{}),
	}
}

// Safely consults the request data
func (rr *RequestResponse[T, R]) ConsultRequest(fn func(T)) {
	rr.mu.Lock()
	fn(rr.request)
	rr.mu.Unlock()
}

// Complete safely marks the request as complete with a response
func (rr *RequestResponse[T, R]) Complete(response R) {
	var completed bool
	rr.mu.RLock()
	completed = rr.isCompleted
	rr.mu.RUnlock()

	if !completed {
		rr.mu.Lock()
		rr.response = response
		rr.isCompleted = true
		close(rr.done)
		rr.mu.Unlock()
	}
}

// CompleteWithError safely marks the request as complete with an error
func (rr *RequestResponse[T, R]) CompleteWithError(err error) {
	var completed bool
	rr.mu.RLock()
	completed = rr.isCompleted
	rr.mu.RUnlock()

	if !completed {
		rr.mu.Lock()
		rr.err = err
		rr.isCompleted = true
		close(rr.done)
		rr.mu.Unlock()
	}
}

// Done returns a channel that's closed when the request is complete
func (rr *RequestResponse[T, R]) Done() <-chan struct{} {
	var done chan struct{}
	rr.mu.RLock()
	done = rr.done
	rr.mu.RUnlock()
	return done
}

// Err returns any error that occurred during the request
func (rr *RequestResponse[T, R]) Err() error {
	var err error
	rr.mu.RLock()
	err = rr.err
	rr.mu.RUnlock()
	return err
}

// Wait waits for the request to complete and returns the response and any error
func (rr *RequestResponse[T, R]) Wait(ctx context.Context) (R, error) {
	var done chan struct{}
	rr.mu.RLock()
	done = rr.done
	rr.mu.RUnlock()

	select {
	case <-done:
		var err error
		var response R
		rr.mu.RLock()
		response = rr.response
		err = rr.err
		rr.mu.RUnlock()
		return response, err
	case <-ctx.Done():
		var completed bool
		rr.mu.RLock()
		completed = rr.isCompleted
		rr.mu.RUnlock()
		if !completed {
			var zero R
			rr.mu.Lock()
			rr.err = ctx.Err()
			rr.isCompleted = true
			close(rr.done)
			rr.mu.Unlock()
			return zero, ctx.Err()
		} else {
			var err error
			var response R
			rr.mu.RLock()
			response = rr.response
			err = rr.err
			rr.mu.RUnlock()
			return response, err
		}
	}
}

// handleDeadTaskNotifications handles dead task notifications
func (p *Pool[T]) handleDeadTaskNotifications(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case idx := <-p.deadTaskNotifications:
			if p.config.onDeadTask != nil {
				p.config.onDeadTask(idx)
			}
		}
	}
}

func (ws *workerState[T]) callMethod(methodName string, ctx context.Context) {
	// ws.logger.Debug(ctx, "Attempting to call method", "method_name", methodName)

	method, exists := ws.methodsCache[methodName]
	if !exists {
		// ws.logger.Debug(ctx, "Method not found in cache", "method_name", methodName)
		return
	}

	methodType := method.Type()
	if methodType.NumIn() == 0 {
		// ws.logger.Debug(ctx, "Calling method without context", "method_name", methodName)
		method.Call(nil)
	} else if methodType.NumIn() == 1 && methodType.In(0) == reflect.TypeOf((*context.Context)(nil)).Elem() {
		// ws.logger.Debug(ctx, "Calling method with context", "method_name", methodName)
		method.Call([]reflect.Value{reflect.ValueOf(ctx)})
	}
}
