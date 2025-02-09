package retrypool

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/k0kubun/pp/v3"
	"github.com/sasha-s/go-deadlock"
)

/// BlockingPool Design and Mechanism
///
/// Core Concepts:
/// - The BlockingPool manages multiple Pool[T], with a maximum number of active pools
/// - Each active group gets assigned its own dedicated pool
/// - Groups without pools wait in a pending queue until a pool becomes available
/// - When a group completes, its pool is cleaned up and assigned to the next pending group
///
/// Pool Management:
/// - We maintain a fixed number of worker pools (maxActivePools)
/// - Each pool runs in asynchronous mode to handle blocking workers
/// - When a group needs a pool:
///   1. If we're under maxActivePools limit, create a new pool
///   2. If at limit, group goes into pending queue
///   3. When a group finishes, its pool is reassigned to next pending group
///
/// Task Execution Order (within each group's pool):
/// 1. Parent tasks start first (e.g., groupA-1 start)
/// 2. Child tasks start immediately after parent (groupA-2 start)
/// 3. Each task blocks until its child completes
/// 4. Tasks complete in reverse order (child first, then parent)
///
/// Example flow:
/// - Group A starts, gets pool1:
///   1. A1 starts -> creates A2 -> blocks
///   2. A2 starts -> creates A3 -> blocks
///   3. A3 starts -> completes
///   4. A2 unblocks -> completes
///   5. A1 unblocks -> completes
///   Pool1 is now free for next group
///
/// Worker Scaling:
/// - Each pool starts with one worker
/// - Pool scales up when:
///   1. Queue has tasks waiting
///   2. Tasks are processing (need +1 worker for child tasks)
///   3. Parent task is blocked waiting on child
/// - Workers in a pool scale down when group completes
///
/// Error Handling:
/// - If a group fails, its pool is cleaned up and reassigned
/// - Tasks within a group can retry within their group's pool
/// - Group completion requires all tasks to succeed
///
/// Group States:
/// - Active: Has an assigned pool, can process tasks
/// - Pending: Waiting for pool assignment
/// - Completed: All tasks done, pool released
///
/// Task States:
/// - Created: Task is initialized
/// - Queued: Waiting in pool's queue
/// - Running: Being processed by worker
/// - Blocked: Waiting for child task
/// - Completed: Task finished successfully

// BlockingConfig holds basic config - each active group gets its own pool of workers
type BlockingConfig[T any] struct {
	logger        Logger
	workerFactory WorkerFactory[T]
	// You always starts with 1 pool but you can define the maximum amount the BP can have at all time
	maxConcurrentPools          int // Maximum number of active pools (one per group)
	maxConcurrentWorkersPerPool int // Maximum number of workers per pool
	// Task callbacks
	OnTaskSubmitted func(task T)
	OnTaskStarted   func(task T)
	OnTaskCompleted func(task T)
	OnTaskFailed    func(task T, err error)
	// Group/Pool callbacks
	OnGroupCreated   func(groupID any)
	OnGroupCompleted func(groupID any, tasks []T)
	OnGroupFailed    func(groupID any, tasks []T)
	OnGroupRemoved   func(groupID any, tasks []T)
	OnPoolCreated    func(groupID any)
	OnPoolDestroyed  func(groupID any)
	OnWorkerAdded    func(groupID any, workerID int)
	OnWorkerRemoved  func(groupID any, workerID int)
	OnPoolClosed     func()

	getData func(T) interface{}

	onSnapshot func()
}

// BlockingPoolOption is a functional option for configuring the blocking pool
type BlockingPoolOption[T any] func(*BlockingConfig[T])

// blockingTaskState holds internal metadata about one task including its completion signal
type blockingTaskState[T any, GID comparable, TID comparable] struct {
	mu           deadlock.RWMutex
	task         T
	taskID       TID
	groupID      GID
	completed    bool
	completionCh chan struct{}
	options      []TaskOption[T]
}

// blockingTaskGroup manages tasks that share the same GroupID
type blockingTaskGroup[T any, GID comparable, TID comparable] struct {
	mu        deadlock.RWMutex
	id        GID
	tasks     map[TID]*blockingTaskState[T, GID, TID]
	completed map[TID]bool
	hasPool   bool // Whether this group currently has an assigned pool
}

// BlockingPool manages multiple pools, assigning one to each active group
type BlockingPool[T any, GID comparable, TID comparable] struct {
	mu            deadlock.RWMutex
	pools         map[GID]Pooler[T] // Active pools, one per active group
	workerFactory WorkerFactory[T]
	groups        map[GID]*blockingTaskGroup[T, GID, TID]
	activeGroups  map[GID]struct{} // Groups that have an assigned pool
	pendingGroups []GID            // Groups waiting for pool assignment
	ctx           context.Context
	cancel        context.CancelFunc
	config        BlockingConfig[T]
	cachedTasks   map[GID][]*blockingTaskState[T, GID, TID] // Store tasks that can't be immediately submitted
	cacheMu       deadlock.RWMutex

	snapshotMu     deadlock.RWMutex // Protects access to the snapshot
	snapshotGroups map[GID]MetricsSnapshot[T]
	snapshot       BlockingMetricsSnapshot[T, GID]
}

// Configuration options
func WithBlockingWorkerFactory[T any](factory WorkerFactory[T]) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.workerFactory = factory
	}
}

func WithBlockingMaxActivePools[T any](max int) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		if max < 1 {
			max = 1
		}
		c.maxConcurrentPools = max
	}
}

// All the callback setters

func WithBlockingSnapshotHandler[T any](cb func()) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.onSnapshot = cb
	}
}

func WithBlockingLogger[T any](logger Logger) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.logger = logger
	}
}

func WithBlockingOnGroupRemoved[T any](cb func(groupID any, tasks []T)) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.OnGroupRemoved = cb
	}
}

func WithBlockingOnGroupFailed[T any](cb func(groupID any, tasks []T)) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.OnGroupFailed = cb
	}
}

func WithBlockingOnTaskSubmitted[T any](cb func(task T)) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.OnTaskSubmitted = cb
	}
}

func WithBlockingOnTaskStarted[T any](cb func(task T)) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.OnTaskStarted = cb
	}
}

func WithBlockingOnTaskCompleted[T any](cb func(task T)) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.OnTaskCompleted = cb
	}
}

func WithBlockingOnTaskFailed[T any](cb func(task T, err error)) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.OnTaskFailed = cb
	}
}

func WithBlockingOnGroupCreated[T any](cb func(groupID any)) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.OnGroupCreated = cb
	}
}

func WithBlockingOnGroupCompleted[T any](cb func(groupID any, tasks []T)) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.OnGroupCompleted = cb
	}
}

func WithBlockingOnPoolCreated[T any](cb func(groupID any)) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.OnPoolCreated = cb
	}
}

func WithBlockingOnPoolDestroyed[T any](cb func(groupID any)) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.OnPoolDestroyed = cb
	}
}

func WithBlockingOnWorkerAdded[T any](cb func(groupID any, workerID int)) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.OnWorkerAdded = cb
	}
}

func WithBlockingOnWorkerRemoved[T any](cb func(groupID any, workerID int)) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.OnWorkerRemoved = cb
	}
}

func WithBlockingOnPoolClosed[T any](cb func()) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.OnPoolClosed = cb
	}
}

func WithBlockingMaxWorkersPerPool[T any](max int) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.maxConcurrentWorkersPerPool = max
	}
}

func WithBlockingGetData[T any](cb func(T) interface{}) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.getData = cb
	}
}

// NewBlockingPool constructs a BlockingPool with the given options.
func NewBlockingPool[T any, GID comparable, TID comparable](
	ctx context.Context,
	opt ...BlockingPoolOption[T],
) (*BlockingPool[T, GID, TID], error) {
	cfg := BlockingConfig[T]{
		logger:                      NewLogger(slog.LevelDebug),
		maxConcurrentPools:          1,  // Default to one active pool
		maxConcurrentWorkersPerPool: -1, // unlimited workers per pool
	}

	// cfg.logger.Disable()

	for _, o := range opt {
		o(&cfg)
	}

	cfg.logger.Info(ctx, "Creating new BlockingPool",
		"max_active_pools", cfg.maxConcurrentPools,
		"has_worker_factory", cfg.workerFactory != nil)

	if cfg.workerFactory == nil {
		cfg.logger.Error(ctx, "Worker factory not provided")
		return nil, fmt.Errorf("worker factory must be provided")
	}

	ctx, cancel := context.WithCancel(ctx)
	pool := &BlockingPool[T, GID, TID]{
		pools:          make(map[GID]Pooler[T]),
		workerFactory:  cfg.workerFactory,
		groups:         make(map[GID]*blockingTaskGroup[T, GID, TID]),
		activeGroups:   make(map[GID]struct{}),
		pendingGroups:  make([]GID, 0),
		config:         cfg,
		ctx:            ctx,
		cancel:         cancel,
		cachedTasks:    make(map[GID][]*blockingTaskState[T, GID, TID]),
		snapshotGroups: make(map[GID]MetricsSnapshot[T]),
	}

	cfg.logger.Info(ctx, "BlockingPool created successfully")
	return pool, nil
}

func (p *BlockingPool[T, GID, TID]) cacheTaskState(groupID GID, state *blockingTaskState[T, GID, TID]) {
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()

	p.config.logger.Debug(p.ctx, "Caching task state for later submission",
		"group_id", groupID,
		"task_id", state.taskID)

	if p.cachedTasks == nil {
		p.cachedTasks = make(map[GID][]*blockingTaskState[T, GID, TID])
	}
	p.cachedTasks[groupID] = append(p.cachedTasks[groupID], state)
}

// Try to submit cached task states
func (p *BlockingPool[T, GID, TID]) trySubmitCachedStates(groupID GID) {
	p.cacheMu.Lock()
	cached, exists := p.cachedTasks[groupID]
	if !exists || len(cached) == 0 {
		p.cacheMu.Unlock()
		return
	}

	p.config.logger.Debug(p.ctx, "Attempting to submit cached tasks",
		"group_id", groupID,
		"cached_count", len(cached))

	pool := p.pools[groupID]
	if pool == nil {
		p.cacheMu.Unlock()
		return
	}

	var remainingStates []*blockingTaskState[T, GID, TID]
	currentStates := cached
	p.cachedTasks[groupID] = nil // Clear before unlocking
	p.cacheMu.Unlock()

	// Try to submit cached states
	for _, state := range currentStates {
		// completion check
		state.mu.Lock()
		if state.completed {
			state.mu.Unlock()
			continue // Skip already completed tasks
		}
		state.mu.Unlock()

		pp.Println("Submitting cached task", "groupID", state.groupID, "taskID", state.taskID)

		err := pool.SubmitToFreeWorker(state.task, state.options...)
		if err != nil {
			remainingStates = append(remainingStates, state)
		}
	}

	// Cache any states that couldn't be submitted
	if len(remainingStates) > 0 {
		p.cacheMu.Lock()
		p.cachedTasks[groupID] = append(p.cachedTasks[groupID], remainingStates...)
		p.cacheMu.Unlock()
	}
}

// Scale the amount of parallel pools that can be active at the same time
func (p *BlockingPool[T, GID, TID]) SetConcurrentPools(max int) {
	if max < 1 {
		max = 1
	}
	p.mu.Lock()
	p.config.maxConcurrentPools = max
	p.mu.Unlock()
}

// Scale the amount of workers that can be active at the same time on each pool
func (p *BlockingPool[T, GID, TID]) SetConcurrentWorkers(max int) {
	if max == 0 {
		max = -1
	} else if max < 0 {
		max = -1
	}
	p.mu.Lock()
	p.config.maxConcurrentWorkersPerPool = max
	p.mu.Unlock()
}

type GroupBlockingMetricSnapshot[T any, GID comparable] struct {
	GroupID GID
	MetricsSnapshot[T]
}

type BlockingMetricsSnapshot[T any, GID comparable] struct {
	TotalTasksSubmitted int64
	TotalTasksProcessed int64
	TotalTasksSucceeded int64
	TotalTasksFailed    int64
	TotalDeadTasks      int64
	Metrics             []GroupBlockingMetricSnapshot[T, GID]
}

func (p *BlockingPool[T, GID, TID]) GetSnapshot() BlockingMetricsSnapshot[T, GID] {
	p.snapshotMu.RLock()
	defer p.snapshotMu.RUnlock()
	return p.snapshot
}

func (p *BlockingPool[T, GID, TID]) RangeWorkerQueues(f func(workerID int, queueSize int64) bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, pool := range p.pools {
		pool.RangeWorkerQueues(f)
	}
}

func (p *BlockingPool[T, GID, TID]) RangeWorkers(f func(workerID int, state WorkerSnapshot[T]) bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, pool := range p.pools {
		pool.RangeWorkers(f)
	}
}

func (p *BlockingPool[T, GID, TID]) RangeTaskQueues(f func(workerID int, queue TaskQueue[T]) bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, pool := range p.pools {
		pool.RangeTaskQueues(f)
	}
}

func (p *BlockingPool[T, GID, TID]) calculateMetricsSnapshot() {
	snapshot := BlockingMetricsSnapshot[T, GID]{}
	for id, metric := range p.snapshotGroups {
		snapshot.TotalTasksSubmitted += metric.TasksSubmitted
		snapshot.TotalTasksProcessed += metric.TasksProcessed
		snapshot.TotalTasksSucceeded += metric.TasksSucceeded
		snapshot.TotalTasksFailed += metric.TasksFailed
		snapshot.TotalDeadTasks += metric.DeadTasks
		snapshot.Metrics = append(snapshot.Metrics, GroupBlockingMetricSnapshot[T, GID]{
			GroupID:         id,
			MetricsSnapshot: metric,
		})
	}
	p.snapshot = snapshot
}

// createPoolForGroup creates a new worker pool for a group
func (p *BlockingPool[T, GID, TID]) createPoolForGroup(groupID GID) (Pooler[T], error) {
	p.config.logger.Debug(p.ctx, "Creating new pool for group", "group_id", groupID)

	worker := p.workerFactory()
	p.config.logger.Debug(p.ctx, "Created initial worker for group", "group_id", groupID)

	options := []Option[T]{
		WithAttempts[T](1),
		WithLogger[T](p.config.logger),
	}

	// if p.config.getData != nil {
	// 	options = append(options, WithGetData[T](p.config.getData))
	// }

	options = append(
		options,
		WithSnapshotInterval[T](time.Second),
		WithSnapshots[T](),
		WithSnapshotCallback[T](func(ms MetricsSnapshot[T]) {
			p.snapshotMu.Lock()
			p.snapshotGroups[groupID] = ms
			p.calculateMetricsSnapshot()
			p.snapshotMu.Unlock()
			if p.config.onSnapshot != nil {
				p.config.onSnapshot()
			}
		}))

	// Create pool with synchronous mode and single retry
	pool := New[T](
		p.ctx,
		[]Worker[T]{worker},
		options...,
	)

	// Set completion handler
	pool.SetOnTaskSuccess(func(data T, metadata map[string]any) {
		p.config.logger.Debug(p.ctx, "Task completed successfully in group",
			"group_id", groupID)
		p.handleTaskCompletion(groupID, data)
	})

	// Add failure handler to trigger group cleanup
	pool.SetOnTaskFailure(func(data T, metadata map[string]any, err error) TaskAction {
		p.config.logger.Error(p.ctx, "Task failed in group",
			"group_id", groupID,
			"error", err)

		if p.config.OnTaskFailed != nil {
			p.config.OnTaskFailed(data, err)
		}

		// Get group's tasks without holding the pool lock
		var tasks []T

		group := p.groups[groupID]
		if group != nil {
			for _, taskState := range group.tasks {
				tasks = append(tasks, taskState.task)
			}
			p.config.logger.Debug(p.ctx, "Collected tasks for failed group",
				"group_id", groupID,
				"task_count", len(tasks))
		}

		// Call callbacks before acquiring any locks
		if p.config.OnGroupFailed != nil {
			p.config.OnGroupFailed(groupID, tasks)
		}

		// Now handle the cleanup
		p.cleanupGroup(groupID, tasks)

		return TaskActionRemove
	})

	if p.config.OnPoolCreated != nil {
		p.config.OnPoolCreated(groupID)
	}

	if p.config.OnWorkerAdded != nil {
		p.config.OnWorkerAdded(groupID, 0)
	}

	p.config.logger.Info(p.ctx, "Pool created successfully for group",
		"group_id", groupID)
	return pool, nil
}

// cleanupGroup handles group cleanup with proper lock ordering
func (p *BlockingPool[T, GID, TID]) cleanupGroup(groupID GID, tasks []T) {
	p.config.logger.Debug(p.ctx, "Starting group cleanup",
		"group_id", groupID,
		"task_count", len(tasks))

	// First check if the group still exists
	if _, exists := p.groups[groupID]; !exists {
		p.config.logger.Debug(p.ctx, "Group already cleaned up", "group_id", groupID)
		return
	}

	p.config.logger.Debug(p.ctx, "Cleaning up group resources", "group_id", groupID)

	if p.config.OnGroupRemoved != nil {
		p.config.OnGroupRemoved(groupID, tasks)
	}

	// Clean up group data
	delete(p.groups, groupID)

	// Clean up pool if it exists
	if _, exists := p.pools[groupID]; exists {
		if err := p.releaseGroupPool(groupID); err != nil {
			p.config.logger.Error(p.ctx, "Failed to release group pool",
				"group_id", groupID,
				"error", err)
		}
	}

	p.config.logger.Info(p.ctx, "Group cleanup completed", "group_id", groupID)
}

func (p *BlockingPool[T, GID, TID]) assignPoolToGroup(groupID GID) error {
	p.config.logger.Debug(p.ctx, "Attempting to assign pool to group", "group_id", groupID)

	group := p.groups[groupID]
	if group == nil {
		p.config.logger.Error(p.ctx, "Group not found", "group_id", groupID)
		return fmt.Errorf("group not found")
	}

	// If group already has pool, nothing to do
	if group.hasPool {
		p.config.logger.Debug(p.ctx, "Group already has pool assigned", "group_id", groupID)
		return nil
	}

	// Create new pool if under limit
	if len(p.activeGroups) < p.config.maxConcurrentPools {
		p.config.logger.Debug(p.ctx, "Creating new pool for group",
			"group_id", groupID,
			"active_pools", len(p.activeGroups),
			"max_pools", p.config.maxConcurrentPools)

		pool, err := p.createPoolForGroup(groupID)
		if err != nil {
			p.config.logger.Error(p.ctx, "Failed to create pool for group",
				"group_id", groupID,
				"error", err)
			return fmt.Errorf("failed to create pool: %w", err)
		}

		p.pools[groupID] = pool
		p.activeGroups[groupID] = struct{}{}
		group.hasPool = true

		p.config.logger.Info(p.ctx, "Pool assigned to group successfully",
			"group_id", groupID)
		return nil
	}

	// Otherwise add to pending if not already there
	for _, id := range p.pendingGroups {
		if id == groupID {
			p.config.logger.Debug(p.ctx, "Group already in pending queue", "group_id", groupID)
			return nil
		}
	}
	p.pendingGroups = append(p.pendingGroups, groupID)
	p.config.logger.Info(p.ctx, "Group added to pending queue",
		"group_id", groupID,
		"pending_count", len(p.pendingGroups))
	return nil
}

// releaseGroupPool cleans up a group's pool and assigns it to next pending group
func (p *BlockingPool[T, GID, TID]) releaseGroupPool(groupID GID) error {
	p.config.logger.Debug(p.ctx, "Starting pool release for group", "group_id", groupID)

	pool, exists := p.pools[groupID]
	if !exists {
		p.config.logger.Error(p.ctx, "No pool found for group", "group_id", groupID)
		return fmt.Errorf("no pool found for group")
	}

	p.config.logger.Debug(p.ctx, "Closing pool for group", "group_id", groupID)
	if err := pool.Close(); err != nil {
		p.config.logger.Error(p.ctx, "Failed to close pool for group",
			"group_id", groupID,
			"error", err)
		return fmt.Errorf("failed to close pool: %w", err)
	}
	delete(p.pools, groupID)
	delete(p.activeGroups, groupID)

	if p.config.OnPoolDestroyed != nil {
		p.config.OnPoolDestroyed(groupID)
	}

	// Process next pending group if any
	if len(p.pendingGroups) > 0 {
		nextGroupID := p.pendingGroups[0]
		p.pendingGroups = p.pendingGroups[1:]

		if nextGroup := p.groups[nextGroupID]; nextGroup != nil {
			if newPool, err := p.createPoolForGroup(nextGroupID); err == nil {
				p.pools[nextGroupID] = newPool
				p.activeGroups[nextGroupID] = struct{}{}
				nextGroup.hasPool = true

				// Get tasks that need to be submitted
				nextGroup.mu.Lock()
				pendingTasks := []*blockingTaskState[T, GID, TID]{}
				for _, taskState := range nextGroup.tasks {
					pendingTasks = append(pendingTasks, taskState)
				}
				nextGroup.mu.Unlock()

				// Submit all pending tasks
				for _, task := range pendingTasks {
					task.mu.Lock()
					if task.completed {
						task.mu.Unlock()
						continue
					}
					pp.Println("Submitting pending task", "groupID", task.groupID, "taskID", task.taskID)
					if err := newPool.SubmitToFreeWorker(task.task, task.options...); err != nil {
						p.config.logger.Error(p.ctx, "Failed to submit pending task",
							"group_id", nextGroupID,
							"error", err)
					}
					task.mu.Unlock()
				}
			} else {
				p.config.logger.Error(p.ctx, "Failed to create pool for next group, returning to pending queue",
					"next_group_id", nextGroupID,
					"error", err)
				p.pendingGroups = append(p.pendingGroups, nextGroupID)
			}
		}
	}

	p.config.logger.Debug(p.ctx, "Pool release completed", "group_id", groupID)
	return nil
}

// scaleWorkersIfNeeded ensures a group's pool has enough workers
func (p *BlockingPool[T, GID, TID]) scaleWorkersIfNeeded(groupID GID) error {
	p.config.logger.Debug(p.ctx, "Checking worker scaling for group", "group_id", groupID)

	pool := p.pools[groupID]
	if pool == nil {
		p.config.logger.Error(p.ctx, "No pool found for group", "group_id", groupID)
		return fmt.Errorf("no pool found for group")
	}

	// Count queued tasks
	var queued int64
	pool.RangeWorkerQueues(func(wid int, qs int64) bool {
		queued += qs
		return true
	})

	// Count processing tasks
	processing := pool.ProcessingCount()

	// Calculate desired worker count
	desired := int(queued + processing)
	if processing > 0 {
		desired++
	}

	// Apply maxWorkersPerPool limit if set
	if p.config.maxConcurrentWorkersPerPool > 0 && desired > p.config.maxConcurrentWorkersPerPool {
		desired = p.config.maxConcurrentWorkersPerPool
		p.config.logger.Debug(p.ctx, "Desired workers capped by maxWorkersPerPool",
			"group_id", groupID,
			"max_workers", p.config.maxConcurrentWorkersPerPool,
			"desired_workers", desired)
	}

	p.config.logger.Debug(p.ctx, "Worker scaling metrics",
		"group_id", groupID,
		"queued_tasks", queued,
		"processing_tasks", processing,
		"desired_workers", desired,
		"max_workers_per_pool", p.config.maxConcurrentWorkersPerPool)

	workers, err := pool.Workers()
	if err != nil {
		p.config.logger.Error(p.ctx, "Failed to get workers from pool",
			"group_id", groupID,
			"error", err)
		return err
	}
	currentWorkers := len(workers)

	// currentWorkers := len(pool.GetFreeWorkers())
	if currentWorkers < desired {
		toAdd := desired - currentWorkers
		p.config.logger.Info(p.ctx, "Scaling up workers",
			"group_id", groupID,
			"current_workers", currentWorkers,
			"workers_to_add", toAdd)

		for i := 0; i < toAdd; i++ {
			worker := p.workerFactory()
			if err := pool.Add(worker, nil); err != nil {
				p.config.logger.Error(p.ctx, "Failed to add worker",
					"group_id", groupID,
					"error", err)
				return err
			}
			if p.config.OnWorkerAdded != nil {
				p.config.OnWorkerAdded(groupID, currentWorkers+i+1)
			}
			p.config.logger.Debug(p.ctx, "Added new worker",
				"group_id", groupID,
				"worker_number", currentWorkers+i+1)
		}
	}

	// pp.Println(pool.GetMetricsSnapshot())

	return nil
}

type submitConfig struct {
	queueNotification     *QueuedNotification
	processedNotification *ProcessedNotification
	metadata              map[string]any
}

type SubmitOption[T any] func(*submitConfig)

func WithBlockingQueueNotification[T any](notification *QueuedNotification) SubmitOption[T] {
	return func(c *submitConfig) {
		c.queueNotification = notification
	}
}

func WithBlockingProcessedNotification[T any](notification *ProcessedNotification) SubmitOption[T] {
	return func(c *submitConfig) {
		c.processedNotification = notification
	}
}

func WithBlockingMetadata[T any](metadata map[string]any) SubmitOption[T] {
	return func(c *submitConfig) {
		c.metadata = metadata
	}
}

// Submit handles task submission, managing group pools as needed
func (p *BlockingPool[T, GID, TID]) Submit(data T, opt ...SubmitOption[T]) error {
	p.config.logger.Debug(p.ctx, "Processing task submission")

	if p.config.OnTaskSubmitted != nil {
		p.config.OnTaskSubmitted(data)
	}

	dtask, ok := any(data).(BlockingDependentTask[GID, TID])
	if !ok {
		return fmt.Errorf("task does not implement DependentTask interface")
	}

	groupID := dtask.GetGroupID()
	taskID := dtask.GetTaskID()

	// Create task state with all metadata
	state := &blockingTaskState[T, GID, TID]{
		task:         data,
		taskID:       taskID,
		groupID:      groupID,
		completionCh: make(chan struct{}),
		options:      []TaskOption[T]{WithTaskBounceRetry[T]()},
	}

	// Add any additional options
	cfg := submitConfig{}
	for _, o := range opt {
		o(&cfg)
	}

	if cfg.queueNotification != nil {
		state.options = append(state.options, WithTaskQueuedNotification[T](cfg.queueNotification))
	}
	if cfg.processedNotification != nil {
		state.options = append(state.options, WithTaskProcessedNotification[T](cfg.processedNotification))
	}
	if cfg.metadata != nil {
		m := NewMetadata()
		m.store = cfg.metadata
		state.options = append(state.options, WithTaskMetadata[T](m))
	}

	p.mu.Lock()
	group, exists := p.groups[groupID]
	if !exists {
		group = &blockingTaskGroup[T, GID, TID]{
			id:        groupID,
			tasks:     make(map[TID]*blockingTaskState[T, GID, TID]),
			completed: make(map[TID]bool),
			hasPool:   false,
		}
		p.groups[groupID] = group

		if p.config.OnGroupCreated != nil {
			p.config.OnGroupCreated(groupID)
		}

		// If group needs a pool, add to pending
		isPending := false
		for _, pendingID := range p.pendingGroups {
			if pendingID == groupID {
				isPending = true
				break
			}
		}

		if !isPending {
			p.config.logger.Debug(p.ctx, "Adding group to pending queue",
				"group_id", groupID)
			p.pendingGroups = append(p.pendingGroups, groupID)
		}

		// Try to assign a pool if possible
		if err := p.assignPoolToGroup(groupID); err != nil {
			// Cache the task state and return
			p.cacheTaskState(groupID, state)
			p.mu.Unlock()
			return nil
		}
	}

	// Store task state in group
	group.mu.Lock()
	group.tasks[taskID] = state
	group.mu.Unlock()

	// If group has no pool, cache the task state
	if !group.hasPool {
		p.cacheTaskState(groupID, state)
		p.mu.Unlock()
		return nil
	}

	pool := p.pools[groupID]
	if err := p.scaleWorkersIfNeeded(groupID); err != nil {
		p.mu.Unlock()
		return fmt.Errorf("failed to scale workers: %w", err)
	}

	if p.config.OnTaskStarted != nil {
		p.config.OnTaskStarted(data)
	}

	// Try to submit with complete state and options
	err := pool.SubmitToFreeWorker(state.task, state.options...)
	if err == ErrNoWorkersAvailable {
		p.cacheTaskState(groupID, state)
		p.mu.Unlock()
		return nil
	}

	p.mu.Unlock()
	return err
}

// handleTaskCompletion processes task completion
func (p *BlockingPool[T, GID, TID]) handleTaskCompletion(groupID GID, data T) {
	p.config.logger.Debug(p.ctx, "Processing task completion", "group_id", groupID)

	if p.config.OnTaskCompleted != nil {
		p.config.OnTaskCompleted(data)
	}

	// Check pending groups first
	p.mu.Lock()
	if len(p.activeGroups) < p.config.maxConcurrentPools && len(p.pendingGroups) > 0 {
		nextGroupID := p.pendingGroups[0]
		p.pendingGroups = p.pendingGroups[1:]

		if nextGroup := p.groups[nextGroupID]; nextGroup != nil {
			if err := p.assignPoolToGroup(nextGroupID); err == nil {
				// Pool assigned successfully, try to submit its cached states
				p.trySubmitCachedStates(nextGroupID)
			} else {
				// If pool assignment failed, put group back in pending queue
				p.pendingGroups = append(p.pendingGroups, nextGroupID)
			}
		}
	}
	p.mu.Unlock()

	// Try to submit cached states for the completing group
	p.trySubmitCachedStates(groupID)

	dtask := any(data).(BlockingDependentTask[GID, TID])
	taskID := dtask.GetTaskID()

	p.mu.RLock()
	group, exists := p.groups[groupID]
	if !exists {
		p.config.logger.Debug(p.ctx, "Group not found for completed task",
			"group_id", groupID,
			"task_id", taskID)
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()

	group.mu.Lock()
	task := group.tasks[taskID]
	if task == nil {
		p.config.logger.Debug(p.ctx, "Task not found in group",
			"group_id", groupID,
			"task_id", taskID)
		group.mu.Unlock()
		return
	}

	task.mu.Lock()
	task.completed = true
	close(task.completionCh)
	task.mu.Unlock()
	group.completed[taskID] = true

	p.config.logger.Debug(p.ctx, "Task marked as completed",
		"group_id", groupID,
		"task_id", taskID)

	allComplete := true
	for _, t := range group.tasks {
		if !t.completed {
			allComplete = false
			break
		}
	}

	// Get tasks before releasing lock
	var tasks []T
	if allComplete {
		p.config.logger.Info(p.ctx, "All tasks in group completed",
			"group_id", groupID)
		tasks = make([]T, 0, len(group.tasks))
		for _, taskState := range group.tasks {
			tasks = append(tasks, taskState.task)
		}
	}
	group.mu.Unlock()

	if allComplete {
		p.mu.Lock()
		// Double check the group still exists
		if cleanGroup, exists := p.groups[groupID]; exists && cleanGroup == group {
			p.config.logger.Debug(p.ctx, "Cleaning up completed group", "group_id", groupID)

			delete(p.groups, groupID)
			// Notify about group removal
			if p.config.OnGroupRemoved != nil {
				p.config.OnGroupRemoved(groupID, tasks)
			}
			// Then release its pool
			if err := p.releaseGroupPool(groupID); err != nil {
				p.config.logger.Error(p.ctx, "Error releasing group pool",
					"group_id", groupID,
					"error", err)
			}
		}
		p.mu.Unlock()
	}
}

// WaitForTask blocks until the specified task completes
func (p *BlockingPool[T, GID, TID]) WaitForTask(groupID GID, taskID TID) error {
	p.config.logger.Debug(p.ctx, "Waiting for task completion",
		"group_id", groupID,
		"task_id", taskID)

	p.mu.RLock()
	group, exists := p.groups[groupID]
	if !exists {
		p.config.logger.Error(p.ctx, "Group not found",
			"group_id", groupID,
			"task_id", taskID)
		p.mu.RUnlock()
		return fmt.Errorf("group %v not found", groupID)
	}

	group.mu.RLock()
	task, exists := group.tasks[taskID]
	if !exists {
		p.config.logger.Error(p.ctx, "Task not found in group",
			"group_id", groupID,
			"task_id", taskID)
		group.mu.RUnlock()
		p.mu.RUnlock()
		return fmt.Errorf("task %v not found in group %v", taskID, groupID)
	}
	completionCh := task.completionCh
	group.mu.RUnlock()
	p.mu.RUnlock()

	select {
	case <-completionCh:
		p.config.logger.Debug(p.ctx, "Task completed",
			"group_id", groupID,
			"task_id", taskID)
		return nil
	case <-p.ctx.Done():
		p.config.logger.Error(p.ctx, "Context cancelled while waiting for task",
			"group_id", groupID,
			"task_id", taskID)
		return p.ctx.Err()
	}
}

// WaitWithCallback monitors all pool progress
func (p *BlockingPool[T, GID, TID]) WaitWithCallback(
	ctx context.Context,
	callback func(queueSize, processingCount, deadTaskCount int) bool,
	interval time.Duration,
) error {
	p.config.logger.Debug(p.ctx, "Starting wait with callback", "interval", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.config.logger.Error(p.ctx, "Context cancelled during wait")
			return ctx.Err()
		case <-ticker.C:
			p.mu.RLock()
			queued := int(p.snapshot.TotalTasksSubmitted - p.snapshot.TotalTasksProcessed)
			processing := int(p.snapshot.TotalTasksProcessed - p.snapshot.TotalTasksSucceeded - p.snapshot.TotalTasksFailed)
			deadTasks := int(p.snapshot.TotalDeadTasks)

			p.mu.RUnlock()

			p.config.logger.Debug(p.ctx, "Pool metrics",
				"queued", queued,
				"processing", processing,
				"dead_tasks", deadTasks)

			if !callback(queued, processing, deadTasks) {
				p.config.logger.Debug(p.ctx, "Callback returned false, ending wait")
				return nil
			}
		}
	}
}

// Close shuts down all pools
func (p *BlockingPool[T, GID, TID]) Close() error {
	p.config.logger.Info(p.ctx, "Closing blocking pool")

	p.mu.Lock()
	defer p.mu.Unlock()

	p.cancel()

	// Close all active pools
	var firstErr error
	for groupID, pool := range p.pools {
		p.config.logger.Debug(p.ctx, "Closing pool for group", "group_id", groupID)
		if err := pool.Close(); err != nil && firstErr == nil {
			p.config.logger.Error(p.ctx, "Error closing pool",
				"group_id", groupID,
				"error", err)
			firstErr = err
		}
		if p.config.OnPoolDestroyed != nil {
			p.config.OnPoolDestroyed(groupID)
		}
	}

	if p.config.OnPoolClosed != nil {
		p.config.OnPoolClosed()
	}

	p.config.logger.Info(p.ctx, "Blocking pool closed successfully")
	return firstErr
}

// BlockingRequestResponse manages the lifecycle of a task request and its response
type BlockingRequestResponse[T any, R any, GID comparable, TID comparable] struct {
	groupID     GID              // The group ID
	taskID      TID              // The task ID
	request     T                // The request data
	done        chan struct{}    // Channel to signal completion
	response    R                // Stores the successful response
	err         error            // Stores any error that occurred
	mu          deadlock.RWMutex // Protects response and err
	isCompleted bool             // Indicates if request is completed
}

// NewBlockingRequestResponse creates a new BlockingRequestResponse instance
func NewBlockingRequestResponse[T any, R any, GID comparable, TID comparable](request T, gid GID, tid TID) *BlockingRequestResponse[T, R, GID, TID] {
	return &BlockingRequestResponse[T, R, GID, TID]{
		request: request,
		done:    make(chan struct{}),
		groupID: gid,
		taskID:  tid,
	}
}

func (rr BlockingRequestResponse[T, R, GID, TID]) GetGroupID() GID {
	return rr.groupID
}

func (rr BlockingRequestResponse[T, R, GID, TID]) GetTaskID() TID {
	return rr.taskID
}

// Safely consults the request data
func (rr *BlockingRequestResponse[T, R, GID, TID]) ConsultRequest(fn func(T) error) error {
	rr.mu.Lock()
	err := fn(rr.request)
	rr.mu.Unlock()
	return err
}

// Complete safely marks the request as complete with a response
func (rr *BlockingRequestResponse[T, R, GID, TID]) Complete(response R) {
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
func (rr *BlockingRequestResponse[T, R, GID, TID]) CompleteWithError(err error) {
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
func (rr *BlockingRequestResponse[T, R, GID, TID]) Done() <-chan struct{} {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	return rr.done
}

// Err returns any error that occurred during the request
func (rr *BlockingRequestResponse[T, R, GID, TID]) Err() error {
	var err error
	rr.mu.RLock()
	err = rr.err
	rr.mu.RUnlock()
	return err
}

// Wait waits for the request to complete and returns the response and any error
func (rr *BlockingRequestResponse[T, R, GID, TID]) Wait(ctx context.Context) (R, error) {
	select {
	case <-rr.done:
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
