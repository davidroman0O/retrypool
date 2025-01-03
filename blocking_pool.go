package retrypool

import (
	"context"
	"fmt"
	"log/slog"
	"time"

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
	maxActivePools int // Maximum number of active pools (one per group)
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
		c.maxActivePools = max
	}
}

// All the callback setters

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

// NewBlockingPool constructs a BlockingPool with the given options.
func NewBlockingPool[T any, GID comparable, TID comparable](
	ctx context.Context,
	opt ...BlockingPoolOption[T],
) (*BlockingPool[T, GID, TID], error) {
	cfg := BlockingConfig[T]{
		logger:         NewLogger(slog.LevelDebug),
		maxActivePools: 1, // Default to one active pool
	}

	cfg.logger.Disable()

	for _, o := range opt {
		o(&cfg)
	}

	cfg.logger.Info(ctx, "Creating new BlockingPool",
		"max_active_pools", cfg.maxActivePools,
		"has_worker_factory", cfg.workerFactory != nil)

	if cfg.workerFactory == nil {
		cfg.logger.Error(ctx, "Worker factory not provided")
		return nil, fmt.Errorf("worker factory must be provided")
	}

	ctx, cancel := context.WithCancel(ctx)
	pool := &BlockingPool[T, GID, TID]{
		pools:         make(map[GID]Pooler[T]),
		workerFactory: cfg.workerFactory,
		groups:        make(map[GID]*blockingTaskGroup[T, GID, TID]),
		activeGroups:  make(map[GID]struct{}),
		pendingGroups: make([]GID, 0),
		config:        cfg,
		ctx:           ctx,
		cancel:        cancel,
	}

	cfg.logger.Info(ctx, "BlockingPool created successfully")
	return pool, nil
}

// Scale the amount of parallel pools that can be active at the same time
func (p *BlockingPool[T, GID, TID]) SetActivePools(max int) {
	if max < 1 {
		max = 1
	}
	p.mu.Lock()
	p.config.maxActivePools = max
	p.mu.Unlock()
}

// Get all the metrics of all pools
func (p *BlockingPool[T, GID, TID]) GetMetricsSnapshot() MetricsSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var metrics MetricsSnapshot = MetricsSnapshot{
		Queues: map[int]int{},
	}
	for _, pool := range p.pools {
		poolMetrics := pool.GetMetricsSnapshot()
		metrics.TasksSubmitted += poolMetrics.TasksSubmitted
		metrics.TasksProcessed += poolMetrics.TasksProcessed
		metrics.DeadTasks += poolMetrics.DeadTasks
		for q, m := range poolMetrics.Queues {
			metrics.Queues[q] += m
		}
	}
	return metrics
}

func (p *BlockingPool[T, GID, TID]) RangeWorkerQueues(f func(workerID int, queueSize int64) bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, pool := range p.pools {
		pool.RangeWorkerQueues(f)
	}
}

func (p *BlockingPool[T, GID, TID]) RangeWorkers(f func(workerID int, worker Worker[T]) bool) {
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

// createPoolForGroup creates a new worker pool for a group
func (p *BlockingPool[T, GID, TID]) createPoolForGroup(groupID GID) (Pooler[T], error) {
	p.config.logger.Debug(p.ctx, "Creating new pool for group", "group_id", groupID)

	worker := p.workerFactory()
	p.config.logger.Debug(p.ctx, "Created initial worker for group", "group_id", groupID)

	// Create pool with synchronous mode and single retry
	pool := New[T](p.ctx, []Worker[T]{worker},
		WithAttempts[T](1),
		WithLogger[T](p.config.logger))

	// Set completion handler
	pool.SetOnTaskSuccess(func(data T) {
		p.config.logger.Debug(p.ctx, "Task completed successfully in group",
			"group_id", groupID)
		p.handleTaskCompletion(groupID, data)
	})

	// Add failure handler to trigger group cleanup
	pool.SetOnTaskFailure(func(data T, err error) TaskAction {
		p.config.logger.Error(p.ctx, "Task failed in group",
			"group_id", groupID,
			"error", err)

		if p.config.OnTaskFailed != nil {
			p.config.OnTaskFailed(data, err)
		}

		// Get group's tasks without holding the pool lock
		var tasks []T
		p.mu.Lock()
		group := p.groups[groupID]
		if group != nil {
			group.mu.Lock()
			for _, taskState := range group.tasks {
				tasks = append(tasks, taskState.task)
			}
			group.mu.Unlock()
			p.config.logger.Debug(p.ctx, "Collected tasks for failed group",
				"group_id", groupID,
				"task_count", len(tasks))
		}
		p.mu.Unlock()

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

	p.mu.Lock()
	defer p.mu.Unlock()

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
	if len(p.activeGroups) < p.config.maxActivePools {
		p.config.logger.Debug(p.ctx, "Creating new pool for group",
			"group_id", groupID,
			"active_pools", len(p.activeGroups),
			"max_pools", p.config.maxActivePools)

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
				pendingTasks := make([]T, 0, len(nextGroup.tasks))
				for _, taskState := range nextGroup.tasks {
					pendingTasks = append(pendingTasks, taskState.task)
				}
				nextGroup.mu.Unlock()

				// Submit all pending tasks
				for _, task := range pendingTasks {
					if err := newPool.SubmitToFreeWorker(task); err != nil {
						p.config.logger.Error(p.ctx, "Failed to submit pending task",
							"group_id", nextGroupID,
							"error", err)
					}
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

	p.config.logger.Debug(p.ctx, "Worker scaling metrics",
		"group_id", groupID,
		"queued_tasks", queued,
		"processing_tasks", processing,
		"desired_workers", desired)

	currentWorkers := len(pool.GetFreeWorkers())
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

	return nil
}

type submitConfig struct {
	queueNotification     *QueuedNotification
	processedNotification *ProcessedNotification
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

// Submit handles task submission, managing group pools as needed
func (p *BlockingPool[T, GID, TID]) Submit(data T, opt ...SubmitOption[T]) error {
	p.config.logger.Debug(p.ctx, "Processing task submission")

	if p.config.OnTaskSubmitted != nil {
		p.config.OnTaskSubmitted(data)
	}

	dtask, ok := any(data).(BlockingDependentTask[GID, TID])
	if !ok {
		p.config.logger.Error(p.ctx, "Task does not implement DependentTask interface")
		return fmt.Errorf("task does not implement DependentTask interface")
	}

	groupID := dtask.GetGroupID()
	taskID := dtask.GetTaskID()

	p.config.logger.Debug(p.ctx, "Task details",
		"group_id", groupID,
		"task_id", taskID)

	opts := []TaskOption[T]{
		WithBounceRetry[T](),
	}

	cfg := submitConfig{}
	for _, o := range opt {
		o(&cfg)
	}

	if cfg.queueNotification != nil {
		opts = append(opts, WithQueuedNotification[T](cfg.queueNotification))
	}
	if cfg.processedNotification != nil {
		opts = append(opts, WithProcessedNotification[T](cfg.processedNotification))
	}

	p.mu.Lock()
	group, exists := p.groups[groupID]
	if !exists {
		p.config.logger.Debug(p.ctx, "Creating new group", "group_id", groupID)
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

		// Try to assign a pool if not at limit
		if err := p.assignPoolToGroup(groupID); err != nil {
			// Group will remain in pending state
			p.mu.Unlock()
			p.config.logger.Error(p.ctx, "Failed to assign pool to group",
				"group_id", groupID,
				"error", err)
			return fmt.Errorf("failed to assign pool: %w", err)
		}
	}

	// Create task state
	task := &blockingTaskState[T, GID, TID]{
		task:         data,
		taskID:       taskID,
		groupID:      groupID,
		completionCh: make(chan struct{}),
		options:      opts,
	}

	group.mu.Lock()
	group.tasks[taskID] = task
	group.mu.Unlock()

	// If group has no pool yet, just store the task
	if !group.hasPool {
		p.config.logger.Debug(p.ctx, "Group has no pool, task queued for later processing",
			"group_id", groupID,
			"task_id", taskID)
		p.mu.Unlock()
		return nil
	}

	pool := p.pools[groupID]
	// Need to scale workers?
	if err := p.scaleWorkersIfNeeded(groupID); err != nil {
		p.mu.Unlock()
		p.config.logger.Error(p.ctx, "Failed to scale workers",
			"group_id", groupID,
			"error", err)
		return fmt.Errorf("failed to scale workers: %w", err)
	}
	p.mu.Unlock()

	if p.config.OnTaskStarted != nil {
		p.config.OnTaskStarted(data)
	}

	p.config.logger.Debug(p.ctx, "Submitting task to pool",
		"group_id", groupID,
		"task_id", taskID)
	return pool.SubmitToFreeWorker(task.task, opts...)
}

// handleTaskCompletion processes task completion
func (p *BlockingPool[T, GID, TID]) handleTaskCompletion(groupID GID, data T) {
	p.config.logger.Debug(p.ctx, "Processing task completion", "group_id", groupID)

	if p.config.OnTaskCompleted != nil {
		p.config.OnTaskCompleted(data)
	}

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

	task.completed = true
	close(task.completionCh)
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
			queued := 0
			processing := 0
			deadTasks := 0

			// Sum metrics across all active pools
			for _, pool := range p.pools {
				metrics := pool.GetMetricsSnapshot()
				queued += int(metrics.TasksSubmitted - metrics.TasksProcessed)
				processing += int(pool.ProcessingCount())
				deadTasks += int(metrics.DeadTasks)
			}
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
	groupID     GID            // The group ID
	taskID      TID            // The task ID
	Request     T              // The request data
	done        chan struct{}  // Channel to signal completion
	response    R              // Stores the successful response
	err         error          // Stores any error that occurred
	mu          deadlock.Mutex // Protects response and err
	isCompleted bool           // Indicates if request is completed
}

// NewBlockingRequestResponse creates a new BlockingRequestResponse instance
func NewBlockingRequestResponse[T any, R any, GID comparable, TID comparable](request T, gid GID, tid TID) *BlockingRequestResponse[T, R, GID, TID] {
	return &BlockingRequestResponse[T, R, GID, TID]{
		Request: request,
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

// Complete safely marks the request as complete with a response
func (rr *BlockingRequestResponse[T, R, GID, TID]) Complete(response R) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if !rr.isCompleted {
		rr.response = response
		rr.isCompleted = true
		close(rr.done)
	}
}

// CompleteWithError safely marks the request as complete with an error
func (rr *BlockingRequestResponse[T, R, GID, TID]) CompleteWithError(err error) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if !rr.isCompleted {
		rr.err = err
		rr.isCompleted = true
		close(rr.done)
	}
}

// Done returns a channel that's closed when the request is complete
func (rr *BlockingRequestResponse[T, R, GID, TID]) Done() <-chan struct{} {
	return rr.done
}

// Err returns any error that occurred during the request
func (rr *BlockingRequestResponse[T, R, GID, TID]) Err() error {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	return rr.err
}

// Wait waits for the request to complete and returns the response and any error
func (rr *BlockingRequestResponse[T, R, GID, TID]) Wait(ctx context.Context) (R, error) {
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
			return rr.response, rr.err
		}
	}
}
