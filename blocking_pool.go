package retrypool

import (
	"context"
	"fmt"
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

/// TODO: we need to clean up groups and tasks when they are no longer needed

// BlockingConfig holds basic config - each active group gets its own pool of workers
type BlockingConfig[T any] struct {
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
	OnGroupCompleted func(groupID any)
	OnGroupRemoved   func(groupID any)
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

func WithBlockingOnGroupRemoved[T any](cb func(groupID any)) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.OnGroupRemoved = cb
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

func WithBlockingOnGroupCompleted[T any](cb func(groupID any)) BlockingPoolOption[T] {
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
		maxActivePools: 1, // Default to one active pool
	}

	for _, o := range opt {
		o(&cfg)
	}

	// Validation
	if cfg.workerFactory == nil {
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

	return pool, nil
}

// createPoolForGroup creates a new worker pool for a group
func (p *BlockingPool[T, GID, TID]) createPoolForGroup(groupID GID) (Pooler[T], error) {
	// Create initial worker
	worker := p.workerFactory()

	// Create pool with synchronous mode and single retry
	pool := New[T](p.ctx, []Worker[T]{worker},
		WithAttempts[T](1))

	// Set completion handler
	pool.SetOnTaskSuccess(func(data T) {
		p.handleTaskCompletion(groupID, data)
	})

	if p.config.OnPoolCreated != nil {
		p.config.OnPoolCreated(groupID)
	}

	if p.config.OnWorkerAdded != nil {
		p.config.OnWorkerAdded(groupID, 0)
	}

	return pool, nil
}

// assignPoolToGroup either creates a new pool or reuses an available one
func (p *BlockingPool[T, GID, TID]) assignPoolToGroup(groupID GID) error {
	group := p.groups[groupID]
	if group == nil {
		return fmt.Errorf("group not found")
	}

	// Create new pool if under limit
	if len(p.pools) < p.config.maxActivePools {
		pool, err := p.createPoolForGroup(groupID)
		if err != nil {
			return fmt.Errorf("failed to create pool: %w", err)
		}

		p.pools[groupID] = pool
		p.activeGroups[groupID] = struct{}{}
		group.hasPool = true

		return nil
	}

	// Otherwise, no pools available
	p.pendingGroups = append(p.pendingGroups, groupID)
	return nil
}

// releaseGroupPool cleans up a group's pool and assigns it to next pending group
func (p *BlockingPool[T, GID, TID]) releaseGroupPool(groupID GID) error {
	_, exists := p.pools[groupID]
	if !exists {
		return fmt.Errorf("no pool found for group")
	}

	// Clean up pool
	delete(p.pools, groupID)
	delete(p.activeGroups, groupID)
	if group := p.groups[groupID]; group != nil {
		group.hasPool = false
	}

	if p.config.OnPoolDestroyed != nil {
		p.config.OnPoolDestroyed(groupID)
	}

	// Activate next pending group if any
	if len(p.pendingGroups) > 0 {
		nextGroupID := p.pendingGroups[0]
		p.pendingGroups = p.pendingGroups[1:]

		newPool, err := p.createPoolForGroup(nextGroupID)
		if err != nil {
			return fmt.Errorf("failed to create pool for next group: %w", err)
		}

		p.pools[nextGroupID] = newPool
		p.activeGroups[nextGroupID] = struct{}{}
		if group := p.groups[nextGroupID]; group != nil {
			group.hasPool = true
		}

		if p.config.OnPoolCreated != nil {
			p.config.OnPoolCreated(nextGroupID)
		}
	}

	return nil
}

// scaleWorkersIfNeeded ensures a group's pool has enough workers
func (p *BlockingPool[T, GID, TID]) scaleWorkersIfNeeded(groupID GID) error {
	pool := p.pools[groupID]
	if pool == nil {
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
		desired++ // Extra worker for child tasks
	}

	// Get current workers
	cur := pool.GetFreeWorkers()
	currentCount := len(cur)

	// Scale up if needed
	if currentCount < desired {
		toAdd := desired - currentCount
		for i := 0; i < toAdd; i++ {
			w := p.workerFactory()
			if err := pool.Add(w, nil); err != nil {
				return err
			}
			if p.config.OnWorkerAdded != nil {
				p.config.OnWorkerAdded(groupID, currentCount+i+1)
			}
		}
	}

	return nil
}

// Submit handles task submission, managing group pools as needed
func (p *BlockingPool[T, GID, TID]) Submit(data T) error {
	if p.config.OnTaskSubmitted != nil {
		p.config.OnTaskSubmitted(data)
	}

	dtask, ok := any(data).(DependentTask[GID, TID])
	if !ok {
		return fmt.Errorf("task does not implement DependentTask interface")
	}

	groupID := dtask.GetGroupID()
	taskID := dtask.GetTaskID()

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

		// Try to assign a pool if not at limit
		if err := p.assignPoolToGroup(groupID); err != nil {
			// Group will remain in pending state
			p.mu.Unlock()
			return fmt.Errorf("failed to assign pool: %w", err)
		}
	}

	// Create task state
	task := &blockingTaskState[T, GID, TID]{
		task:         data,
		taskID:       taskID,
		groupID:      groupID,
		completionCh: make(chan struct{}),
	}

	group.mu.Lock()
	group.tasks[taskID] = task
	group.mu.Unlock()

	// If group has no pool yet, just store the task
	if !group.hasPool {
		p.mu.Unlock()
		return nil
	}

	pool := p.pools[groupID]
	// Need to scale workers?
	if err := p.scaleWorkersIfNeeded(groupID); err != nil {
		p.mu.Unlock()
		return fmt.Errorf("failed to scale workers: %w", err)
	}
	p.mu.Unlock()

	if p.config.OnTaskStarted != nil {
		p.config.OnTaskStarted(data)
	}

	// Submit to this group's pool
	return pool.SubmitToFreeWorker(task.task, WithBounceRetry[T]())
}

// handleTaskCompletion processes task completion
func (p *BlockingPool[T, GID, TID]) handleTaskCompletion(groupID GID, data T) {
	if p.config.OnTaskCompleted != nil {
		p.config.OnTaskCompleted(data)
	}

	dtask := any(data).(DependentTask[GID, TID])
	taskID := dtask.GetTaskID()

	p.mu.RLock()
	group, exists := p.groups[groupID]
	if !exists {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()

	group.mu.Lock()
	task := group.tasks[taskID]
	if task == nil {
		group.mu.Unlock()
		return
	}

	task.mu.Lock()
	task.completed = true
	close(task.completionCh)
	task.mu.Unlock()

	group.completed[taskID] = true

	// Check if all tasks in group are complete
	allComplete := true
	for _, t := range group.tasks {
		if !t.completed {
			allComplete = false
			break
		}
	}
	group.mu.Unlock()

	if allComplete {
		p.mu.Lock()
		// Release pool and activate next group
		if err := p.releaseGroupPool(groupID); err != nil {
			p.mu.Unlock()
			return
		}

		if p.config.OnGroupCompleted != nil {
			p.config.OnGroupCompleted(groupID)
		}

		// Clean up group data by removing it from groups
		delete(p.groups, groupID)

		// Trigger group removal callback
		if p.config.OnGroupRemoved != nil {
			p.config.OnGroupRemoved(groupID)
		}

		p.mu.Unlock()
	}
}

// WaitForTask blocks until the specified task completes
func (p *BlockingPool[T, GID, TID]) WaitForTask(groupID GID, taskID TID) error {
	p.mu.RLock()
	group, exists := p.groups[groupID]
	if !exists {
		p.mu.RUnlock()
		return fmt.Errorf("group %v not found", groupID)
	}

	group.mu.RLock()
	task, exists := group.tasks[taskID]
	if !exists {
		group.mu.RUnlock()
		p.mu.RUnlock()
		return fmt.Errorf("task %v not found in group %v", taskID, groupID)
	}
	completionCh := task.completionCh
	group.mu.RUnlock()
	p.mu.RUnlock()

	select {
	case <-completionCh:
		return nil
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

// WaitWithCallback monitors all pool progress
func (p *BlockingPool[T, GID, TID]) WaitWithCallback(
	ctx context.Context,
	callback func(queueSize, processingCount, deadTaskCount int) bool,
	interval time.Duration,
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
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

			if !callback(queued, processing, deadTasks) {
				return nil
			}
		}
	}
}

// Close shuts down all pools
func (p *BlockingPool[T, GID, TID]) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.cancel()

	// Close all active pools
	var firstErr error
	for groupID, pool := range p.pools {
		if err := pool.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if p.config.OnPoolDestroyed != nil {
			p.config.OnPoolDestroyed(groupID)
		}
	}

	if p.config.OnPoolClosed != nil {
		p.config.OnPoolClosed()
	}

	return firstErr
}
