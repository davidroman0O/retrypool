package retrypool

import (
	"context"
	"fmt"
	"time"

	"github.com/sasha-s/go-deadlock"
)

/// TODO: we need to clean up groups and tasks when they are no longer needed
/// TODO: we need a channel that will be listened to to close a group
/// TODO: to scale and make it work, instead of having ONE pool we could have ONE pool PER active group, aka allocating a pool to a group that we will process
/// TODO: on failure, we can provide the pool's options for retry attempts, we will refuse unlimited attempts, adding to task tasks is a task failed and group fail which result in the group being removed

// BlockingConfig holds basic config—mainly the worker factory and min/max worker count.
type BlockingConfig[T any] struct {
	workerFactory WorkerFactory[T]
	minWorkers    int
	maxWorkers    int
	// Task callbacks
	OnTaskSubmitted func(task T)
	OnTaskStarted   func(task T)
	OnTaskCompleted func(task T)
	OnTaskFailed    func(task T, err error)
	// Group callbacks
	OnGroupCreated   func(groupID any)
	OnGroupCompleted func(groupID any)
	// Pool events
	OnWorkerAdded   func(workerID int)
	OnWorkerRemoved func(workerID int)
	OnPoolClosed    func()
}

// BlockingPoolOption is a functional option for configuring the blocking pool
type BlockingPoolOption[T any] func(*BlockingConfig[T])

// WithBlockingWorkerFactory sets the worker factory for creating new workers
func WithBlockingWorkerFactory[T any](factory WorkerFactory[T]) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		fmt.Printf("[CONFIG] Setting worker factory\n")
		c.workerFactory = factory
	}
}

// WithBlockingWorkerLimits specifies how many workers to keep (min) and the absolute limit (max)
func WithBlockingWorkerLimits[T any](min, max int) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		fmt.Printf("[CONFIG] Setting worker limits: min=%d, max=%d\n", min, max)
		if min < 1 {
			fmt.Printf("[CONFIG] min was < 1 => setting min=1\n")
			min = 1
		}
		c.minWorkers = min
		// Ensure max ≥ min
		if max < min {
			fmt.Printf("[CONFIG] max < min => setting max=min=%d\n", min)
			max = min
		}
		c.maxWorkers = max
	}
}

// Callback options
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

func WithBlockingOnWorkerAdded[T any](cb func(workerID int)) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.OnWorkerAdded = cb
	}
}

func WithBlockingOnWorkerRemoved[T any](cb func(workerID int)) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.OnWorkerRemoved = cb
	}
}

func WithBlockingOnPoolClosed[T any](cb func()) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		c.OnPoolClosed = cb
	}
}

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
	pool      *Pool[T] // Each group has its own worker pool
}

// BlockingPool is our high-level pool wrapper that delegates work to group-specific pools
type BlockingPool[T any, GID comparable, TID comparable] struct {
	mu            deadlock.RWMutex
	workerFactory WorkerFactory[T]
	groups        map[GID]*blockingTaskGroup[T, GID, TID]
	ctx           context.Context
	cancel        context.CancelFunc
	config        BlockingConfig[T]
}

// NewBlockingPool constructs a BlockingPool with the given options.
func NewBlockingPool[T any, GID comparable, TID comparable](
	ctx context.Context,
	opt ...BlockingPoolOption[T],
) (*BlockingPool[T, GID, TID], error) {
	fmt.Printf("[INIT] Creating new BlockingPool...\n")

	cfg := BlockingConfig[T]{
		minWorkers: 1,
		maxWorkers: 1, // default if not overridden
	}

	for _, o := range opt {
		o(&cfg)
	}

	// Validation
	if cfg.workerFactory == nil {
		return nil, fmt.Errorf("[INIT] worker factory must be provided")
	}
	if cfg.minWorkers < 1 {
		cfg.minWorkers = 1
	}
	if cfg.maxWorkers < cfg.minWorkers {
		cfg.maxWorkers = cfg.minWorkers
	}

	fmt.Printf("[INIT] minWorkers=%d, maxWorkers=%d\n", cfg.minWorkers, cfg.maxWorkers)

	ctx, cancel := context.WithCancel(ctx)
	pool := &BlockingPool[T, GID, TID]{
		workerFactory: cfg.workerFactory,
		groups:        make(map[GID]*blockingTaskGroup[T, GID, TID]),
		config:        cfg,
		ctx:           ctx,
		cancel:        cancel,
	}

	fmt.Printf("[INIT] Finished creating BlockingPool.\n")
	return pool, nil
}

// createGroupPool creates a new worker pool for a group with initial minimum workers
func (p *BlockingPool[T, GID, TID]) createGroupPool(groupID GID) (*Pool[T], error) {
	workers := make([]Worker[T], 0, p.config.minWorkers)
	for i := 0; i < p.config.minWorkers; i++ {
		workers = append(workers, p.workerFactory())
		if p.config.OnWorkerAdded != nil {
			p.config.OnWorkerAdded(i)
		}
	}

	return New[T](p.ctx, workers), nil
}

// scaleGroupPoolIfNeeded ensures group's pool has enough workers for tasks
func (p *BlockingPool[T, GID, TID]) scaleGroupPoolIfNeeded(group *blockingTaskGroup[T, GID, TID]) error {
	fmt.Println("[SCALE] Checking if we need to scale up workers for group...")

	// For blocking tasks, we need a worker per active task
	var activeTaskCount int
	group.mu.RLock()
	for _, task := range group.tasks {
		if !task.completed {
			activeTaskCount++
		}
	}
	group.mu.RUnlock()

	workers, _ := group.pool.Workers()
	workerCount := len(workers)

	fmt.Printf("[SCALE] active tasks: %d, current workers: %d\n", activeTaskCount, workerCount)

	if workerCount < activeTaskCount {
		needed := activeTaskCount - workerCount
		fmt.Printf("[SCALE] Adding %d workers\n", needed)

		for i := 0; i < needed; i++ {
			worker := p.workerFactory()
			if err := group.pool.Add(worker, nil); err != nil {
				return fmt.Errorf("[SCALE] failed to add worker: %w", err)
			}
			if p.config.OnWorkerAdded != nil {
				p.config.OnWorkerAdded(workerCount + i + 1)
			}
		}
	}

	return nil
}

// Submit enqueues the given data (which must implement DependentTask[GID,TID]).
// Each group gets its own worker pool that scales independently.
func (p *BlockingPool[T, GID, TID]) Submit(data T) error {
	fmt.Printf("[SUBMIT] Received Submit() for data=%#v\n", data)

	if p.config.OnTaskSubmitted != nil {
		p.config.OnTaskSubmitted(data)
	}

	dtask, ok := any(data).(DependentTask[GID, TID])
	if !ok {
		err := fmt.Errorf("[SUBMIT] data does not implement DependentTask interface: %#v", data)
		if p.config.OnTaskFailed != nil {
			p.config.OnTaskFailed(data, err)
		}
		return err
	}

	groupID := dtask.GetGroupID()
	taskID := dtask.GetTaskID()
	dependencies := dtask.GetDependencies()

	p.mu.Lock()
	group, gexists := p.groups[groupID]
	if !gexists {
		fmt.Printf("[SUBMIT] Creating new taskGroup for groupID=%#v\n", groupID)

		groupPool, err := p.createGroupPool(groupID)
		if err != nil {
			p.mu.Unlock()
			return fmt.Errorf("[SUBMIT] failed to create group pool: %w", err)
		}

		group = &blockingTaskGroup[T, GID, TID]{
			id:        groupID,
			tasks:     make(map[TID]*blockingTaskState[T, GID, TID]),
			completed: make(map[TID]bool),
			pool:      groupPool,
		}

		groupPool.SetOnTaskSuccess(func(data T) {
			p.handleTaskCompletion(groupID, data)
		})

		p.groups[groupID] = group
		if p.config.OnGroupCreated != nil {
			p.config.OnGroupCreated(groupID)
		}
	}
	p.mu.Unlock()

	task := &blockingTaskState[T, GID, TID]{
		task:         data,
		taskID:       taskID,
		groupID:      groupID,
		completionCh: make(chan struct{}),
	}

	group.mu.Lock()
	fmt.Printf("[SUBMIT] groupID=%#v: adding taskID=%#v to group...\n", groupID, taskID)
	group.tasks[taskID] = task

	// Check if we can execute now
	canExecute := true
	for _, depID := range dependencies {
		if !group.completed[depID] {
			canExecute = false
			break
		}
	}

	// Calculate needed workers while still holding lock
	var activeTaskCount int
	if canExecute {
		for _, t := range group.tasks {
			if !t.completed {
				activeTaskCount++
			}
		}
	}
	group.mu.Unlock()

	if !canExecute {
		fmt.Printf("[SUBMIT] taskID=%#v waiting for dependencies\n", taskID)
		return nil
	}

	// Scale workers if needed
	workers, _ := group.pool.Workers()
	workerCount := len(workers)

	fmt.Printf("[SCALE] active tasks: %d, current workers: %d\n", activeTaskCount, workerCount)

	if workerCount < activeTaskCount {
		needed := activeTaskCount - workerCount
		fmt.Printf("[SCALE] Adding %d workers\n", needed)

		for i := 0; i < needed; i++ {
			worker := p.workerFactory()
			if err := group.pool.Add(worker, nil); err != nil {
				if p.config.OnTaskFailed != nil {
					p.config.OnTaskFailed(data, err)
				}
				return fmt.Errorf("[SCALE] failed to add worker: %w", err)
			}
			if p.config.OnWorkerAdded != nil {
				p.config.OnWorkerAdded(workerCount + i + 1)
			}
		}
	}

	if p.config.OnTaskStarted != nil {
		p.config.OnTaskStarted(data)
	}

	fmt.Printf("[SUBMIT] Submitting taskID=%#v to group pool...\n", taskID)
	err := group.pool.SubmitToFreeWorker(task.task)
	if err != nil {
		fmt.Printf("[SUBMIT] Group pool Submit returned error: %v\n", err)
		if p.config.OnTaskFailed != nil {
			p.config.OnTaskFailed(data, err)
		}
	} else {
		fmt.Printf("[SUBMIT] Successfully submitted taskID=%#v\n", taskID)
	}
	return err
}

// handleTaskCompletion handles task completion
func (p *BlockingPool[T, GID, TID]) handleTaskCompletion(groupID GID, data T) {
	if p.config.OnTaskCompleted != nil {
		p.config.OnTaskCompleted(data)
	}

	dtask, _ := any(data).(DependentTask[GID, TID])
	taskID := dtask.GetTaskID()

	fmt.Printf("[COMPLETE] Task complete for groupID=%#v, taskID=%#v\n", groupID, taskID)

	p.mu.RLock()
	group, exists := p.groups[groupID]
	if !exists {
		fmt.Printf("[COMPLETE] groupID=%#v not found in p.groups\n", groupID)
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()

	var tasksToSubmit []T
	var activeTaskCount int

	group.mu.Lock()
	st := group.tasks[taskID]
	if st == nil {
		fmt.Printf("[COMPLETE] groupID=%#v, taskID=%#v => no matching task\n", groupID, taskID)
		group.mu.Unlock()
		return
	}

	// Mark as completed
	if !st.completed {
		st.completed = true
		close(st.completionCh)
		group.completed[taskID] = true
	}

	// Find ready tasks and count active ones
	for _, t := range group.tasks {
		if t.completed {
			continue
		}
		activeTaskCount++

		depTask, ok := any(t.task).(DependentTask[GID, TID])
		if !ok {
			continue
		}

		allDependenciesMet := true
		for _, depID := range depTask.GetDependencies() {
			if !group.completed[depID] {
				allDependenciesMet = false
				break
			}
		}

		if allDependenciesMet {
			tasksToSubmit = append(tasksToSubmit, t.task)
		}
	}

	group.mu.Unlock()

	// Scale workers if needed
	workers, _ := group.pool.Workers()
	workerCount := len(workers)

	if workerCount < activeTaskCount {
		needed := activeTaskCount - workerCount
		fmt.Printf("[SCALE] Adding %d workers\n", needed)

		for i := 0; i < needed; i++ {
			worker := p.workerFactory()
			if err := group.pool.Add(worker, nil); err != nil {
				fmt.Printf("[COMPLETE] Error adding worker: %v\n", err)
				continue
			}
			if p.config.OnWorkerAdded != nil {
				p.config.OnWorkerAdded(workerCount + i + 1)
			}
		}
	}

	// Submit ready tasks
	for _, t := range tasksToSubmit {
		if p.config.OnTaskStarted != nil {
			p.config.OnTaskStarted(t)
		}

		if err := group.pool.SubmitToFreeWorker(t); err != nil {
			fmt.Printf("[COMPLETE] Error submitting task: %v\n", err)
		}
	}

	fmt.Printf("[COMPLETE] Marked groupID=%#v, taskID=%#v as complete\n", groupID, taskID)
}

// WaitForTask blocks until the specified task finishes or the pool is closed.
func (p *BlockingPool[T, GID, TID]) WaitForTask(groupID GID, taskID TID) error {
	fmt.Printf("[WAIT] WaitForTask(%#v, %#v) called\n", groupID, taskID)

	p.mu.RLock()
	group, exists := p.groups[groupID]
	if !exists {
		p.mu.RUnlock()
		msg := fmt.Sprintf("[WAIT] group %v not found in WaitForTask", groupID)
		fmt.Println(msg)
		return fmt.Errorf(msg)
	}

	group.mu.RLock()
	task, texists := group.tasks[taskID]
	if !texists {
		group.mu.RUnlock()
		p.mu.RUnlock()
		msg := fmt.Sprintf("[WAIT] task %v not found in group %v", taskID, groupID)
		fmt.Println(msg)
		return fmt.Errorf(msg)
	}
	completionCh := task.completionCh
	group.mu.RUnlock()
	p.mu.RUnlock()

	fmt.Printf("[WAIT] Blocking on completionCh for groupID=%#v, taskID=%#v...\n", groupID, taskID)
	select {
	case <-completionCh:
		fmt.Printf("[WAIT] unblocked => task completed for groupID=%#v, taskID=%#v\n", groupID, taskID)
		return nil
	case <-p.ctx.Done():
		fmt.Printf("[WAIT] blocking context canceled => returning p.ctx.Err()\n")
		return p.ctx.Err()
	}
}

// WaitWithCallback waits for all groups to complete while calling a callback function
func (p *BlockingPool[T, GID, TID]) WaitWithCallback(
	ctx context.Context,
	callback func(queueSize, processingCount, deadTaskCount int) bool,
	interval time.Duration,
) error {
	fmt.Println("[WAITCB] WaitWithCallback called")
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("[WAITCB] Context done, returning error")
			return ctx.Err()
		case <-ticker.C:
			totalQueued := 0
			totalProcessing := 0
			totalDeadTasks := 0

			// Take a snapshot of the current groups to avoid holding the lock
			p.mu.RLock()
			groupsCopy := make([]*blockingTaskGroup[T, GID, TID], 0, len(p.groups))
			for _, group := range p.groups {
				groupsCopy = append(groupsCopy, group)
			}
			p.mu.RUnlock()

			// Process metrics for each group
			for _, group := range groupsCopy {
				metrics := group.pool.GetMetricsSnapshot()
				totalQueued += int(metrics.TasksSubmitted - metrics.TasksProcessed)
				totalProcessing += int(group.pool.ProcessingCount())
				totalDeadTasks += int(metrics.DeadTasks)
			}

			if !callback(totalQueued, totalProcessing, totalDeadTasks) {
				fmt.Println("[WAITCB] Callback returned false, ending wait")
				return nil
			}
		}
	}
}

// Close gracefully shuts down all group pools
func (p *BlockingPool[T, GID, TID]) Close() error {
	if p.config.OnPoolClosed != nil {
		p.config.OnPoolClosed()
	}
	fmt.Println("[CLOSE] Canceling BlockingPool context, then closing group pools.")

	p.mu.Lock()
	defer p.mu.Unlock()

	p.cancel()

	// Close all group pools
	for groupID, group := range p.groups {
		if err := group.pool.Close(); err != nil {
			fmt.Printf("[CLOSE] Error closing pool for group %v: %v\n", groupID, err)
		}
	}

	return nil
}

// safeRemoveWorkers safely scales down a group's worker pool
func (p *BlockingPool[T, GID, TID]) safeRemoveWorkers(group *blockingTaskGroup[T, GID, TID]) {
	if workers, err := group.pool.Workers(); err == nil {
		for _, workerID := range workers[p.config.minWorkers:] {
			if p.config.OnWorkerRemoved != nil {
				p.config.OnWorkerRemoved(workerID)
			}
			group.pool.Remove(workerID)
		}
	}
}
