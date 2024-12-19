package retrypool

import (
	"context"
	"fmt"
	"time"

	"github.com/sasha-s/go-deadlock"
)

// BlockingConfig holds basic config—mainly the worker factory and min/max worker count.
type BlockingConfig[T any] struct {
	workerFactory WorkerFactory[T]
	minWorkers    int
	maxWorkers    int
}

// BlockingPoolOption is a functional option for configuring the blocking pool
type BlockingPoolOption[T any] func(*BlockingConfig[T])

// WithBlockingWorkerFactory sets the worker factory for creating new workers
func WithBlockingWorkerFactory[T any](factory WorkerFactory[T]) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		// fmt.Printf("[CONFIG] Setting worker factory\n")
		c.workerFactory = factory
	}
}

// WithBlockingWorkerLimits specifies how many workers to keep (min) and the absolute limit (max)
func WithBlockingWorkerLimits[T any](min, max int) BlockingPoolOption[T] {
	return func(c *BlockingConfig[T]) {
		// fmt.Printf("[CONFIG] Setting worker limits: min=%d, max=%d\n", min, max)
		if min < 1 {
			// fmt.Printf("[CONFIG] min was < 1 => setting min=1\n")
			min = 1
		}
		c.minWorkers = min
		// Ensure max ≥ min
		if max < min {
			// fmt.Printf("[CONFIG] max < min => setting max=min=%d\n", min)
			max = min
		}
		c.maxWorkers = max
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
}

// BlockingPool is our high-level pool wrapper that delegates work to the internal retrypool
type BlockingPool[T any, GID comparable, TID comparable] struct {
	mu            deadlock.RWMutex
	pooler        Pooler[T]
	workerFactory WorkerFactory[T]
	groups        map[GID]*blockingTaskGroup[T, GID, TID]
	ctx           context.Context
	cancel        context.CancelFunc
	config        BlockingConfig[T]
}

// NewBlockingPool constructs a BlockingPool with the given options. It also creates
// the minimum required number of workers in the underlying pool.
func NewBlockingPool[T any, GID comparable, TID comparable](
	ctx context.Context,
	opt ...BlockingPoolOption[T],
) (*BlockingPool[T, GID, TID], error) {
	// fmt.Printf("[INIT] Creating new BlockingPool...\n")

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

	// fmt.Printf("[INIT] minWorkers=%d, maxWorkers=%d\n", cfg.minWorkers, cfg.maxWorkers)

	// Create the initial set of workers = minWorkers
	workers := make([]Worker[T], 0, cfg.minWorkers)
	for i := 0; i < cfg.minWorkers; i++ {
		// fmt.Printf("[INIT] Creating initial worker #%d\n", i+1)
		workers = append(workers, cfg.workerFactory())
	}

	pooler := New[T](ctx, workers)

	ctx, cancel := context.WithCancel(ctx)
	pool := &BlockingPool[T, GID, TID]{
		pooler:        pooler,
		workerFactory: cfg.workerFactory,
		groups:        make(map[GID]*blockingTaskGroup[T, GID, TID]),
		config:        cfg,
		ctx:           ctx,
		cancel:        cancel,
	}

	// fmt.Printf("[INIT] Finished creating BlockingPool. Setting OnTaskSuccess handler.\n")
	// Whenever a task succeeds, mark it complete in our metadata
	pooler.SetOnTaskSuccess(pool.handleTaskCompletion)
	return pool, nil
}

// Submit enqueues the given data (which must implement DependentTask[GID,TID]).
// We also attempt to scale up the worker count if needed to avoid deadlocks when tasks spawn children.
func (p *BlockingPool[T, GID, TID]) Submit(data T) error {
	// fmt.Printf("[SUBMIT] Received Submit() for data=%#v\n", data)

	dtask, ok := any(data).(DependentTask[GID, TID])
	if !ok {
		return fmt.Errorf("[SUBMIT] data does not implement DependentTask interface: %#v", data)
	}

	// Set the Pool field if the task implements a SetPool method
	if setter, ok := any(data).(interface {
		SetPool(*BlockingPool[T, GID, TID])
	}); ok {
		setter.SetPool(p)
	}

	p.mu.Lock()
	groupID := dtask.GetGroupID()
	taskID := dtask.GetTaskID()
	// fmt.Printf("[SUBMIT] GroupID=%#v, TaskID=%#v\n", groupID, taskID)

	group, gexists := p.groups[groupID]
	if !gexists {
		// fmt.Printf("[SUBMIT] Creating new taskGroup for groupID=%#v\n", groupID)
		group = &blockingTaskGroup[T, GID, TID]{
			id:        groupID,
			tasks:     make(map[TID]*blockingTaskState[T, GID, TID]),
			completed: make(map[TID]bool),
		}
		p.groups[groupID] = group
	}
	p.mu.Unlock()

	task := &blockingTaskState[T, GID, TID]{
		task:         data,
		taskID:       taskID,
		groupID:      groupID,
		completionCh: make(chan struct{}),
	}

	group.mu.Lock()
	// fmt.Printf("[SUBMIT] groupID=%#v: adding taskID=%#v to group...\n", groupID, taskID)
	group.tasks[taskID] = task
	group.mu.Unlock()

	// Attempt to scale up if we're short on workers:
	if err := p.scaleWorkersIfNeeded(); err != nil {
		return fmt.Errorf("[SUBMIT] failed scaling workers: %w", err)
	}

	// fmt.Printf("[SUBMIT] Submitting taskID=%#v to underlying pool...\n", taskID)
	err := p.pooler.SubmitToFreeWorker(task.task, WithBounceRetry[T]())
	if err != nil {
		// fmt.Printf("[SUBMIT] Underlying pooler.Submit returned error: %v\n", err)
	} else {
		// fmt.Printf("[SUBMIT] Successfully submitted taskID=%#v\n", taskID)
	}
	return err
}

// handleTaskCompletion is called by the pool when a task finishes successfully.
// We mark its local completionCh as closed so WaitForTask can unblock.
func (p *BlockingPool[T, GID, TID]) handleTaskCompletion(data T) {
	dtask, _ := any(data).(DependentTask[GID, TID])
	groupID := dtask.GetGroupID()
	taskID := dtask.GetTaskID()

	// fmt.Printf("[COMPLETE] Task complete for groupID=%#v, taskID=%#v\n", groupID, taskID)

	p.mu.RLock()
	group, exists := p.groups[groupID]
	if !exists {
		// fmt.Printf("[COMPLETE] groupID=%#v not found in p.groups (possibly removed?)\n", groupID)
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()

	group.mu.Lock()
	defer group.mu.Unlock()

	st := group.tasks[taskID]
	if st == nil {
		// fmt.Printf("[COMPLETE] groupID=%#v, taskID=%#v => no matching task in group map\n", groupID, taskID)
		return
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	st.completed = true
	close(st.completionCh)
	group.completed[taskID] = true

	// fmt.Printf("[COMPLETE] Marked groupID=%#v, taskID=%#v as complete. completionCh closed.\n", groupID, taskID)
}

// WaitForTask blocks until the specified task finishes or the pool is closed.
func (p *BlockingPool[T, GID, TID]) WaitForTask(groupID GID, taskID TID) error {
	// fmt.Printf("[WAIT] WaitForTask(%#v, %#v) called\n", groupID, taskID)

	p.mu.RLock()
	group, exists := p.groups[groupID]
	if !exists {
		p.mu.RUnlock()
		msg := fmt.Sprintf("[WAIT] group %v not found in WaitForTask", groupID)
		// fmt.Println(msg)
		return fmt.Errorf(msg)
	}

	group.mu.RLock()
	task, texists := group.tasks[taskID]
	if !texists {
		group.mu.RUnlock()
		p.mu.RUnlock()
		msg := fmt.Sprintf("[WAIT] task %v not found in group %v", taskID, groupID)
		// fmt.Println(msg)
		return fmt.Errorf(msg)
	}
	completionCh := task.completionCh
	group.mu.RUnlock()
	p.mu.RUnlock()

	// fmt.Printf("[WAIT] Blocking on completionCh for groupID=%#v, taskID=%#v...\n", groupID, taskID)
	select {
	case <-completionCh:
		// fmt.Printf("[WAIT] unblocked => task completed for groupID=%#v, taskID=%#v\n", groupID, taskID)
		return nil
	case <-p.ctx.Done():
		// fmt.Printf("[WAIT] blocking context canceled => returning p.ctx.Err()\n")
		return p.ctx.Err()
	}
}

// WaitWithCallback just defers to the underlying pool’s WaitWithCallback
func (p *BlockingPool[T, GID, TID]) WaitWithCallback(
	ctx context.Context,
	callback func(queueSize, processingCount, deadTaskCount int) bool,
	interval time.Duration,
) error {
	// fmt.Println("[WAITCB] WaitWithCallback called, deferring to underlying pooler.")
	return p.pooler.WaitWithCallback(ctx, callback, interval)
}

// Close cancels our context and closes the underlying pool
func (p *BlockingPool[T, GID, TID]) Close() error {
	// fmt.Println("[CLOSE] Canceling BlockingPool context, then closing underlying pooler.")
	p.cancel()
	return p.pooler.Close()
}

// scaleWorkersIfNeeded checks the total tasks in queue + tasks in processing
// and ensures we have at least that many workers (plus 1 if any tasks are processing),
// capped by p.config.maxWorkers. This way, if we have a chain of blocking tasks
// (each waiting on its child), there is always at least one extra worker free
// for the newly spawned child.
func (p *BlockingPool[T, GID, TID]) scaleWorkersIfNeeded() error {
	// fmt.Println("[SCALE] Checking if we need to scale up workers...")

	// Count how many tasks are queued across all workers
	var queued int64
	p.pooler.RangeWorkerQueues(func(wid int, qs int64) bool {
		// fmt.Printf("[SCALE] worker=%d => queueSize=%d\n", wid, qs)
		queued += qs
		return true
	})

	// Count how many tasks are currently processing
	processing := p.pooler.ProcessingCount()
	// fmt.Printf("[SCALE] total queueSize=%d, currently processing=%d\n", queued, processing)

	// We need enough workers to handle currently processing tasks
	// plus any queued tasks, plus 1 extra if there’s at least one task processing.
	// That “+1” ensures the newly spawned child isn’t starved by its blocked parent.
	desired := int(queued + processing)
	if processing > 0 {
		desired++
	}
	// fmt.Printf("[SCALE] raw desired count (queued + processing + optional extra)=%d\n", desired)

	// Enforce min/max
	if desired < p.config.minWorkers {
		// fmt.Printf("[SCALE] desired < minWorkers => setting desired=%d\n", p.config.minWorkers)
		desired = p.config.minWorkers
	}
	if desired > p.config.maxWorkers {
		// fmt.Printf("[SCALE] desired > maxWorkers => setting desired=%d\n", p.config.maxWorkers)
		desired = p.config.maxWorkers
	}

	// Compare to current worker count
	cur := p.pooler.GetFreeWorkers()

	currentCount := len(cur)
	// fmt.Printf("[SCALE] current worker count=%d, desired count=%d\n", currentCount, desired)

	// Scale up if needed
	if currentCount < desired {
		toAdd := desired - currentCount
		// fmt.Printf("[SCALE] Need to add %d worker(s)\n", toAdd)
		for i := 0; i < toAdd; i++ {
			w := p.workerFactory()
			// fmt.Printf("[SCALE] Adding worker #%d (plus existing %d => total %d)\n",
			// i+1, currentCount, currentCount+i+1)
			if e := p.pooler.Add(w, nil); e != nil {
				// fmt.Printf("[SCALE] error adding worker: %v\n", e)
				return e
			}
		}
	} else {
		// fmt.Println("[SCALE] current worker count is already >= desired => no scaling needed.")
	}

	return nil
}
