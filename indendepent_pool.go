package retrypool

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/sasha-s/go-deadlock"
)

/// TODO: we need to clean up groups and tasks when they are no longer needed
/// TODO: we need a channel that will be listened to to close a group
/// TODO: to scale and to make it work for real we enforce to submit an array of ALL tasks a group then schedule it
/// TODO: we should refuse a new array of tasks that belong to a group that is already scheduled
/// TODO: on failure, we can provide the pool's options for retry attempts, we will refuse unlimieted attempts, adding to task tasks is a task failed and group fail which result in the group being removed
/// TODO: we should have a soft scalling strategy since we don't have blocking tasks, we just need the group to be scheduled with a correct execution order

type independentTaskState[T any, GID comparable, TID comparable] struct {
	mu           deadlock.RWMutex
	task         T
	taskID       TID
	groupID      GID
	dependencies []TID
	submitted    bool
	completed    bool
	completionCh chan struct{}
}

type independentTaskGroup[T any, GID comparable, TID comparable] struct {
	mu        deadlock.RWMutex
	id        GID
	tasks     map[TID]*independentTaskState[T, GID, TID]
	completed map[TID]bool
	pending   map[TID]*independentTaskState[T, GID, TID]
}

type IndependentPool[T any, GID comparable, TID comparable] struct {
	mu     deadlock.RWMutex
	pooler Pooler[T]
	groups map[GID]*independentTaskGroup[T, GID, TID]
	ctx    context.Context
	cancel context.CancelFunc
	config IndependentConfig[T]
}

type IndependentConfig[T any] struct {
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

type IndependentPoolOption[T any] func(*IndependentConfig[T])

func WithIndependentWorkerFactory[T any](factory WorkerFactory[T]) IndependentPoolOption[T] {
	return func(c *IndependentConfig[T]) {
		c.workerFactory = factory
	}
}

func WithIndependentWorkerLimits[T any](min, max int) IndependentPoolOption[T] {
	return func(c *IndependentConfig[T]) {
		if min < 1 {
			min = 1
		}
		c.minWorkers = min
		c.maxWorkers = max
	}
}

// Callback options
func WithIndependentOnTaskSubmitted[T any](cb func(task T)) IndependentPoolOption[T] {
	return func(c *IndependentConfig[T]) {
		c.OnTaskSubmitted = cb
	}
}

func WithIndependentOnTaskStarted[T any](cb func(task T)) IndependentPoolOption[T] {
	return func(c *IndependentConfig[T]) {
		c.OnTaskStarted = cb
	}
}

func WithIndependentOnTaskCompleted[T any](cb func(task T)) IndependentPoolOption[T] {
	return func(c *IndependentConfig[T]) {
		c.OnTaskCompleted = cb
	}
}

func WithIndependentOnTaskFailed[T any](cb func(task T, err error)) IndependentPoolOption[T] {
	return func(c *IndependentConfig[T]) {
		c.OnTaskFailed = cb
	}
}

func WithIndependentOnGroupCreated[T any](cb func(groupID any)) IndependentPoolOption[T] {
	return func(c *IndependentConfig[T]) {
		c.OnGroupCreated = cb
	}
}

func WithIndependentOnGroupCompleted[T any](cb func(groupID any)) IndependentPoolOption[T] {
	return func(c *IndependentConfig[T]) {
		c.OnGroupCompleted = cb
	}
}

func WithIndependentOnWorkerAdded[T any](cb func(workerID int)) IndependentPoolOption[T] {
	return func(c *IndependentConfig[T]) {
		c.OnWorkerAdded = cb
	}
}

func WithIndependentOnWorkerRemoved[T any](cb func(workerID int)) IndependentPoolOption[T] {
	return func(c *IndependentConfig[T]) {
		c.OnWorkerRemoved = cb
	}
}

func WithIndependentOnPoolClosed[T any](cb func()) IndependentPoolOption[T] {
	return func(c *IndependentConfig[T]) {
		c.OnPoolClosed = cb
	}
}

func NewIndependentPool[T any, GID comparable, TID comparable](
	ctx context.Context,
	opt ...IndependentPoolOption[T],
) (*IndependentPool[T, GID, TID], error) {

	// Initialize with defaults
	cfg := IndependentConfig[T]{
		minWorkers: 1, // At least one worker
	}

	for _, o := range opt {
		o(&cfg)
	}
	if cfg.workerFactory == nil {
		return nil, fmt.Errorf("worker factory must be provided")
	}
	if cfg.minWorkers == 0 {
		cfg.minWorkers = 1
	}

	ctx, cancel := context.WithCancel(ctx)

	worker := cfg.workerFactory()
	if cfg.OnWorkerAdded != nil {
		cfg.OnWorkerAdded(0)
	}

	pool := &IndependentPool[T, GID, TID]{
		pooler: New[T](ctx, []Worker[T]{worker}),
		groups: make(map[GID]*independentTaskGroup[T, GID, TID]),
		config: cfg,
		ctx:    ctx,
		cancel: cancel,
	}

	pool.pooler.SetOnTaskSuccess(pool.handleTaskCompletion)
	pool.pooler.SetOnTaskFailure(func(data T, err error) TaskAction {
		fmt.Println("Task failed:", err)
		if pool.config.OnTaskFailed != nil {
			pool.config.OnTaskFailed(data, err)
		}
		return TaskActionRetry
	})

	return pool, nil
}

func (p *IndependentPool[T, GID, TID]) Submit(data T) error {
	if p.config.OnTaskSubmitted != nil {
		p.config.OnTaskSubmitted(data)
	}

	dtask, ok := any(data).(DependentTask[GID, TID])
	if !ok {
		err := fmt.Errorf("data does not implement DependentTask interface")
		if p.config.OnTaskFailed != nil {
			p.config.OnTaskFailed(data, err)
		}
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	groupID := dtask.GetGroupID()
	taskID := dtask.GetTaskID()
	deps := dtask.GetDependencies()

	log.Printf("DEBUG: Submitting task %v with deps %v", taskID, deps)

	group, ok := p.groups[groupID]
	if !ok {
		group = &independentTaskGroup[T, GID, TID]{
			id:        groupID,
			tasks:     make(map[TID]*independentTaskState[T, GID, TID]),
			completed: make(map[TID]bool),
			pending:   make(map[TID]*independentTaskState[T, GID, TID]),
		}
		p.groups[groupID] = group
		if p.config.OnGroupCreated != nil {
			p.config.OnGroupCreated(groupID)
		}
	}

	task := &independentTaskState[T, GID, TID]{
		task:         data,
		taskID:       taskID,
		groupID:      groupID,
		dependencies: deps,
		completionCh: make(chan struct{}),
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	group.tasks[taskID] = task

	if p.canSubmitTask(group, taskID) {
		log.Printf("DEBUG: Task %v can be submitted immediately", taskID)
		if p.config.OnTaskStarted != nil {
			p.config.OnTaskStarted(data)
		}
		return p.submitTask(task)
	}

	log.Printf("DEBUG: Task %v stored in pending", taskID)
	group.pending[taskID] = task
	return nil
}

func (p *IndependentPool[T, GID, TID]) handleTaskCompletion(data T) {
	if p.config.OnTaskCompleted != nil {
		p.config.OnTaskCompleted(data)
	}

	dtask := any(data).(DependentTask[GID, TID])

	p.mu.RLock()
	group, exists := p.groups[dtask.GetGroupID()]
	if !exists {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()

	groupID := dtask.GetGroupID()
	taskID := dtask.GetTaskID()
	log.Printf("DEBUG: Handling completion of task %v in group %v", taskID, groupID)

	group.mu.Lock()
	defer group.mu.Unlock()

	if task := group.tasks[taskID]; task != nil {
		task.mu.Lock()
		task.completed = true
		close(task.completionCh)
		task.mu.Unlock()
		group.completed[taskID] = true
		log.Printf("DEBUG: Marked task %v as completed", taskID)

		allCompleted := true
		for _, t := range group.tasks {
			if !t.completed {
				allCompleted = false
				break
			}
		}

		if allCompleted && p.config.OnGroupCompleted != nil {
			p.config.OnGroupCompleted(groupID)
		}

		log.Printf("DEBUG: Checking pending tasks, count: %d", len(group.pending))
		for pendingID, pendingTask := range group.pending {
			log.Printf("DEBUG: Checking pending task %v", pendingID)
			if p.canSubmitTask(group, pendingID) {
				log.Printf("DEBUG: Submitting previously pending task %v", pendingID)
				delete(group.pending, pendingID)
				if p.config.OnTaskStarted != nil {
					p.config.OnTaskStarted(pendingTask.task)
				}
				if err := p.submitTask(pendingTask); err != nil {
					log.Printf("ERROR: Failed to submit pending task %v: %v", pendingID, err)
					if p.config.OnTaskFailed != nil {
						p.config.OnTaskFailed(pendingTask.task, err)
					}
				}
			} else {
				log.Printf("DEBUG: Pending task %v still not ready", pendingID)
			}
		}
	}
}

func (p *IndependentPool[T, GID, TID]) canSubmitTask(group *independentTaskGroup[T, GID, TID], taskID TID) bool {
	task := group.tasks[taskID]
	if task == nil {
		return false
	}

	for _, depID := range task.dependencies {
		if !group.completed[depID] {
			return false
		}
	}
	return true
}

func (p *IndependentPool[T, GID, TID]) submitTask(task *independentTaskState[T, GID, TID]) error {
	task.mu.Lock()
	if task.submitted {
		task.mu.Unlock()
		return nil
	}

	// Get current free workers
	freeWorkers := p.pooler.GetFreeWorkers()

	// If no free workers and we can add more...
	if len(freeWorkers) == 0 {
		workers, _ := p.pooler.Workers()
		numWorkers := len(workers)

		// Add new worker if under max (or if no max set)
		if p.config.maxWorkers == 0 || numWorkers < p.config.maxWorkers {
			w := p.config.workerFactory()
			if err := p.pooler.Add(w, nil); err != nil {
				task.mu.Unlock()
				if p.config.OnTaskFailed != nil {
					p.config.OnTaskFailed(task.task, err)
				}
				return fmt.Errorf("failed to add worker: %w", err)
			}
			if p.config.OnWorkerAdded != nil {
				p.config.OnWorkerAdded(numWorkers + 1)
			}
		}
	}

	task.submitted = true
	task.mu.Unlock()

	err := p.pooler.SubmitToFreeWorker(task.task)
	if err != nil && p.config.OnTaskFailed != nil {
		p.config.OnTaskFailed(task.task, err)
	}
	return err
}

func (p *IndependentPool[T, GID, TID]) WaitWithCallback(
	ctx context.Context,
	callback func(queueSize, processingCount, deadTaskCount int) bool,
	interval time.Duration,
) error {
	return p.pooler.WaitWithCallback(ctx, callback, interval)
}

func (p *IndependentPool[T, GID, TID]) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.config.OnPoolClosed != nil {
		p.config.OnPoolClosed()
	}
	p.cancel()
	return p.pooler.Close()
}
