package retrypool

import (
	"context"
	"fmt"
	"time"

	"github.com/sasha-s/go-deadlock"
)

/// Independent Pool Design and Mechanisms
///
/// Core Concepts:
/// - Tasks can depend on other tasks within the same group
/// - Tasks are executed in parallel when dependencies are met
/// - Multiple groups can execute concurrently
/// - Each group represents an independent set of tasks
///
/// Task Groups:
/// 1. Task Group Structure
///    - Group is a set of tasks with unique IDs
///    - Tasks can depend on other tasks in the same group
///    - Groups are isolated - tasks can't depend on tasks in other groups
///    - Groups are submitted as complete units to validate dependencies
///
/// 2. Group Management:
///    - Groups are validated on submission (cycle detection, missing deps)
///    - Each group maintains its own execution state and completion tracking
///    - Groups can execute concurrently with other groups
///    - Group completion requires all tasks to complete
///
/// Task Execution:
/// 1. Submission Phase:
///    - Tasks are submitted as part of a group
///    - Dependencies are validated
///    - Topological sort determines execution order
///    - Root tasks (no dependencies) start immediately
///
/// 2. Execution Order:
///    - Root tasks start first
///    - Tasks start when all dependencies complete
///    - Multiple tasks can run in parallel within a group
///    - Different groups execute independently
///
/// State Management:
/// - Groups track completed/pending tasks
/// - Tasks track execution state and dependencies
/// - Workers scale based on pending work
/// - Groups are removed when all tasks complete
///
/// Example Flow:
/// 1. Submit Group A: [A1 -> A2 -> A3]
///    - A1 starts immediately (no deps)
///    - A2 waits for A1
///    - A3 waits for A2
///
/// 2. Submit Group B: [B1, B2 -> B3]
///    - B1 and B2 start immediately (no deps)
///    - B3 waits for B2
///    - Group B executes concurrently with Group A
///
/// 3. Task Completion:
///    - When task completes, check dependents
///    - Start dependent tasks when dependencies met
///    - Group completes when all tasks done
///
/// Error Handling:
/// - Invalid dependencies rejected on submission
/// - Failed tasks can retry based on policy
/// - Group state tracks failed tasks
/// - Groups can fail if tasks can't complete
/// - When the group fail, the task that failed goes into the deadtasks so please clean it up yourself
/// - Use the OnGroupRemoved callback to know when a group is removed and get the list of tasks
///
/// Thread Safety:
/// - Groups isolated from each other
/// - Task state protected by locks
/// - Worker pool handles concurrency
/// - Safe for concurrent group submission

/// After thinking submitting tasks individually with complex dependency trees could lead to:
/// - Deadlocks if circular dependencies exist
/// - Unresolvable pending tasks if dependencies are never submitted
/// - Nondeterministic execution order between sibling dependencies
/// - Memory leaks from orphaned tasks in the pending map
///
/// The safer approach would be:
/// - Submit entire task groups at once
/// - Validate the entire dependency tree before accepting any tasks
/// - Build a complete execution plan
/// - Reject additional tasks for groups already being processed

// DependencyGraph represents the complete dependency structure for a task group
type DependencyGraph[TID comparable] struct {
	Nodes map[TID]*Node[TID]
	Order []TID // Topologically sorted execution order
}

type Node[TID comparable] struct {
	ID           TID
	Dependencies []TID
	Dependents   []TID
	Visited      bool
	InProgress   bool // Used for cycle detection
}

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
	mu           deadlock.RWMutex
	id           GID
	tasks        map[TID]*independentTaskState[T, GID, TID]
	completed    map[TID]bool
	pending      map[TID]*independentTaskState[T, GID, TID]
	graph        *DependencyGraph[TID]
	executedTask map[TID]bool
}

type IndependentPool[T any, GID comparable, TID comparable] struct {
	mu     deadlock.RWMutex
	pooler Pooler[T]
	groups map[GID]*independentTaskGroup[T, GID, TID]
	ctx    context.Context
	cancel context.CancelFunc
	config IndependentConfig[T]
}

func (i *IndependentPool[T, GID, TID]) PullDeadTask(id int) (*DeadTask[T], error) {
	return i.pooler.PullDeadTask(id)
}

type IndependentConfig[T any] struct {
	workerFactory WorkerFactory[T]
	minWorkers    int
	maxWorkers    int
	// mode
	dependencyMode DependencyMode
	// Task callbacks
	OnTaskSubmitted func(task T)
	OnTaskStarted   func(task T)
	OnTaskCompleted func(task T)
	OnTaskFailed    func(task T, err error)
	// Group callbacks
	OnGroupCreated   func(groupID any)
	OnGroupCompleted func(groupID any)
	OnGroupRemoved   func(groupID any, tasks []T)
	// Pool events
	OnWorkerAdded   func(workerID int)
	OnWorkerRemoved func(workerID int)
	OnPoolClosed    func()
	//
	options []Option[T]
}

type IndependentPoolOption[T any] func(*IndependentConfig[T])

func WithIndependentOnDeadTask[T any](handler func(deadTaskIndex int)) IndependentPoolOption[T] {
	return func(c *IndependentConfig[T]) {
		c.options = append(c.options, WithOnDeadTask[T](handler))
	}
}

func WithIndependentWorkerFactory[T any](factory WorkerFactory[T]) IndependentPoolOption[T] {
	return func(c *IndependentConfig[T]) {
		c.workerFactory = factory
	}
}

func WithIndependentDependencyMode[T any](mode DependencyMode) IndependentPoolOption[T] {
	return func(c *IndependentConfig[T]) {
		c.dependencyMode = mode
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

func WithIndependentOnGroupRemoved[T any](cb func(groupID any, tasks []T)) IndependentPoolOption[T] {
	return func(c *IndependentConfig[T]) {
		c.OnGroupRemoved = cb
	}
}

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
	cfg := IndependentConfig[T]{
		minWorkers:     1,
		dependencyMode: ForwardMode,
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

	cfg.options = append(cfg.options, WithAttempts[T](1))

	pool := &IndependentPool[T, GID, TID]{
		pooler: New[T](ctx, []Worker[T]{worker}, cfg.options...),
		groups: make(map[GID]*independentTaskGroup[T, GID, TID]),
		config: cfg,
		ctx:    ctx,
		cancel: cancel,
	}

	pool.pooler.SetOnTaskSuccess(pool.handleTaskCompletion)
	pool.pooler.SetOnTaskFailure(func(data T, metadata Metadata, err error) TaskAction {
		if pool.config.OnTaskFailed != nil {
			pool.config.OnTaskFailed(data, err)
		}

		if dt, ok := any(data).(IndependentDependentTask[GID, TID]); ok {
			groupID := dt.GetGroupID()

			// Clean up group data
			pool.mu.Lock()
			if group, exists := pool.groups[groupID]; exists {
				// Call removal callback if set
				if pool.config.OnGroupRemoved != nil {
					tasks := make([]T, 0, len(group.tasks))
					for _, task := range group.tasks {
						tasks = append(tasks, task.task)
					}
					pool.config.OnGroupRemoved(groupID, tasks)
				}
				// Delete group from memory
				delete(pool.groups, groupID)
			}
			pool.mu.Unlock()
		}
		return TaskActionAddToDeadTasks
	})

	return pool, nil
}

// Submit submits a complete group of tasks with dependencies
func (p *IndependentPool[T, GID, TID]) Submit(tasks []T) error {
	if len(tasks) == 0 {
		return fmt.Errorf("empty task group")
	}

	// Extract group ID from first task
	dtask, ok := any(tasks[0]).(IndependentDependentTask[GID, TID])
	if !ok {
		return fmt.Errorf("tasks must implement DependentTask interface")
	}
	groupID := dtask.GetGroupID()

	p.mu.Lock()
	if _, exists := p.groups[groupID]; exists {
		p.mu.Unlock()
		return fmt.Errorf("group %v already exists", groupID)
	}

	// Build and validate dependency graph
	graph, err := p.buildDependencyGraph(tasks)
	if err != nil {
		p.mu.Unlock()
		return fmt.Errorf("invalid dependency graph: %w", err)
	}

	// Create new group
	group := &independentTaskGroup[T, GID, TID]{
		id:           groupID,
		tasks:        make(map[TID]*independentTaskState[T, GID, TID]),
		completed:    make(map[TID]bool),
		pending:      make(map[TID]*independentTaskState[T, GID, TID]),
		graph:        graph,
		executedTask: make(map[TID]bool),
	}

	// Initialize all tasks in the group
	for _, task := range tasks {
		dtask := any(task).(IndependentDependentTask[GID, TID])
		taskID := dtask.GetTaskID()

		taskState := &independentTaskState[T, GID, TID]{
			task:         task,
			taskID:       taskID,
			groupID:      groupID,
			dependencies: dtask.GetDependencies(),
			completionCh: make(chan struct{}),
		}
		group.tasks[taskID] = taskState
	}

	p.groups[groupID] = group
	if p.config.OnGroupCreated != nil {
		p.config.OnGroupCreated(groupID)
	}

	// Submit initial tasks (those with no dependencies)
	for _, taskID := range graph.Order {
		node := graph.Nodes[taskID]
		if len(node.Dependencies) == 0 {
			task := group.tasks[taskID]
			if err := p.submitTask(task); err != nil {
				p.mu.Unlock()
				return fmt.Errorf("failed to submit initial task %v: %w", taskID, err)
			}
		}
	}

	p.mu.Unlock()
	return nil
}

func (p *IndependentPool[T, GID, TID]) buildDependencyGraph(tasks []T) (*DependencyGraph[TID], error) {
	graph := &DependencyGraph[TID]{
		Nodes: make(map[TID]*Node[TID]),
	}

	// First pass: Create nodes and validate task IDs are unique
	for _, task := range tasks {
		dtask := any(task).(IndependentDependentTask[GID, TID])
		taskID := dtask.GetTaskID()

		if _, exists := graph.Nodes[taskID]; exists {
			return nil, fmt.Errorf("duplicate task ID: %v", taskID)
		}

		// In reverse mode, we swap dependencies and dependents
		var deps []TID
		if p.config.dependencyMode == ReverseMode {
			// In reverse mode, if A depends on B, then B should wait for A
			// So we don't set dependencies here
			deps = []TID{}
		} else {
			deps = dtask.GetDependencies()
		}

		graph.Nodes[taskID] = &Node[TID]{
			ID:           taskID,
			Dependencies: deps,
		}
	}

	// Second pass: Build dependency links
	for _, task := range tasks {
		dtask := any(task).(IndependentDependentTask[GID, TID])
		taskID := dtask.GetTaskID()
		declaredDeps := dtask.GetDependencies()

		for _, depID := range declaredDeps {
			depNode, exists := graph.Nodes[depID]
			if !exists {
				return nil, fmt.Errorf("dependency %v not found for task %v", depID, taskID)
			}

			if p.config.dependencyMode == ForwardMode {
				// Forward mode: task depends on depID
				depNode.Dependents = append(depNode.Dependents, taskID)
			} else {
				// Reverse mode: depID depends on taskID
				graph.Nodes[depID].Dependencies = append(graph.Nodes[depID].Dependencies, taskID)
				graph.Nodes[taskID].Dependents = append(graph.Nodes[taskID].Dependents, depID)
			}
		}
	}

	// Perform topological sort
	sorted, err := p.topologicalSort(graph)
	if err != nil {
		return nil, err
	}
	graph.Order = sorted

	return graph, nil
}

func (p *IndependentPool[T, GID, TID]) topologicalSort(graph *DependencyGraph[TID]) ([]TID, error) {
	var order []TID
	visited := make(map[TID]bool)
	inProgress := make(map[TID]bool)

	var visit func(TID) error
	visit = func(id TID) error {
		if inProgress[id] {
			return fmt.Errorf("cycle detected at task %v", id)
		}
		if visited[id] {
			return nil
		}

		inProgress[id] = true
		node := graph.Nodes[id]

		for _, depID := range node.Dependencies {
			if err := visit(depID); err != nil {
				return err
			}
		}

		delete(inProgress, id)
		visited[id] = true
		order = append(order, id)
		return nil
	}

	for id := range graph.Nodes {
		if !visited[id] {
			if err := visit(id); err != nil {
				return nil, err
			}
		}
	}

	// Reverse the order to get correct dependency ordering
	for i := 0; i < len(order)/2; i++ {
		order[i], order[len(order)-1-i] = order[len(order)-1-i], order[i]
	}

	return order, nil
}

func (p *IndependentPool[T, GID, TID]) handleTaskCompletion(data T, metadata Metadata) {
	if p.config.OnTaskCompleted != nil {
		p.config.OnTaskCompleted(data)
	}

	dtask := any(data).(IndependentDependentTask[GID, TID])
	groupID := dtask.GetGroupID()
	taskID := dtask.GetTaskID()

	p.mu.RLock()
	group, exists := p.groups[groupID]
	if !exists {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()

	group.mu.Lock()
	defer group.mu.Unlock()

	task := group.tasks[taskID]
	if task == nil {
		return
	}

	// Mark task as completed
	task.mu.Lock()
	task.completed = true
	close(task.completionCh)
	task.mu.Unlock()
	group.completed[taskID] = true
	group.executedTask[taskID] = true

	// Find and submit tasks whose dependencies are now met
	node := group.graph.Nodes[taskID]
	for _, depID := range node.Dependents {
		depNode := group.graph.Nodes[depID]
		allDepsComplete := true
		for _, parentID := range depNode.Dependencies {
			if !group.completed[parentID] {
				allDepsComplete = false
				break
			}
		}
		if allDepsComplete {
			depTask := group.tasks[depID]
			if err := p.submitTask(depTask); err != nil {
				if p.config.OnTaskFailed != nil {
					p.config.OnTaskFailed(depTask.task, err)
				}
			}
		}
	}

	// Check if group is completed
	allCompleted := true
	for _, t := range group.tasks {
		if !t.completed {
			allCompleted = false
			break
		}
	}

	if allCompleted {
		// Trigger completion callback
		if p.config.OnGroupCompleted != nil {
			p.config.OnGroupCompleted(groupID)
		}

		// Clean up the group's internal data first
		for k := range group.tasks {
			delete(group.tasks, k)
		}
		for k := range group.completed {
			delete(group.completed, k)
		}
		for k := range group.pending {
			delete(group.pending, k)
		}
		for k := range group.executedTask {
			delete(group.executedTask, k)
		}
		group.graph = nil

		// Call removal callback while group still exists in p.groups
		if p.config.OnGroupRemoved != nil {
			tasks := make([]T, 0, len(group.tasks))
			for _, task := range group.tasks {
				tasks = append(tasks, task.task)
			}
			p.config.OnGroupRemoved(groupID, tasks)
		}

		// Finally remove the group from p.groups
		group.mu.Unlock()
		p.mu.Lock()
		delete(p.groups, groupID)
		p.mu.Unlock()
		group.mu.Lock() // Reacquire for deferred unlock
	}
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

	if p.config.OnTaskStarted != nil {
		p.config.OnTaskStarted(task.task)
	}

	task.submitted = true
	task.mu.Unlock()

	err := p.pooler.Submit(task.task) // after careful reviewing, i remembered that we don't need to execute on a free worker because you know... we already have all the tasks sorted out
	if err != nil && p.config.OnTaskFailed != nil {
		p.config.OnTaskFailed(task.task, err)
	}
	return err
}

// WaitWithCallback waits for the pool to complete while calling a callback function
func (p *IndependentPool[T, GID, TID]) WaitWithCallback(
	ctx context.Context,
	callback func(queueSize, processingCount, deadTaskCount int) bool,
	interval time.Duration,
) error {
	return p.pooler.WaitWithCallback(ctx, callback, interval)
}

// Close gracefully shuts down the pool
func (p *IndependentPool[T, GID, TID]) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.config.OnPoolClosed != nil {
		p.config.OnPoolClosed()
	}
	p.cancel()
	return p.pooler.Close()
}

// GetGroupStatus returns the status of a task group
func (p *IndependentPool[T, GID, TID]) GetGroupStatus(groupID GID) (completed, total int, err error) {
	p.mu.RLock()
	group, exists := p.groups[groupID]
	if !exists {
		p.mu.RUnlock()
		return 0, 0, fmt.Errorf("group %v not found", groupID)
	}
	p.mu.RUnlock()

	group.mu.RLock()
	defer group.mu.RUnlock()

	total = len(group.tasks)
	completed = len(group.completed)
	return completed, total, nil
}

// WaitForGroup waits for all tasks in a group to complete
func (p *IndependentPool[T, GID, TID]) WaitForGroup(ctx context.Context, groupID GID) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			p.mu.RLock()
			_, exists := p.groups[groupID]
			if !exists {
				// Group not found means it was cleaned up after completion
				p.mu.RUnlock()
				return nil
			}
			p.mu.RUnlock()
			completed, total, err := p.GetGroupStatus(groupID)
			if err != nil {
				return err
			}
			if completed == total {
				return nil
			}
		}
	}
}
