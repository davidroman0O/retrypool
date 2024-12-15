package retrypool

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/sasha-s/go-deadlock"
)

/// DependencyPool: a pool that manage the execution order of tasks that depends on other tasks from same group.
///
/// - The pool is specialized for tasks that have dependencies on other tasks from the same group, tasks must implement the DependentTask interface.
/// - Each DependentTask has a groupID and a list of taskIDs that it depends on.
/// - We can only apply the dependencies on the same groupID.
/// - We must track dynamically the dependencies and the state of each task in the group.
/// - Tasks that have a dependency that we didn't detected or didn't completed yet, will be waiting until the dependencies are met, `OnTaskWaiting` will be called.
/// - By using the QueueNotification option of the Pool when we submit a task, the `OnTaskQueued` will be called.
/// - By using the Processednotification option of the Pool when a task is processed, the `OnTaskProcessed` will be called.
/// - Each time we detect a group that we don't have `OnGroupCreated` callback is called (of course we create a new group).
/// - When a task is sent to the pool, after waiting or because it is allowed to run (met our conditions), `OnTaskRunning` is called.
/// - When a task that depends on other tasks is running and it is the first of it's group, `OnGroupStarted` is called.
/// - When `OnGroupCompletedChan` is NOT set, and if all the tasks that depends on a group are completed, the group is removed from the pool and `OnGroupCompleted` is called.
/// - When `OnGroupCompletedChan` is set, the group is not removed from the pool until the developer is sending a request, when we receive that request we check if the group is completed AND if we can remove it, when we remove it and it is a success we call Complete(nil) otherwise we call Complete(err), if we have an error from the system we call CompleteWithError(err).
/// - We use EqualsTaskID and EqualsGroupID to compare the taskID and groupID, if they are not set we return an error on `NewDependencyPool`
/// - If `AutoCreateWorkers` is set to true, we will create new workers when we lack of workers to process the tasks. It must be used with `WorkerFactory` set.
/// - If MaxDynamicWorkers is not set (0 by default), then we will create workers without any limit, otherwise we will create workers until we reach the limit.
/// - When a task is failling, `OnDependencyFailure` is called.
/// - When a new worker is going to be created, `OnCreateWorker` is called.
///
/// The dependency algorithm must respect many criterea:
/// - If `DependencyStrategyOrder` is set, we will only make sure the execution order of the tasks from the same group is right.
/// - If `DependencyStrategyPriority` is set, we will make sure the execution order of the tasks from the same group is right and we will prioritize the groups that is already started/processing.
///
/// The algorithm must in any cases be smart enough that when it track the state of all tasks, to take in account the queueing of each task on each worker to dispatch them in a way that would prevent any kind of deadlocks.
/// The algorithm must also be able to handle the case where a task is failing and we need to retry it, we must be able to track the state of the task and the dependencies.
///
/// The dependency pool must use the round-robin strategy option to have a deterministic behavior when a task is submitted to the pool. It will helps to track the state of the tasks and the dependencies and in which order they are processed by which worker to prevent any kind of deadlocks.

// TaskDistributionStrategy defines how tasks are distributed across workers
type TaskDistributionStrategy int

const (
	TaskDistributionRoundRobin TaskDistributionStrategy = iota
	TaskDistributionLeastLoaded
)

// DependencyStrategy defines how tasks and groups are prioritized
type DependencyStrategy int

const (
	DependencyStrategyOrder    DependencyStrategy = iota // Execute tasks in dependency order within groups
	DependencyStrategyPriority                           // Prioritize completing started groups before starting new ones
)

// DependentTask represents a task that depends on other tasks
type DependentTask interface {
	GetDependencies() []interface{}
	GetGroupID() interface{}
	GetTaskID() interface{}
	HashID() uint64
}

// DependencyConfig holds configuration for handling dependent tasks
type DependencyConfig[T any] struct {
	// Core configuration
	EqualsTaskID      func(originTaskID interface{}, targetTaskID interface{}) bool
	EqualsGroupID     func(originGroupID interface{}, targetGroupID interface{}) bool
	Strategy          DependencyStrategy // Determines task scheduling strategy
	MaxDynamicWorkers int                // Maximum number of workers to create dynamically
	AutoCreateWorkers bool               // Whether to automatically create new workers when needed
	WorkerFactory     func() Worker[T]   // Factory function for creating new workers

	// Callbacks for task lifecycle events
	OnDependencyFailure func(groupID interface{}, taskID interface{}, dependentIDs []interface{}, reason string) TaskAction
	OnCreateWorker      func(workerID int)
	OnTaskWaiting       func(groupID interface{}, taskID interface{})
	OnGroupCreated      func(groupID interface{})
	OnTaskRunning       func(groupID interface{}, taskID interface{})
	OnGroupCompleted    func(groupID interface{})
	OnGroupStarted      func(groupID interface{})
	OnTaskQueued        func(groupID interface{}, taskID interface{})
	OnTaskProcessed     func(groupID interface{}, taskID interface{})

	// Channel for handling group completion requests
	OnGroupCompletedChan chan *RequestResponse[interface{}, error]
}

// TaskInfo holds metadata about a task
type TaskInfo struct {
	State        TaskState
	Dependencies []interface{}
	Completed    bool
	Failed       bool
	Processing   bool
	WorkerID     int // Tracks which worker is processing the task
	RetryCount   int
	QueuedAt     time.Time
	StartedAt    time.Time
	WaitingOn    []interface{} // List of task IDs this task is waiting on
}

// GroupInfo holds metadata about a group
type GroupInfo struct {
	Tasks          map[interface{}]*TaskInfo
	Started        bool
	Priority       int
	TaskOrder      []interface{} // Maintains order of task submission
	ActiveTasks    int32         // Number of currently processing tasks
	CompletedTasks int32
	FailedTasks    int32
}

// DependencyPool manages task dependencies and execution order
type DependencyPool[T any] struct {
	pool                 *Pool[T]
	config               *DependencyConfig[T]
	groups               map[interface{}]*GroupInfo
	waitingTasks         map[interface{}][]pendingTask[T]
	dynamicWorkers       int64
	workerStats          map[int]*workerStats
	mu                   deadlock.RWMutex
	completionChan       chan *RequestResponse[interface{}, error]
	logger               Logger
	distributionStrategy TaskDistributionStrategy
	lastWorkerIndex      atomic.Int64 // For round-robin distribution
}

// workerStats tracks worker-specific metrics
type workerStats struct {
	activeTaskCount atomic.Int64
	totalProcessed  atomic.Int64
	lastTaskTime    atomic.Int64
}

// pendingTask holds a task waiting for dependencies
type pendingTask[T any] struct {
	data    T
	options []TaskOption[T]
}

func NewDependencyPool[T any](pool *Pool[T], config *DependencyConfig[T]) (*DependencyPool[T], error) {
	pool.logger.Debug(pool.ctx, "Creating new DependencyPool", "has_completion_chan", config.OnGroupCompletedChan != nil, "strategy", config.Strategy)

	if config.EqualsTaskID == nil || config.EqualsGroupID == nil {
		pool.logger.Error(pool.ctx, "Missing required comparison functions", "equals_task_id_nil", config.EqualsTaskID == nil, "equals_group_id_nil", config.EqualsGroupID == nil)
		return nil, fmt.Errorf("EqualsTaskID and EqualsGroupID functions must be provided")
	}
	if !pool.config.roundRobin {
		pool.logger.Error(pool.ctx, "Pool not configured for round-robin distribution")
		return nil, fmt.Errorf("dependency pool requires round-robin task distribution")
	}

	dp := &DependencyPool[T]{
		pool:                 pool,
		config:               config,
		groups:               make(map[interface{}]*GroupInfo),
		waitingTasks:         make(map[interface{}][]pendingTask[T]),
		completionChan:       config.OnGroupCompletedChan,
		logger:               pool.logger,
		workerStats:          make(map[int]*workerStats),
		distributionStrategy: TaskDistributionRoundRobin,
	}

	// Initialize worker stats
	workers, _ := pool.Workers()
	pool.logger.Debug(pool.ctx, "Initializing worker stats", "worker_count", len(workers))
	for _, workerID := range workers {
		dp.workerStats[workerID] = &workerStats{}
	}

	// Set up handlers
	pool.SetOnTaskSuccess(func(data T) {
		if task, ok := any(data).(DependentTask); ok {
			dp.pool.logger.Debug(dp.pool.ctx, "Task success callback triggered", "task_id", task.GetTaskID(), "group_id", task.GetGroupID())
			dp.handleTaskSuccess(task)
		}
	})

	pool.SetOnTaskFailure(func(data T, err error) TaskAction {
		if task, ok := any(data).(DependentTask); ok {
			pool.logger.Debug(pool.ctx, "Task failure callback triggered", "task_id", task.GetTaskID(), "group_id", task.GetGroupID(), "error", err)
			return dp.handleTaskFailure(task, err)
		}
		return TaskActionRetry
	})

	if dp.completionChan != nil {
		pool.logger.Debug(pool.ctx, "Starting group completion handler")
		go dp.handleCompletionRequests()
	}

	pool.logger.Info(pool.ctx, "DependencyPool created successfully", "worker_count", len(workers))
	return dp, nil
}

func (dp *DependencyPool[T]) Submit(data T, options ...TaskOption[T]) error {
	task, ok := any(data).(DependentTask)
	if !ok {
		dp.pool.logger.Error(dp.pool.ctx, "Invalid task type", "data_type", fmt.Sprintf("%T", data))
		return fmt.Errorf("submitted data must implement DependentTask interface")
	}

	dp.pool.logger.Debug(dp.pool.ctx, "Submitting task", "group_id", task.GetGroupID(), "task_id", task.GetTaskID(), "dependencies", task.GetDependencies())

	dp.mu.Lock()
	defer dp.mu.Unlock()

	groupID := task.GetGroupID()
	taskID := task.GetTaskID()

	group, exists := dp.groups[groupID]
	if !exists {
		dp.pool.logger.Debug(dp.pool.ctx, "Creating new group", "group_id", groupID)
		group = &GroupInfo{
			Tasks:     make(map[interface{}]*TaskInfo),
			TaskOrder: make([]interface{}, 0),
		}
		dp.groups[groupID] = group

		if dp.config.OnGroupCreated != nil {
			dp.config.OnGroupCreated(groupID)
		}
	}

	taskInfo := &TaskInfo{
		State:        TaskStateCreated,
		Dependencies: task.GetDependencies(),
		QueuedAt:     time.Now(),
	}
	group.Tasks[taskID] = taskInfo
	group.TaskOrder = append(group.TaskOrder, taskID)

	dp.pool.logger.Debug(dp.pool.ctx, "Checking task submission eligibility", "group_id", groupID, "task_id", taskID, "dependencies_count", len(task.GetDependencies()))

	if dp.canSubmitTask(task) {
		dp.pool.logger.Debug(dp.pool.ctx, "Task eligible for immediate submission", "group_id", groupID, "task_id", taskID)
		dp.mu.Unlock() // Release lock before submitting
		err := dp.submitTask(data, options...)
		dp.mu.Lock() // Reacquire lock
		return err
	}

	dp.pool.logger.Debug(dp.pool.ctx, "Adding task to waiting list", "group_id", groupID, "task_id", taskID)

	if _, exists := dp.waitingTasks[groupID]; !exists {
		dp.waitingTasks[groupID] = make([]pendingTask[T], 0)
	}
	dp.waitingTasks[groupID] = append(dp.waitingTasks[groupID], pendingTask[T]{
		data:    data,
		options: options,
	})

	dp.checkAndCreateWorkers()
	return nil
}

func (dp *DependencyPool[T]) canSubmitTask(task DependentTask) bool {
	dp.pool.logger.Debug(dp.pool.ctx, "Checking task dependencies", "group_id", task.GetGroupID(), "task_id", task.GetTaskID())

	group, exists := dp.groups[task.GetGroupID()]
	if !exists {
		dp.pool.logger.Error(dp.pool.ctx, "Group not found for dependency check", "group_id", task.GetGroupID())
		return false
	}

	// Check each dependency with detailed logging
	for _, depID := range task.GetDependencies() {
		depTask, exists := group.Tasks[depID]
		if !exists {
			dp.pool.logger.Debug(dp.pool.ctx, "Dependency task not found", "group_id", task.GetGroupID(), "task_id", task.GetTaskID(), "dependency_id", depID)
			return false
		}

		if !depTask.Completed {
			dp.pool.logger.Debug(dp.pool.ctx, "Dependency not yet completed", "group_id", task.GetGroupID(), "task_id", task.GetTaskID(), "dependency_id", depID, "dependency_state", depTask.State)
			return false
		}
	}

	// Priority strategy check with proper group state validation
	if dp.config.Strategy == DependencyStrategyPriority {
		if !group.Started {
			for otherGroupID, otherGroup := range dp.groups {
				if otherGroup.Started {
					dp.pool.logger.Debug(dp.pool.ctx, "Cannot start new group while another is in progress",
						"group_id", task.GetGroupID(),
						"task_id", task.GetTaskID(),
						"blocking_group", otherGroupID)
					return false
				}
			}
		}
	}

	dp.pool.logger.Debug(dp.pool.ctx, "Task dependencies satisfied", "group_id", task.GetGroupID(), "task_id", task.GetTaskID())
	return true
}

func (dp *DependencyPool[T]) checkAndCreateWorkers() {
	if !dp.config.AutoCreateWorkers || dp.config.WorkerFactory == nil {
		dp.pool.logger.Debug(dp.pool.ctx, "Auto worker creation disabled or no factory available", "auto_create", dp.config.AutoCreateWorkers, "has_factory", dp.config.WorkerFactory != nil)
		return
	}

	currentWorkers := atomic.LoadInt64(&dp.dynamicWorkers)
	if dp.config.MaxDynamicWorkers > 0 && currentWorkers >= int64(dp.config.MaxDynamicWorkers) {
		dp.pool.logger.Debug(dp.pool.ctx, "Maximum dynamic workers reached", "current", currentWorkers, "max", dp.config.MaxDynamicWorkers)
		return
	}

	totalLoad := int64(0)
	for _, stats := range dp.workerStats {
		totalLoad += stats.activeTaskCount.Load()
	}

	availableWorkers := len(dp.workerStats)
	dp.pool.logger.Debug(dp.pool.ctx, "Checking worker load", "total_load", totalLoad, "available_workers", availableWorkers)

	if availableWorkers == 0 || totalLoad/int64(availableWorkers) > 2 {
		dp.pool.logger.Info(dp.pool.ctx, "Creating new worker", "current_workers", currentWorkers)
		worker := dp.config.WorkerFactory()
		if err := dp.pool.Add(worker, nil); err == nil {
			newWorkerID := dp.pool.nextWorkerID - 1
			dp.workerStats[newWorkerID] = &workerStats{}
			atomic.AddInt64(&dp.dynamicWorkers, 1)

			if dp.config.OnCreateWorker != nil {
				dp.config.OnCreateWorker(newWorkerID)
			}
			dp.pool.logger.Info(dp.pool.ctx, "New worker created successfully", "worker_id", newWorkerID)
		} else {
			dp.pool.logger.Error(dp.pool.ctx, "Failed to create new worker", "error", err)
		}
	}
}

func (dp *DependencyPool[T]) selectWorker(task DependentTask) (int, error) {
	workers, err := dp.pool.Workers()
	if err != nil || len(workers) == 0 {
		dp.pool.logger.Error(dp.pool.ctx, "No workers available", "error", err, "workers_count", len(workers))
		return -1, fmt.Errorf("no workers available")
	}

	dp.pool.logger.Debug(dp.pool.ctx, "Selecting worker for task", "task_id", task.GetTaskID(), "strategy", dp.distributionStrategy, "workers_count", len(workers))

	switch dp.distributionStrategy {
	case TaskDistributionRoundRobin:
		nextIndex := dp.lastWorkerIndex.Add(1) % int64(len(workers))
		dp.pool.logger.Debug(dp.pool.ctx, "Selected worker using round-robin", "worker_id", workers[nextIndex], "index", nextIndex)
		return workers[nextIndex], nil

	case TaskDistributionLeastLoaded:
		var minLoad int64 = math.MaxInt64
		selectedWorker := -1

		for _, workerID := range workers {
			if stats := dp.workerStats[workerID]; stats != nil {
				currentLoad := stats.activeTaskCount.Load()
				if currentLoad < minLoad {
					minLoad = currentLoad
					selectedWorker = workerID
				}
			}
		}
		dp.pool.logger.Debug(dp.pool.ctx, "Selected worker using least-loaded strategy", "worker_id", selectedWorker, "load", minLoad)
		return selectedWorker, nil
	}

	dp.pool.logger.Error(dp.pool.ctx, "Invalid distribution strategy", "strategy", dp.distributionStrategy)
	return -1, fmt.Errorf("invalid distribution strategy")
}

func (dp *DependencyPool[T]) submitTask(data T, options ...TaskOption[T]) error {
	task := any(data).(DependentTask)
	groupID := task.GetGroupID()
	taskID := task.GetTaskID()

	dp.pool.logger.Debug(dp.pool.ctx, "Submitting task", "group_id", groupID, "task_id", taskID, "dependencies", task.GetDependencies())

	workerID, err := dp.selectWorker(task)
	if err != nil {
		dp.pool.logger.Error(dp.pool.ctx, "Worker selection failed", "error", err, "group_id", groupID, "task_id", taskID)
		return fmt.Errorf("worker selection failed: %w", err)
	}

	dp.mu.Lock()
	group := dp.groups[groupID]
	taskInfo := group.Tasks[taskID]
	taskInfo.State = TaskStateQueued
	taskInfo.WorkerID = workerID

	dp.pool.logger.Debug(dp.pool.ctx, "Checking for potential deadlocks", "worker_id", workerID, "task_id", taskID)
	for _, depID := range task.GetDependencies() {
		if depInfo := group.Tasks[depID]; depInfo != nil {
			if depInfo.State == TaskStateQueued && depInfo.WorkerID == workerID {
				newWorkerID, err := dp.selectWorker(task)
				if err != nil || newWorkerID == workerID {
					dp.mu.Unlock()
					dp.pool.logger.Error(dp.pool.ctx, "Deadlock prevention failed", "error", err, "original_worker", workerID, "new_worker", newWorkerID)
					return fmt.Errorf("deadlock prevention: cannot find alternative worker")
				}
				workerID = newWorkerID
				taskInfo.WorkerID = workerID
				dp.pool.logger.Debug(dp.pool.ctx, "Reassigned task to prevent deadlock", "new_worker_id", workerID, "task_id", taskID)
			}
			taskInfo.WaitingOn = append(taskInfo.WaitingOn, depID)
		}
	}
	dp.mu.Unlock()

	dp.pool.logger.Debug(dp.pool.ctx, "Setting up task callbacks", "group_id", groupID, "task_id", taskID)
	options = append(options,

		WithQueuedCb[T](func() {
			dp.pool.logger.Debug(dp.pool.ctx, "Task queued callback triggered", "group_id", groupID, "task_id", taskID)
			if dp.config.OnTaskQueued != nil {
				dp.config.OnTaskQueued(groupID, taskID)
			}
		}),
		// WithQueuedCb[T](func() {
		// 	dp.pool.logger.Debug(dp.pool.ctx, "Task queued callback triggered", "group_id", groupID, "task_id", taskID)
		// 	if dp.config.OnTaskQueued != nil {
		// 		dp.config.OnTaskQueued(groupID, taskID)
		// 	}
		// }),
		// WithProcessedCb[T](func() {
		// 	dp.pool.logger.Debug(dp.pool.ctx, "Task processed callback triggered", "group_id", groupID, "task_id", taskID)
		// 	dp.mu.Lock()
		// 	if group := dp.groups[groupID]; group != nil {
		// 		if taskInfo := group.Tasks[taskID]; taskInfo != nil {
		// 			dp.pool.logger.Debug(dp.pool.ctx, "Updating task state in processed callback", "group_id", groupID, "task_id", taskID, "old_state", taskInfo.State)
		// 			taskInfo.State = TaskStateCompleted
		// 			taskInfo.Completed = true
		// 			taskInfo.Processing = false
		// 			atomic.AddInt32(&group.CompletedTasks, 1)
		// 		}
		// 	}
		// 	dp.mu.Unlock()

		// 	if dp.config.OnTaskProcessed != nil {
		// 		dp.config.OnTaskProcessed(groupID, taskID)
		// 	}

		// 	// Trigger processing of waiting tasks that might depend on this one
		// 	dp.processWaitingTasks(groupID)
		// }),
	)

	if stats := dp.workerStats[workerID]; stats != nil {
		stats.activeTaskCount.Add(1)
		dp.pool.logger.Debug(dp.pool.ctx, "Updated worker stats", "worker_id", workerID, "active_count", stats.activeTaskCount.Load())
	}

	options = append(options, WithBounceRetry[T]())

	if err := dp.pool.Submit(data, options...); err != nil {
		if stats := dp.workerStats[workerID]; stats != nil {
			stats.activeTaskCount.Add(-1)
		}
		dp.pool.logger.Error(dp.pool.ctx, "Failed to submit task to pool", "error", err, "group_id", groupID, "task_id", taskID)
		return err
	}

	if dp.config.OnTaskRunning != nil {
		dp.config.OnTaskRunning(groupID, taskID)
	}

	if !group.Started {
		group.Started = true
		dp.pool.logger.Debug(dp.pool.ctx, "Starting group", "group_id", groupID)
		if dp.config.OnGroupStarted != nil {
			dp.config.OnGroupStarted(groupID)
		}
	}

	atomic.AddInt32(&group.ActiveTasks, 1)
	dp.pool.logger.Debug(dp.pool.ctx, "Task submitted successfully", "group_id", groupID, "task_id", taskID, "worker_id", workerID)
	return nil
}

func (dp *DependencyPool[T]) handleTaskSuccess(task DependentTask) {
	dp.pool.logger.Debug(dp.pool.ctx, "Processing task success", "group_id", task.GetGroupID(), "task_id", task.GetTaskID())
	dp.mu.Lock()

	groupID := task.GetGroupID()
	taskID := task.GetTaskID()
	group := dp.groups[groupID]

	if group == nil {
		dp.mu.Unlock()
		dp.pool.logger.Error(dp.pool.ctx, "Group not found for successful task", "group_id", groupID, "task_id", taskID)
		return
	}

	taskInfo := group.Tasks[taskID]
	if taskInfo == nil {
		dp.mu.Unlock()
		dp.pool.logger.Error(dp.pool.ctx, "Task info not found", "group_id", groupID, "task_id", taskID)
		return
	}

	// Update task state and metrics while holding the lock
	dp.pool.logger.Debug(dp.pool.ctx, "Updating task state", "group_id", groupID, "task_id", taskID, "previous_state", taskInfo.State)
	taskInfo.State = TaskStateCompleted
	taskInfo.Completed = true
	taskInfo.Processing = false

	if stats := dp.workerStats[taskInfo.WorkerID]; stats != nil {
		stats.activeTaskCount.Add(-1)
		stats.totalProcessed.Add(1)
		stats.lastTaskTime.Store(time.Now().UnixNano())
	}

	atomic.AddInt32(&group.ActiveTasks, -1)
	atomic.AddInt32(&group.CompletedTasks, 1)

	// Call OnTaskProcessed here, while we still have task state information
	if dp.config.OnTaskProcessed != nil {
		dp.config.OnTaskProcessed(groupID, taskID)
	}

	// Store necessary information before releasing lock
	completed := atomic.LoadInt32(&group.CompletedTasks)+atomic.LoadInt32(&group.FailedTasks) == int32(len(group.Tasks))

	dp.mu.Unlock()

	// Process waiting tasks without holding the lock
	dp.processWaitingTasks(groupID)

	// Handle group completion
	if completed {
		dp.mu.Lock()
		dp.pool.logger.Info(dp.pool.ctx, "Group completed", "group_id", groupID, "total_tasks", len(group.Tasks))
		if dp.config.OnGroupCompleted != nil {
			dp.config.OnGroupCompleted(groupID)
		}

		if dp.completionChan == nil {
			delete(dp.groups, groupID)
			delete(dp.waitingTasks, groupID)
			dp.pool.logger.Debug(dp.pool.ctx, "Cleaned up completed group", "group_id", groupID)
		}
		dp.mu.Unlock()
	}
}

func (dp *DependencyPool[T]) handleTaskFailure(task DependentTask, err error) TaskAction {
	dp.pool.logger.Debug(dp.pool.ctx, "Processing task failure", "group_id", task.GetGroupID(), "task_id", task.GetTaskID(), "error", err)
	dp.mu.Lock()
	defer dp.mu.Unlock()

	groupID := task.GetGroupID()
	taskID := task.GetTaskID()

	group := dp.groups[groupID]
	if group == nil {
		dp.pool.logger.Error(dp.pool.ctx, "Group not found for failed task", "group_id", groupID, "task_id", taskID)
		return TaskActionAddToDeadTasks
	}

	taskInfo := group.Tasks[taskID]
	if taskInfo == nil {
		dp.pool.logger.Error(dp.pool.ctx, "Task info not found for failed task", "group_id", groupID, "task_id", taskID)
		return TaskActionAddToDeadTasks
	}

	taskInfo.RetryCount++
	taskInfo.Failed = true
	taskInfo.Processing = false
	dp.pool.logger.Debug(dp.pool.ctx, "Updated task failure state", "group_id", groupID, "task_id", taskID, "retry_count", taskInfo.RetryCount)

	if stats := dp.workerStats[taskInfo.WorkerID]; stats != nil {
		stats.activeTaskCount.Add(-1)
	}

	atomic.AddInt32(&group.ActiveTasks, -1)
	atomic.AddInt32(&group.FailedTasks, 1)
	dp.pool.logger.Debug(dp.pool.ctx, "Updated group failure stats", "group_id", groupID, "active_tasks", atomic.LoadInt32(&group.ActiveTasks), "failed_tasks", atomic.LoadInt32(&group.FailedTasks))

	if dp.config.OnDependencyFailure != nil {
		action := dp.config.OnDependencyFailure(groupID, taskID, task.GetDependencies(), err.Error())
		dp.pool.logger.Debug(dp.pool.ctx, "Dependency failure callback executed", "group_id", groupID, "task_id", taskID, "action", action)
		return action
	}

	if taskInfo.RetryCount < 3 {
		taskInfo.Failed = false
		atomic.AddInt32(&group.FailedTasks, -1)
		dp.pool.logger.Debug(dp.pool.ctx, "Retrying failed task", "group_id", groupID, "task_id", taskID, "retry_count", taskInfo.RetryCount)
		return TaskActionRetry
	}

	dp.pool.logger.Info(dp.pool.ctx, "Task exceeded retry limit", "group_id", groupID, "task_id", taskID, "retry_count", taskInfo.RetryCount)
	return TaskActionAddToDeadTasks
}

func (dp *DependencyPool[T]) processWaitingTasks(groupID interface{}) {
	dp.pool.logger.Debug(dp.pool.ctx, "Processing waiting tasks", "group_id", groupID)

	waiting, exists := dp.waitingTasks[groupID]
	if !exists || len(waiting) == 0 {
		dp.pool.logger.Debug(dp.pool.ctx, "No waiting tasks found", "group_id", groupID)
		return
	}

	var remainingTasks []pendingTask[T]
	var lastProcessedCount int
	processedCount := 0

	// Process tasks until we can't make any more progress
	for {
		lastProcessedCount = processedCount
		remainingTasks = make([]pendingTask[T], 0, len(waiting))

		for _, pt := range waiting {
			task, ok := any(pt.data).(DependentTask)
			if !ok {
				dp.pool.logger.Error(dp.pool.ctx, "Invalid task type in waiting list", "group_id", groupID)
				continue
			}

			dp.pool.logger.Debug(dp.pool.ctx, "Checking waiting task", "group_id", groupID, "task_id", task.GetTaskID())

			if dp.canSubmitTask(task) {
				dp.pool.logger.Debug(dp.pool.ctx, "Attempting to submit previously waiting task", "group_id", groupID, "task_id", task.GetTaskID())

				if err := dp.submitTask(pt.data, pt.options...); err != nil {
					dp.pool.logger.Error(dp.pool.ctx, "Failed to submit waiting task", "error", err, "group_id", groupID, "task_id", task.GetTaskID())
					remainingTasks = append(remainingTasks, pt)
				} else {
					processedCount++
					dp.pool.logger.Debug(dp.pool.ctx, "Successfully submitted waiting task", "group_id", groupID, "task_id", task.GetTaskID())
				}
			} else {
				dp.pool.logger.Debug(dp.pool.ctx, "Task still waiting on dependencies", "group_id", groupID, "task_id", task.GetTaskID())
				remainingTasks = append(remainingTasks, pt)
			}
		}

		waiting = remainingTasks

		// If we didn't process any new tasks in this iteration, break
		if processedCount == lastProcessedCount {
			break
		}
	}

	dp.pool.logger.Debug(dp.pool.ctx, "Waiting tasks processing completed", "group_id", groupID, "processed_count", processedCount, "remaining_count", len(remainingTasks))

	if len(remainingTasks) > 0 {
		dp.waitingTasks[groupID] = remainingTasks
	} else {
		delete(dp.waitingTasks, groupID)
	}
}

func (dp *DependencyPool[T]) handleCompletionRequests() {
	dp.pool.logger.Info(dp.pool.ctx, "Starting group completion request handler")
	for req := range dp.completionChan {
		dp.pool.logger.Debug(dp.pool.ctx, "Processing group completion request", "request_group", req.Request)
		dp.mu.Lock()
		group, exists := dp.groups[req.Request]
		if !exists {
			dp.pool.logger.Error(dp.pool.ctx, "Group not found for completion request", "request_group", req.Request)
			dp.mu.Unlock()
			req.CompleteWithError(fmt.Errorf("group %v not found", req.Request))
			continue
		}

		completed := atomic.LoadInt32(&group.CompletedTasks)+atomic.LoadInt32(&group.FailedTasks) == int32(len(group.Tasks))
		dp.pool.logger.Debug(dp.pool.ctx, "Checking group completion status", "group_id", req.Request, "completed", completed, "total_tasks", len(group.Tasks))

		if completed {
			groupID := req.Request
			delete(dp.groups, groupID)
			delete(dp.waitingTasks, groupID)
			dp.mu.Unlock()
			dp.pool.logger.Info(dp.pool.ctx, "Group completion confirmed and cleaned up", "group_id", groupID)
			req.Complete(nil)
		} else {
			dp.mu.Unlock()
			dp.pool.logger.Debug(dp.pool.ctx, "Group not yet completed", "group_id", req.Request, "completed_tasks", group.CompletedTasks, "failed_tasks", group.FailedTasks)
			req.CompleteWithError(fmt.Errorf("group %v is not yet completed", req.Request))
		}
	}
}

func (dp *DependencyPool[T]) GetTaskState(groupID, taskID interface{}) (TaskState, error) {
	dp.pool.logger.Debug(dp.pool.ctx, "Getting task state", "group_id", groupID, "task_id", taskID)
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	group, exists := dp.groups[groupID]
	if !exists {
		dp.pool.logger.Error(dp.pool.ctx, "Group not found for task state query", "group_id", groupID)
		return TaskState(0), fmt.Errorf("group %v not found", groupID)
	}

	taskInfo, exists := group.Tasks[taskID]
	if !exists {
		dp.pool.logger.Error(dp.pool.ctx, "Task not found in group", "group_id", groupID, "task_id", taskID)
		return TaskState(0), fmt.Errorf("task %v not found in group %v", taskID, groupID)
	}

	dp.pool.logger.Debug(dp.pool.ctx, "Task state retrieved", "group_id", groupID, "task_id", taskID, "state", taskInfo.State)
	return taskInfo.State, nil
}

func (dp *DependencyPool[T]) GetGroupStats(groupID interface{}) (active, completed, failed int32, err error) {
	dp.pool.logger.Debug(dp.pool.ctx, "Getting group stats", "group_id", groupID)
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	group, exists := dp.groups[groupID]
	if !exists {
		dp.pool.logger.Error(dp.pool.ctx, "Group not found for stats query", "group_id", groupID)
		return 0, 0, 0, fmt.Errorf("group %v not found", groupID)
	}

	active = atomic.LoadInt32(&group.ActiveTasks)
	completed = atomic.LoadInt32(&group.CompletedTasks)
	failed = atomic.LoadInt32(&group.FailedTasks)
	dp.pool.logger.Debug(dp.pool.ctx, "Group stats retrieved", "group_id", groupID, "active", active, "completed", completed, "failed", failed)
	return
}

func (dp *DependencyPool[T]) GetWorkerStats(workerID int) (activeCount, totalProcessed int64, err error) {
	dp.pool.logger.Debug(dp.pool.ctx, "Getting worker stats", "worker_id", workerID)
	stats, exists := dp.workerStats[workerID]
	if !exists {
		dp.pool.logger.Error(dp.pool.ctx, "Worker stats not found", "worker_id", workerID)
		return 0, 0, fmt.Errorf("worker %d not found", workerID)
	}

	activeCount = stats.activeTaskCount.Load()
	totalProcessed = stats.totalProcessed.Load()
	dp.pool.logger.Debug(dp.pool.ctx, "Worker stats retrieved", "worker_id", workerID, "active_count", activeCount, "total_processed", totalProcessed)
	return
}

func (dp *DependencyPool[T]) Close() error {
	dp.pool.logger.Info(dp.pool.ctx, "Closing dependency pool")
	dp.mu.Lock()
	defer dp.mu.Unlock()

	if dp.completionChan != nil {
		close(dp.completionChan)
		dp.pool.logger.Debug(dp.pool.ctx, "Closed completion channel")
	}

	for groupID, group := range dp.groups {
		if dp.config.OnGroupCompleted != nil && len(group.Tasks) > 0 {
			dp.pool.logger.Debug(dp.pool.ctx, "Calling completion callback for remaining group", "group_id", groupID, "remaining_tasks", len(group.Tasks))
			dp.config.OnGroupCompleted(groupID)
		}
	}

	dp.groups = make(map[interface{}]*GroupInfo)
	dp.waitingTasks = make(map[interface{}][]pendingTask[T])
	dp.workerStats = make(map[int]*workerStats)
	dp.pool.logger.Debug(dp.pool.ctx, "Cleared all internal state")

	err := dp.pool.Close()
	if err != nil {
		dp.pool.logger.Error(dp.pool.ctx, "Error closing underlying pool", "error", err)
	}
	return err
}

func (dp *DependencyPool[T]) GetWaitingTasksCount(groupID interface{}) int {
	dp.mu.RLock()
	defer dp.mu.RUnlock()
	count := 0
	if tasks, exists := dp.waitingTasks[groupID]; exists {
		count = len(tasks)
	}
	dp.pool.logger.Debug(dp.pool.ctx, "Getting waiting tasks count", "group_id", groupID, "count", count)
	return count
}

func (dp *DependencyPool[T]) GetGroupTaskOrder(groupID interface{}) ([]interface{}, error) {
	dp.pool.logger.Debug(dp.pool.ctx, "Getting group task order", "group_id", groupID)
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	group, exists := dp.groups[groupID]
	if !exists {
		dp.pool.logger.Error(dp.pool.ctx, "Group not found for task order query", "group_id", groupID)
		return nil, fmt.Errorf("group %v not found", groupID)
	}

	order := make([]interface{}, len(group.TaskOrder))
	copy(order, group.TaskOrder)
	dp.pool.logger.Debug(dp.pool.ctx, "Group task order retrieved", "group_id", groupID, "order_length", len(order))
	return order, nil
}

func (dp *DependencyPool[T]) IsGroupStarted(groupID interface{}) bool {
	dp.mu.RLock()
	defer dp.mu.RUnlock()
	started := false
	if group, exists := dp.groups[groupID]; exists {
		started = group.Started
	}
	dp.pool.logger.Debug(dp.pool.ctx, "Checking if group is started", "group_id", groupID, "started", started)
	return started
}

func (dp *DependencyPool[T]) GetTaskDependencies(groupID, taskID interface{}) ([]interface{}, error) {
	dp.pool.logger.Debug(dp.pool.ctx, "Getting task dependencies", "group_id", groupID, "task_id", taskID)
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	group, exists := dp.groups[groupID]
	if !exists {
		dp.pool.logger.Error(dp.pool.ctx, "Group not found for dependencies query", "group_id", groupID)
		return nil, fmt.Errorf("group %v not found", groupID)
	}

	taskInfo, exists := group.Tasks[taskID]
	if !exists {
		dp.pool.logger.Error(dp.pool.ctx, "Task not found for dependencies query", "group_id", groupID, "task_id", taskID)
		return nil, fmt.Errorf("task %v not found in group %v", taskID, groupID)
	}

	deps := make([]interface{}, len(taskInfo.Dependencies))
	copy(deps, taskInfo.Dependencies)
	dp.pool.logger.Debug(dp.pool.ctx, "Task dependencies retrieved", "group_id", groupID, "task_id", taskID, "dependencies_count", len(deps))
	return deps, nil
}
