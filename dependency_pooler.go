package retrypool

import (
	"fmt"
	"sync"
	"sync/atomic"
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

type DependencyStrategy int

const (
	DependencyStrategyOrder DependencyStrategy = iota
	DependencyStrategyPriority
)

// DependencyConfig holds configuration for handling dependent tasks
type DependencyConfig[T any] struct {
	EqualsTaskID            func(originTaskID interface{}, targetTaskID interface{}) bool
	EqualsGroupID           func(originGroupID interface{}, targetGroupID interface{}) bool
	MaxDynamicWorkers       int
	AutoCreateWorkers       bool
	WorkerFactory           func() Worker[T]
	OnDependencyFailure     func(groupID interface{}, taskID interface{}, dependentIDs []interface{}, reason string) TaskAction
	OnCreateWorker          func(workerID int)
	OnTaskWaiting           func(groupID interface{}, taskID interface{})
	OnGroupCreated          func(groupID interface{})
	OnTaskRunning           func(groupID interface{}, taskID interface{})
	OnGroupCompleted        func(groupID interface{})
	OnGroupCompletedChan    chan *RequestResponse[interface{}, error]
	OnGroupStarted          func(groupID interface{})
	OnTaskQueued            func(groupID interface{}, taskID interface{})
	OnTaskProcessed         func(groupID interface{}, taskID interface{})
	PrioritizeStartedGroups bool
}

// DependentTask represents a task that depends on other tasks
type DependentTask interface {
	GetDependencies() []interface{}
	GetGroupID() interface{}
	GetTaskID() interface{}
	HashID() uint64
}

// dependencyGraph manages the dependency relationships between tasks
type dependencyGraph[T any] struct {
	mu           sync.RWMutex
	groups       map[interface{}]map[interface{}]*Task[T]
	dependencies map[interface{}][]interface{}
}

// pendingTask holds task data and options
type pendingTask[T any] struct {
	data    T
	options []TaskOption[T]
}

// DependencyPool wraps the Pool to manage dependent tasks
type DependencyPool[T any] struct {
	pool             *Pool[T]
	config           *DependencyConfig[T]
	dependencyGraph  *dependencyGraph[T]
	waitingTasks     map[interface{}][]pendingTask[T]
	groupStates      map[interface{}]map[interface{}]TaskState
	groupCompletedCh chan *RequestResponse[interface{}, error]
	mu               sync.Mutex

	dynamicWorkerCount int64
	startedGroups      []interface{}
}

// NewDependencyPool creates a new DependencyPool
func NewDependencyPool[T any](pool *Pool[T], config *DependencyConfig[T]) *DependencyPool[T] {
	pool.logger.Info(pool.ctx, "Creating new DependencyPool")

	dp := &DependencyPool[T]{
		pool:   pool,
		config: config,
		dependencyGraph: &dependencyGraph[T]{
			groups:       make(map[interface{}]map[interface{}]*Task[T]),
			dependencies: make(map[interface{}][]interface{}),
		},
		waitingTasks:     make(map[interface{}][]pendingTask[T]),
		groupStates:      make(map[interface{}]map[interface{}]TaskState),
		groupCompletedCh: config.OnGroupCompletedChan,
		startedGroups:    make([]interface{}, 0),
	}

	pool.logger.Debug(pool.ctx, "Setting up success handler")
	pool.SetOnTaskSuccess(func(data T) {
		pool.logger.Debug(pool.ctx, "Task success callback triggered", "data", fmt.Sprintf("%+v", data))
		dp.HandlePoolTaskSuccess(data)
	})

	pool.logger.Debug(pool.ctx, "Setting up failure handler")
	pool.SetOnTaskFailure(func(data T, err error) TaskAction {
		pool.logger.Debug(pool.ctx, "Task failure callback triggered", "data", fmt.Sprintf("%+v", data), "error", err)
		if dp.config.OnDependencyFailure != nil {
			if dependentTask, ok := any(data).(DependentTask); ok {
				dp.config.OnDependencyFailure(
					dependentTask.GetGroupID(),
					dependentTask.GetTaskID(),
					dependentTask.GetDependencies(),
					err.Error(),
				)
			}
		}
		dp.HandlePoolTaskFailure(data, err)
		return TaskActionRetry
	})

	if config.OnGroupCompletedChan != nil {
		pool.logger.Debug(pool.ctx, "Starting group completion handler")
		go dp.handleGroupCompletedRequests()
	}

	pool.logger.Info(pool.ctx, "DependencyPool created successfully")
	return dp
}

// Close closes the underlying pool
func (dp *DependencyPool[T]) Close() error {
	dp.pool.logger.Info(dp.pool.ctx, "Closing DependencyPool")
	return dp.pool.Close()
}

// Submit submits a task to the pool, handling dependencies
// We only care about DependentTask
func (dp *DependencyPool[T]) Submit(data T, options ...TaskOption[T]) error {
	var dependentTask DependentTask
	var ok bool
	if dependentTask, ok = any(data).(DependentTask); !ok {
		dp.pool.logger.Error(dp.pool.ctx, "Task data is not a DependentTask", "data", fmt.Sprintf("%+v", data))
		return fmt.Errorf("task data is not a DependentTask")
	}

	dp.pool.logger.Debug(dp.pool.ctx, "Submitting task", "data", fmt.Sprintf("%+v", data))

	dp.pool.logger.Debug(dp.pool.ctx, "Task is dependent",
		"group_id", dependentTask.GetGroupID(),
		"task_id", dependentTask.GetTaskID(),
		"dependencies", dependentTask.GetDependencies())
	return dp.submitDependentTask(data, dependentTask, options...)
}

// checkAndCreateWorkers checks if more workers are needed and creates them if allowed
func (dp *DependencyPool[T]) checkAndCreateWorkers() {
	dp.pool.logger.Debug(dp.pool.ctx, "Checking if new workers are needed",
		"auto_create", dp.config.AutoCreateWorkers,
		"has_factory", dp.config.WorkerFactory != nil)

	if !dp.config.AutoCreateWorkers || dp.config.WorkerFactory == nil {
		return
	}

	availableWorkers := dp.pool.availableWorkers.Load()
	totalQueueSize := dp.pool.QueueSize()

	dp.pool.logger.Debug(dp.pool.ctx, "Worker stats",
		"available_workers", availableWorkers,
		"queue_size", totalQueueSize,
		"dynamic_workers", dp.dynamicWorkerCount,
		"max_dynamic_workers", dp.config.MaxDynamicWorkers)

	if totalQueueSize > availableWorkers && dp.dynamicWorkerCount < int64(dp.config.MaxDynamicWorkers) {
		dp.pool.logger.Info(dp.pool.ctx, "Creating new worker")
		newWorker := dp.config.WorkerFactory()
		if err := dp.pool.Add(newWorker, nil); err == nil {
			newWorkerID := dp.pool.nextWorkerID - 1
			atomic.AddInt64(&dp.dynamicWorkerCount, 1)
			if dp.config.OnCreateWorker != nil {
				dp.pool.logger.Debug(dp.pool.ctx, "Calling OnCreateWorker callback", "worker_id", newWorkerID)
				dp.config.OnCreateWorker(newWorkerID)
			}
			dp.pool.logger.Info(dp.pool.ctx, "New worker created successfully", "worker_id", newWorkerID)
		} else {
			dp.pool.logger.Error(dp.pool.ctx, "Failed to create new worker", "error", err)
		}
	}
}

// HandlePoolTaskSuccess handles successful task completion
func (dp *DependencyPool[T]) HandlePoolTaskSuccess(data T) {
	dependentTask, ok := any(data).(DependentTask)
	if !ok {
		dp.pool.logger.Error(dp.pool.ctx, "Task data is not a DependentTask", "data", fmt.Sprintf("%+v", data))
		return
	}

	groupID := dependentTask.GetGroupID()
	taskID := dependentTask.GetTaskID()

	dp.mu.Lock()
	defer dp.mu.Unlock()

	dp.pool.logger.Info(dp.pool.ctx, "Processing task completion",
		"group_id", groupID,
		"task_id", taskID,
		"dependencies", dependentTask.GetDependencies())

	dp.groupStates[groupID][taskID] = TaskStateCompleted

	// Check group completion
	allCompleted := true
	completedCount := 0
	totalCount := len(dp.groupStates[groupID])
	for tid, state := range dp.groupStates[groupID] {
		if state == TaskStateCompleted {
			completedCount++
		} else {
			dp.pool.logger.Debug(dp.pool.ctx, "Task not completed",
				"task_id", tid,
				"state", state)
			allCompleted = false
		}
	}

	dp.pool.logger.Debug(dp.pool.ctx, "Group completion status",
		"group_id", groupID,
		"completed_tasks", completedCount,
		"total_tasks", totalCount,
		"all_completed", allCompleted)

	if allCompleted {
		dp.pool.logger.Info(dp.pool.ctx, "Group completed", "group_id", groupID)

		// Remove from started groups
		for i, id := range dp.startedGroups {
			if id == groupID {
				dp.startedGroups = append(dp.startedGroups[:i], dp.startedGroups[i+1:]...)
				break
			}
		}

		if dp.config.OnGroupCompleted != nil {
			dp.pool.logger.Debug(dp.pool.ctx, "Calling OnGroupCompleted callback", "group_id", groupID)
			dp.config.OnGroupCompleted(groupID)
		}

		// Handle prioritized groups
		if dp.config.PrioritizeStartedGroups {
			dp.pool.logger.Debug(dp.pool.ctx, "Processing next group due to prioritization")

			for nextGroupID, tasks := range dp.waitingTasks {
				dp.pool.logger.Info(dp.pool.ctx, "Starting next group",
					"group_id", nextGroupID,
					"waiting_tasks", len(tasks))

				dp.startedGroups = append(dp.startedGroups, nextGroupID)

				if dp.config.OnGroupStarted != nil {
					dp.pool.logger.Debug(dp.pool.ctx, "Calling OnGroupStarted callback", "group_id", nextGroupID)
					dp.config.OnGroupStarted(nextGroupID)
				}

				var remainingTasks []pendingTask[T]
				submittedCount := 0

				for _, task := range tasks {
					if dt, ok := any(task.data).(DependentTask); ok {
						if len(dt.GetDependencies()) == 0 {
							dp.pool.logger.Debug(dp.pool.ctx, "Submitting task with no dependencies",
								"group_id", nextGroupID,
								"task_id", dt.GetTaskID())

							if err := dp.pool.Submit(task.data, task.options...); err != nil {
								dp.pool.logger.Error(dp.pool.ctx, "Failed to submit task",
									"error", err,
									"group_id", nextGroupID,
									"task_id", dt.GetTaskID())
								continue
							}

							dp.groupStates[nextGroupID][dt.GetTaskID()] = TaskStateQueued
							submittedCount++

							if dp.config.OnTaskQueued != nil {
								dp.config.OnTaskQueued(nextGroupID, dt.GetTaskID())
							}
						} else {
							dp.pool.logger.Debug(dp.pool.ctx, "Task has dependencies, keeping in waiting list",
								"group_id", nextGroupID,
								"task_id", dt.GetTaskID(),
								"dependencies", dt.GetDependencies())
							remainingTasks = append(remainingTasks, task)
						}
					}
				}

				dp.pool.logger.Info(dp.pool.ctx, "Group processing complete",
					"group_id", nextGroupID,
					"submitted_tasks", submittedCount,
					"remaining_tasks", len(remainingTasks))

				if len(remainingTasks) > 0 {
					dp.waitingTasks[nextGroupID] = remainingTasks
				} else {
					delete(dp.waitingTasks, nextGroupID)
				}

				break // Only start one group
			}
		}
	}

	// Check for dependent tasks that can now be submitted
	dp.checkAndSubmitDependentTasks(groupID)
}

// HandlePoolTaskFailure handles task failures
func (dp *DependencyPool[T]) HandlePoolTaskFailure(data T, err error) {
	if dependentTask, ok := any(data).(DependentTask); ok {
		dp.mu.Lock()
		defer dp.mu.Unlock()

		groupID := dependentTask.GetGroupID()
		taskID := dependentTask.GetTaskID()

		dp.pool.logger.Error(dp.pool.ctx, "Task failed",
			"group_id", groupID,
			"task_id", taskID,
			"error", err)

		dp.groupStates[groupID][taskID] = TaskStateFailed
	}
}

// submitDependentTask handles the submission of dependent tasks
func (dp *DependencyPool[T]) submitDependentTask(data T, dependentTask DependentTask, options ...TaskOption[T]) error {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	groupID := dependentTask.GetGroupID()
	taskID := dependentTask.GetTaskID()

	dp.pool.logger.Info(dp.pool.ctx, "Submitting dependent task",
		"group_id", groupID,
		"task_id", taskID,
		"dependencies", dependentTask.GetDependencies())

	// Initialize group if needed
	if _, exists := dp.groupStates[groupID]; !exists {
		dp.pool.logger.Debug(dp.pool.ctx, "Creating new group", "group_id", groupID)
		dp.groupStates[groupID] = make(map[interface{}]TaskState)
		if dp.config.OnGroupCreated != nil {
			dp.config.OnGroupCreated(groupID)
		}
	}

	dp.groupStates[groupID][taskID] = TaskStatePending

	canSubmit := true
	if dp.config.PrioritizeStartedGroups {
		if len(dp.startedGroups) == 0 {
			dp.pool.logger.Debug(dp.pool.ctx, "Starting first group", "group_id", groupID)
			dp.startedGroups = append(dp.startedGroups, groupID)
			if dp.config.OnGroupStarted != nil {
				dp.config.OnGroupStarted(groupID)
			}
		} else {
			isStarted := false
			for _, id := range dp.startedGroups {
				if id == groupID {
					isStarted = true
					break
				}
			}
			if !isStarted {
				dp.pool.logger.Debug(dp.pool.ctx, "Group not started, delaying submission",
					"group_id", groupID)
				canSubmit = false
			}
		}
	}

	// Check dependencies
	if canSubmit && len(dependentTask.GetDependencies()) > 0 {
		for _, depID := range dependentTask.GetDependencies() {
			if state, exists := dp.groupStates[groupID][depID]; !exists || state != TaskStateCompleted {
				dp.pool.logger.Debug(dp.pool.ctx, "Dependency not satisfied",
					"group_id", groupID,
					"task_id", taskID,
					"dependency_id", depID,
					"dependency_state", state)
				canSubmit = false
				break
			}
		}
	}

	if canSubmit {
		dp.pool.logger.Info(dp.pool.ctx, "Submitting task to pool",
			"group_id", groupID,
			"task_id", taskID)

		err := dp.pool.Submit(data, options...)
		if err != nil {
			dp.pool.logger.Error(dp.pool.ctx, "Failed to submit task",
				"group_id", groupID,
				"task_id", taskID,
				"error", err)
			return err
		}

		dp.groupStates[groupID][taskID] = TaskStateQueued
		if dp.config.OnTaskQueued != nil {
			dp.config.OnTaskQueued(groupID, taskID)
		}
	} else {
		dp.pool.logger.Debug(dp.pool.ctx, "Adding task to waiting list",
			"group_id", groupID,
			"task_id", taskID)

		dp.waitingTasks[groupID] = append(dp.waitingTasks[groupID],
			pendingTask[T]{data: data, options: options})

		if dp.config.OnTaskWaiting != nil {
			dp.config.OnTaskWaiting(groupID, taskID)
		}
	}

	dp.checkAndCreateWorkers()
	return nil
}

// checkAndSubmitDependentTasks checks and submits tasks whose dependencies are met
func (dp *DependencyPool[T]) checkAndSubmitDependentTasks(groupID interface{}) {
	tasks := dp.waitingTasks[groupID]
	if len(tasks) == 0 {
		return
	}

	dp.pool.logger.Debug(dp.pool.ctx, "Checking dependent tasks",
		"group_id", groupID,
		"waiting_tasks", len(tasks))

	var remainingTasks []pendingTask[T]
	submittedCount := 0

	for _, task := range tasks {
		if dt, ok := any(task.data).(DependentTask); ok {
			canSubmit := true
			for _, depID := range dt.GetDependencies() {
				if state, exists := dp.groupStates[groupID][depID]; !exists || state != TaskStateCompleted {
					dp.pool.logger.Debug(dp.pool.ctx, "Dependency not yet satisfied",
						"group_id", groupID,
						"task_id", dt.GetTaskID(),
						"dependency_id", depID,
						"dependency_state", state)
					canSubmit = false
					break
				}
			}

			if canSubmit {
				dp.pool.logger.Info(dp.pool.ctx, "Submitting dependent task",
					"group_id", groupID,
					"task_id", dt.GetTaskID())

				if err := dp.pool.Submit(task.data, task.options...); err != nil {
					dp.pool.logger.Error(dp.pool.ctx, "Failed to submit dependent task",
						"group_id", groupID,
						"task_id", dt.GetTaskID(),
						"error", err)
					continue
				}

				dp.groupStates[groupID][dt.GetTaskID()] = TaskStateQueued
				submittedCount++

				if dp.config.OnTaskQueued != nil {
					dp.config.OnTaskQueued(groupID, dt.GetTaskID())
				}
			} else {
				remainingTasks = append(remainingTasks, task)
			}
		}
	}

	dp.pool.logger.Info(dp.pool.ctx, "Dependent task processing complete",
		"group_id", groupID,
		"submitted_tasks", submittedCount,
		"remaining_tasks", len(remainingTasks))

	if len(remainingTasks) > 0 {
		dp.waitingTasks[groupID] = remainingTasks
	} else {
		delete(dp.waitingTasks, groupID)
	}
}

// handleGroupCompletedRequests handles completion status requests for groups
func (dp *DependencyPool[T]) handleGroupCompletedRequests() {
	dp.pool.logger.Info(dp.pool.ctx, "Starting group completion request handler")

	for req := range dp.groupCompletedCh {
		groupID := req.Request

		dp.pool.logger.Debug(dp.pool.ctx, "Processing group completion request",
			"group_id", groupID)

		dp.mu.Lock()
		if states, exists := dp.groupStates[groupID]; exists {
			allCompleted := true
			for tid, state := range states {
				if state != TaskStateCompleted && state != TaskStateFailed {
					dp.pool.logger.Debug(dp.pool.ctx, "Group not completed",
						"group_id", groupID,
						"task_id", tid,
						"state", state)
					allCompleted = false
					break
				}
			}

			if allCompleted {
				dp.pool.logger.Info(dp.pool.ctx, "Group is completed, cleaning up",
					"group_id", groupID)
				delete(dp.groupStates, groupID)
				delete(dp.waitingTasks, groupID)
				req.Complete(nil)
			} else {
				dp.pool.logger.Debug(dp.pool.ctx, "Group not yet completed",
					"group_id", groupID)
				req.CompleteWithError(fmt.Errorf("group %v is not completed", groupID))
			}
		} else {
			dp.pool.logger.Error(dp.pool.ctx, "Group not found",
				"group_id", groupID)
			req.CompleteWithError(fmt.Errorf("group %v does not exist", groupID))
		}
		dp.mu.Unlock()
	}
}
