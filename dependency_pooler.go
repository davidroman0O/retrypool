package retrypool

import (
	"fmt"
	"sync"
)

// DependencyConfig holds configuration for handling dependent tasks
type DependencyConfig[T any] struct {
	EqualsTaskID         func(originTaskID interface{}, targetTaskID interface{}) bool
	EqualsGroupID        func(originGroupID interface{}, targetGroupID interface{}) bool
	MaxDynamicWorkers    int
	AutoCreateWorkers    bool
	WorkerFactory        func() Worker[T]
	OnDependencyFailure  func(groupID interface{}, taskID interface{}, dependentIDs []interface{}, reason string) TaskAction
	OnCreateWorker       func(workerID int)
	OnTaskWaiting        func(groupID interface{}, taskID interface{})
	OnGroupCreated       func(groupID interface{})
	OnTaskRunning        func(groupID interface{}, taskID interface{})
	OnGroupCompleted     func(groupID interface{})
	OnGroupCompletedChan chan *RequestResponse[interface{}, error]
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
	groups       map[interface{}]map[interface{}]*Task[T] // GroupID -> TaskID -> Task
	dependencies map[interface{}][]interface{}            // TaskID -> []TaskID
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
	waitingTasks     map[interface{}][]pendingTask[T]          // GroupID -> []pendingTask[T]
	groupStates      map[interface{}]map[interface{}]TaskState // GroupID -> TaskID -> TaskState
	groupCompletedCh chan *RequestResponse[interface{}, error]
	mu               sync.Mutex
}

// NewDependencyPool creates a new DependencyPool
func NewDependencyPool[T any](pool *Pool[T], config *DependencyConfig[T]) *DependencyPool[T] {
	dp := &DependencyPool[T]{
		pool:             pool,
		config:           config,
		dependencyGraph:  &dependencyGraph[T]{groups: make(map[interface{}]map[interface{}]*Task[T]), dependencies: make(map[interface{}][]interface{})},
		waitingTasks:     make(map[interface{}][]pendingTask[T]),
		groupStates:      make(map[interface{}]map[interface{}]TaskState),
		groupCompletedCh: config.OnGroupCompletedChan,
	}

	// Set up the pool's onTaskSuccess and onTaskFailure handlers
	prevOnTaskSuccess := pool.config.onTaskSuccess
	pool.SetOnTaskSuccess(func(data T) {
		if prevOnTaskSuccess != nil {
			prevOnTaskSuccess(data)
		}
		dp.HandlePoolTaskSuccess(data)
	})

	prevOnTaskFailure := pool.config.onTaskFailure
	pool.SetOnTaskFailure(func(data T, err error) TaskAction {
		action := TaskActionRetry
		if prevOnTaskFailure != nil {
			action = prevOnTaskFailure(data, err)
		}
		dp.HandlePoolTaskFailure(data, err)
		return action
	})

	if dp.groupCompletedCh != nil {
		go dp.handleGroupCompletedRequests()
	}

	return dp
}

func (dp *DependencyPool[T]) Close() error {
	return dp.pool.Close()
}

// Submit submits a task to the pool, handling dependencies
func (dp *DependencyPool[T]) Submit(data T, options ...TaskOption[T]) error {
	if dependentTask, ok := any(data).(DependentTask); ok {
		return dp.submitDependentTask(data, dependentTask, options...)
	}

	return dp.pool.Submit(data, options...)
}

func (dp *DependencyPool[T]) submitDependentTask(data T, dependentTask DependentTask, options ...TaskOption[T]) error {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	groupID := dependentTask.GetGroupID()
	taskID := dependentTask.GetTaskID()
	dependencies := dependentTask.GetDependencies()

	// Add task to the dependency graph
	dp.dependencyGraph.mu.Lock()
	if _, exists := dp.dependencyGraph.groups[groupID]; !exists {
		dp.dependencyGraph.groups[groupID] = make(map[interface{}]*Task[T])
		// Invoke OnGroupCreated callback if provided
		if dp.config.OnGroupCreated != nil {
			dp.config.OnGroupCreated(groupID)
		}
	}
	dp.dependencyGraph.groups[groupID][taskID] = nil // Task will be set when submitted
	dp.dependencyGraph.dependencies[taskID] = dependencies
	dp.dependencyGraph.mu.Unlock()

	if _, exists := dp.groupStates[groupID]; !exists {
		dp.groupStates[groupID] = make(map[interface{}]TaskState)
	}

	dp.groupStates[groupID][taskID] = TaskStatePending

	// Check if all dependencies are completed
	allCompleted := true
	for _, depID := range dependencies {
		if state, exists := dp.groupStates[groupID][depID]; !exists || state != TaskStateCompleted {
			allCompleted = false
			break
		}
	}

	if allCompleted {
		// All dependencies are completed, submit the task
		err := dp.pool.Submit(data, options...)
		if err != nil {
			return err
		}
		dp.groupStates[groupID][taskID] = TaskStateQueued
		if dp.config.OnTaskRunning != nil {
			dp.config.OnTaskRunning(groupID, taskID)
		}
	} else {
		// Add to waiting tasks
		dp.waitingTasks[groupID] = append(dp.waitingTasks[groupID], pendingTask[T]{data: data, options: options})
		if dp.config.OnTaskWaiting != nil {
			dp.config.OnTaskWaiting(groupID, taskID)
		}
	}

	return nil
}

// HandlePoolTaskSuccess handles task success notifications from the pool
// TODO: use an interface to avoid exposing that publicly please
func (dp *DependencyPool[T]) HandlePoolTaskSuccess(data T) {
	fmt.Printf("HandlePoolTaskSuccess called with data: %v\n", data)
	dependentTask, ok := any(data).(DependentTask)
	if !ok {
		fmt.Println("Data is not a DependentTask:", data)
		return
	}
	groupID := dependentTask.GetGroupID()
	taskID := dependentTask.GetTaskID()
	dp.handleTaskCompletion(groupID, taskID, TaskStateCompleted)
}

// HandlePoolTaskFailure handles task failure notifications from the pool
// TODO: use an interface to avoid exposing that publicly please
func (dp *DependencyPool[T]) HandlePoolTaskFailure(data T, err error) {
	dependentTask, ok := any(data).(DependentTask)
	if !ok {
		return
	}
	// Handle failure as needed
	// For now, we can consider the task as failed
	groupID := dependentTask.GetGroupID()
	taskID := dependentTask.GetTaskID()
	dp.handleTaskCompletion(groupID, taskID, TaskStateFailed)
}

// handleTaskCompletion updates the state of a task and checks for group completion
func (dp *DependencyPool[T]) handleTaskCompletion(groupID interface{}, taskID interface{}, state TaskState) {
	dp.mu.Lock()
	dp.groupStates[groupID][taskID] = state
	fmt.Printf("Task %v in group %v marked as %v\n", taskID, groupID, state)

	// Check waiting tasks
	dp.checkWaitingTasks(groupID)

	// Check if the group is completed
	allCompleted := true
	for _, taskState := range dp.groupStates[groupID] {
		if taskState != TaskStateCompleted {
			allCompleted = false
			break
		}
	}

	if allCompleted {
		// Notify that the group is completed, but do not remove it
		if dp.config.OnGroupCompleted != nil {
			dp.config.OnGroupCompleted(groupID)
		}
		// Do not remove the group if OnGroupCompletedChan is provided
		// The developer must explicitly request group removal
		if dp.groupCompletedCh == nil {
			dp.removeGroup(groupID)
		}
	}
	dp.mu.Unlock()
}

// checkWaitingTasks checks if any waiting tasks can be submitted
func (dp *DependencyPool[T]) checkWaitingTasks(groupID interface{}) {
	// Assume dp.mu is already locked by the caller

	tasksToSubmit := []pendingTask[T]{}
	waitingTasks := dp.waitingTasks[groupID]

	fmt.Printf("Checking waiting tasks for group %v: %d tasks\n", groupID, len(waitingTasks))

	i := 0
	for i < len(waitingTasks) {
		pending := waitingTasks[i]
		data := pending.data
		// options := pending.options
		dependentTask, ok := any(data).(DependentTask)
		if !ok {
			waitingTasks = append(waitingTasks[:i], waitingTasks[i+1:]...)
			continue
		}

		dependencies := dependentTask.GetDependencies()
		allCompleted := true
		for _, depID := range dependencies {
			if state, exists := dp.groupStates[groupID][depID]; !exists || state != TaskStateCompleted {
				allCompleted = false
				break
			}
		}

		if allCompleted {
			fmt.Printf("All dependencies completed for taskID=%v, preparing to submit task\n", dependentTask.GetTaskID())
			dp.groupStates[groupID][dependentTask.GetTaskID()] = TaskStateQueued
			if dp.config.OnTaskRunning != nil {
				dp.config.OnTaskRunning(groupID, dependentTask.GetTaskID())
			}
			tasksToSubmit = append(tasksToSubmit, pending)
			waitingTasks = append(waitingTasks[:i], waitingTasks[i+1:]...)
		} else {
			i++
		}
	}

	dp.waitingTasks[groupID] = waitingTasks

	// Release dp.mu before submitting the tasks
	dp.mu.Unlock()

	// Submit the tasks
	for _, pending := range tasksToSubmit {
		err := dp.pool.Submit(pending.data, pending.options...)
		if err != nil {
			fmt.Printf("Failed to submit task %v: %v\n", pending.data, err)
			// Optionally, handle the error (e.g., retry submission, add back to waitingTasks)
		}
	}

	// Re-acquire dp.mu
	dp.mu.Lock()
}

// removeGroup removes a group from the dependency graph
func (dp *DependencyPool[T]) removeGroup(groupID interface{}) {
	dp.dependencyGraph.mu.Lock()
	delete(dp.dependencyGraph.groups, groupID)
	// Remove task dependencies associated with this group
	for taskID := range dp.groupStates[groupID] {
		delete(dp.dependencyGraph.dependencies, taskID)
	}
	dp.dependencyGraph.mu.Unlock()

	dp.mu.Lock()
	delete(dp.waitingTasks, groupID)
	delete(dp.groupStates, groupID)
	dp.mu.Unlock()
}

// handleGroupCompletedRequests handles requests to check if a group is completed
func (dp *DependencyPool[T]) handleGroupCompletedRequests() {
	for req := range dp.groupCompletedCh {
		groupID := req.Request

		dp.mu.Lock()
		groupExists := dp.groupStates[groupID] != nil

		if !groupExists {
			dp.mu.Unlock()
			req.CompleteWithError(fmt.Errorf("group %v does not exist", groupID))
			continue
		}

		// Check if all tasks in the group are completed
		allCompleted := true
		for _, taskState := range dp.groupStates[groupID] {
			if taskState != TaskStateCompleted {
				allCompleted = false
				break
			}
		}

		if allCompleted {
			dp.removeGroup(groupID)
			dp.mu.Unlock()
			req.Complete(nil)
		} else {
			dp.mu.Unlock()
			req.CompleteWithError(fmt.Errorf("group %v is not yet completed", groupID))
		}
	}
}
