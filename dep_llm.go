package retrypool

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sasha-s/go-deadlock"
)

type DependencyMode int
type TaskOrderMode int

const (
	DependencyOrdered DependencyMode = iota
	DependencyReversed
)

const (
	TaskOrderOrdered TaskOrderMode = iota
	TaskOrderReversed
)

type DependencyConfig[T any, GID comparable, TID comparable] struct {
	DependencyMode DependencyMode
	TaskOrderMode  TaskOrderMode
	MinWorkers     int
	MaxWorkers     int
	ScaleUpRatio   float64
}

type DependentTask[GID comparable, TID comparable] interface {
	GetDependencies() []TID
	GetGroupID() GID
	GetTaskID() TID
}

type taskGroup[T any, GID comparable, TID comparable] struct {
	id        GID
	tasks     map[TID]*taskState[T, GID, TID]
	completed map[TID]bool
	order     []TID
	blocked   map[TID][]TID
	mu        deadlock.RWMutex
}

type taskState[T any, GID comparable, TID comparable] struct {
	task         T
	taskID       TID
	groupID      GID
	dependencies []TID
	submitted    bool
	completed    bool
	completionCh chan struct{}
	parentTask   TID
}

type DependencyPool[T any, GID comparable, TID comparable] struct {
	pooler        Pooler[T]
	workerFactory WorkerFactory[T]
	config        DependencyConfig[T, GID, TID]

	mu          deadlock.RWMutex
	taskGroups  map[GID]*taskGroup[T, GID, TID]
	workerCount int
}

func NewDependencyPool[T any, GID comparable, TID comparable](
	pooler Pooler[T],
	workerFactory WorkerFactory[T],
	config DependencyConfig[T, GID, TID],
) (*DependencyPool[T, GID, TID], error) {
	if config.MinWorkers <= 0 {
		config.MinWorkers = 1
	}
	if config.MaxWorkers <= 0 {
		config.MaxWorkers = 100
	}
	if config.ScaleUpRatio <= 0 {
		config.ScaleUpRatio = 2.0
	}

	dp := &DependencyPool[T, GID, TID]{
		pooler:        pooler,
		workerFactory: workerFactory,
		config:        config,
		taskGroups:    make(map[GID]*taskGroup[T, GID, TID]),
	}

	for i := 0; i < config.MinWorkers; i++ {
		worker := workerFactory()
		if err := pooler.Add(worker, nil); err != nil {
			return nil, err
		}
		dp.workerCount++
	}

	pooler.SetOnTaskSuccess(dp.handleTaskCompletion)
	return dp, nil
}

func (dp *DependencyPool[T, GID, TID]) Submit(data T) error {
	dtask, ok := any(data).(DependentTask[GID, TID])
	if !ok {
		return errors.New("data does not implement DependentTask interface")
	}

	dp.mu.Lock()
	defer dp.mu.Unlock()

	groupID := dtask.GetGroupID()
	taskID := dtask.GetTaskID()
	deps := dtask.GetDependencies()

	if err := dp.scaleWorkersIfNeeded(); err != nil {
		return fmt.Errorf("worker scaling failed: %w", err)
	}

	group, exists := dp.taskGroups[groupID]
	if !exists {
		group = &taskGroup[T, GID, TID]{
			id:        groupID,
			tasks:     make(map[TID]*taskState[T, GID, TID]),
			completed: make(map[TID]bool),
			blocked:   make(map[TID][]TID),
			order:     make([]TID, 0),
		}
		dp.taskGroups[groupID] = group
	}

	task := &taskState[T, GID, TID]{
		task:         data,
		taskID:       taskID,
		groupID:      groupID,
		dependencies: deps,
		completionCh: make(chan struct{}),
	}

	group.mu.Lock()
	group.tasks[taskID] = task
	group.order = append(group.order, taskID)

	// In reversed mode, update blocked tasks
	if dp.config.DependencyMode == DependencyReversed {
		for _, depID := range deps {
			group.blocked[depID] = append(group.blocked[depID], taskID)
		}
	}
	group.mu.Unlock()

	if dp.canSubmitTask(group, task) {
		fmt.Printf("Submitting task %v\n", taskID)
		return dp.submitTask(task)
	}

	fmt.Printf("Task %v stored (waiting for deps: %v)\n", taskID, deps)
	return nil
}

func (dp *DependencyPool[T, GID, TID]) canSubmitTask(group *taskGroup[T, GID, TID], task *taskState[T, GID, TID]) bool {
	if task.submitted || task.completed {
		return false
	}

	group.mu.RLock()
	defer group.mu.RUnlock()

	// Different dependency checking based on mode
	if dp.config.DependencyMode == DependencyOrdered {
		// Normal dependency checking - task runs when deps complete
		for _, depID := range task.dependencies {
			if !group.completed[depID] {
				return false
			}
		}
	} else {
		// Reversed dependency checking - task runs if it blocks nothing
		if blockedTasks, exists := group.blocked[task.taskID]; exists && len(blockedTasks) > 0 {
			return false
		}
	}

	return true
}

func (dp *DependencyPool[T, GID, TID]) handleTaskCompletion(data T) {
	dtask := any(data).(DependentTask[GID, TID])

	dp.mu.Lock()
	defer dp.mu.Unlock()

	groupID := dtask.GetGroupID()
	taskID := dtask.GetTaskID()

	group, exists := dp.taskGroups[groupID]
	if !exists {
		return
	}

	group.mu.Lock()
	group.completed[taskID] = true
	task := group.tasks[taskID]
	if task != nil {
		task.completed = true
		close(task.completionCh)

		// In reversed mode, remove this task from blocking others
		if dp.config.DependencyMode == DependencyReversed {
			delete(group.blocked, taskID)
		}
	}
	group.mu.Unlock()

	// Process pending tasks based on configured order
	if dp.config.TaskOrderMode == TaskOrderOrdered {
		dp.processOrderedTasks(group)
	} else {
		dp.processReversedTasks(group)
	}
}

func (dp *DependencyPool[T, GID, TID]) processOrderedTasks(group *taskGroup[T, GID, TID]) {
	group.mu.RLock()
	defer group.mu.RUnlock()

	for _, taskID := range group.order {
		task := group.tasks[taskID]
		if dp.canSubmitTask(group, task) {
			dp.submitTask(task)
		}
	}
}

func (dp *DependencyPool[T, GID, TID]) processReversedTasks(group *taskGroup[T, GID, TID]) {
	group.mu.RLock()
	defer group.mu.RUnlock()

	for i := len(group.order) - 1; i >= 0; i-- {
		taskID := group.order[i]
		task := group.tasks[taskID]
		if dp.canSubmitTask(group, task) {
			dp.submitTask(task)
		}
	}
}

func (dp *DependencyPool[T, GID, TID]) submitTask(task *taskState[T, GID, TID]) error {
	if task.submitted {
		return nil
	}

	task.submitted = true
	if err := dp.pooler.Submit(task.task); err != nil {
		task.submitted = false
		return err
	}

	return nil
}

func (dp *DependencyPool[T, GID, TID]) scaleWorkersIfNeeded() error {
	currentWorkers, err := dp.pooler.Workers()
	if err != nil {
		return err
	}

	totalTasks := int64(0)
	dp.pooler.RangeWorkerQueues(func(workerID int, queueSize int64) bool {
		totalTasks += queueSize
		return true
	})
	totalTasks += dp.pooler.ProcessingCount()

	desiredWorkers := int(float64(totalTasks) / dp.config.ScaleUpRatio)
	if desiredWorkers < dp.config.MinWorkers {
		desiredWorkers = dp.config.MinWorkers
	}
	if desiredWorkers > dp.config.MaxWorkers {
		desiredWorkers = dp.config.MaxWorkers
	}

	for i := len(currentWorkers); i < desiredWorkers; i++ {
		worker := dp.workerFactory()
		if err := dp.pooler.Add(worker, nil); err != nil {
			return err
		}
		dp.workerCount++
	}

	return nil
}

func (dp *DependencyPool[T, GID, TID]) WaitForTask(groupID GID, taskID TID) error {
	dp.mu.RLock()
	group, exists := dp.taskGroups[groupID]
	if !exists {
		dp.mu.RUnlock()
		return fmt.Errorf("group %v not found", groupID)
	}

	group.mu.RLock()
	task, exists := group.tasks[taskID]
	if !exists {
		group.mu.RUnlock()
		dp.mu.RUnlock()
		return fmt.Errorf("task %v not found", taskID)
	}
	completionCh := task.completionCh
	group.mu.RUnlock()
	dp.mu.RUnlock()

	<-completionCh
	return nil
}

func (dp *DependencyPool[T, GID, TID]) WaitWithCallback(ctx context.Context,
	callback func(queueSize, processingCount, deadTaskCount int) bool,
	interval time.Duration) error {
	return dp.pooler.WaitWithCallback(ctx, callback, interval)
}

func (dp *DependencyPool[T, GID, TID]) Close() error {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	dp.taskGroups = make(map[GID]*taskGroup[T, GID, TID])
	return dp.pooler.Close()
}

func (dp *DependencyPool[T, GID, TID]) GetWorkerCount() int {
	dp.mu.RLock()
	defer dp.mu.RUnlock()
	return dp.workerCount
}

func (dp *DependencyPool[T, GID, TID]) ScaleTo(count int) error {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	if count < dp.config.MinWorkers {
		count = dp.config.MinWorkers
	}
	if count > dp.config.MaxWorkers {
		count = dp.config.MaxWorkers
	}

	currentWorkers, err := dp.pooler.Workers()
	if err != nil {
		return err
	}

	for i := len(currentWorkers); i > count; i-- {
		if err := dp.pooler.Remove(currentWorkers[i-1]); err != nil {
			return err
		}
		dp.workerCount--
	}

	for i := len(currentWorkers); i < count; i++ {
		worker := dp.workerFactory()
		if err := dp.pooler.Add(worker, nil); err != nil {
			return err
		}
		dp.workerCount++
	}

	return nil
}
