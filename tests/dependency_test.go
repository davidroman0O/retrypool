package tests

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davidroman0O/retrypool"
)

// SimpleWorker represents a basic worker for testing purposes
type SimpleWorker[T any] struct {
	sync.Mutex
	ID         int
	Processed  []T
	ProcessErr error
}

func (w *SimpleWorker[T]) Run(ctx context.Context, data T) error {
	w.Lock()
	defer w.Unlock()
	fmt.Println("Worker", w.ID, "processing data:", data)
	w.Processed = append(w.Processed, data)
	return w.ProcessErr
}

// DependentTaskImpl is an implementation of the DependentTask interface for testing
type DependentTaskImpl struct {
	TaskID       string
	GroupID      string
	Dependencies []string
}

func (d *DependentTaskImpl) GetDependencies() []interface{} {
	deps := make([]interface{}, len(d.Dependencies))
	for i, dep := range d.Dependencies {
		deps[i] = dep
	}
	return deps
}

func (d *DependentTaskImpl) GetGroupID() interface{} {
	return d.GroupID
}

func (d *DependentTaskImpl) GetTaskID() interface{} {
	return d.TaskID
}

func (d *DependentTaskImpl) HashID() uint64 {
	// For simplicity in testing, we'll return a fixed value
	return 0
}

// TestDependencyPool_SubmitAndExecutionOrder tests the submission and execution order of dependent tasks
func TestDependencyPool_SubmitAndExecutionOrder(t *testing.T) {
	ctx := context.Background()
	worker := &SimpleWorker[interface{}]{}
	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{worker})

	whenCompleted := make(chan *retrypool.RequestResponse[interface{}, error])

	dpConfig := &retrypool.DependencyConfig[interface{}]{
		OnGroupCompletedChan: whenCompleted,
		OnGroupCompleted: func(groupID interface{}) {
			fmt.Printf("OnGroupCompleted called for group: %v\n", groupID)
		},
	}
	dp := retrypool.NewDependencyPool(pool, dpConfig)

	// Set up synchronization
	var wg sync.WaitGroup
	wg.Add(3) // We expect 3 tasks to be processed

	// Set up the pool's onTaskSuccess and onTaskFailure handlers
	pool.SetOnTaskSuccess(func(data interface{}) {
		fmt.Println("onTaskSuccess called with data:", data)
		dp.HandlePoolTaskSuccess(data)
		wg.Done()
	})

	pool.SetOnTaskFailure(func(data interface{}, err error) retrypool.TaskAction {
		fmt.Println("onTaskFailure called with data:", data, "error:", err)
		dp.HandlePoolTaskFailure(data, err)
		wg.Done()
		return retrypool.TaskActionRemove
	})

	// Define tasks with dependencies
	task1a := &DependentTaskImpl{
		TaskID:  "task1",
		GroupID: "group1",
	}

	task2a := &DependentTaskImpl{
		TaskID:       "task2",
		GroupID:      "group1",
		Dependencies: []string{"task1"},
	}

	task3a := &DependentTaskImpl{
		TaskID:       "task3",
		GroupID:      "group1",
		Dependencies: []string{"task2"},
	}

	// Submit tasks (order of submission should not affect execution order)
	err := dp.Submit(task3a)
	if err != nil {
		t.Fatalf("Failed to submit task3: %v", err)
	}
	err = dp.Submit(task1a)
	if err != nil {
		t.Fatalf("Failed to submit task1: %v", err)
	}
	err = dp.Submit(task2a)
	if err != nil {
		t.Fatalf("Failed to submit task2: %v", err)
	}

	// Wait for tasks to complete
	wg.Wait()

	// Safely copy the processed tasks
	worker.Lock()
	processedTasks := make([]interface{}, len(worker.Processed))
	copy(processedTasks, worker.Processed)
	worker.Unlock()

	if len(processedTasks) != 3 {
		t.Fatalf("Expected 3 tasks to be processed, got %d", len(processedTasks))
	}

	// Verify execution order based on dependencies
	expectedOrder := []string{"task1", "task2", "task3"}
	for i, processedTask := range processedTasks {
		dependentTask, ok := processedTask.(*DependentTaskImpl)
		if !ok {
			t.Fatalf("Processed task is not of type *DependentTaskImpl")
		}
		if dependentTask.TaskID != expectedOrder[i] {
			t.Errorf("Expected task ID %s at index %d, got %s", expectedOrder[i], i, dependentTask.TaskID)
		}
	}

	if err := dp.Close(); err != nil {
		t.Fatalf("Failed to close dependency pool: %v", err)
	}
}

func TestDependencyPool_SubmitAndExecutionOrderGroups(t *testing.T) {
	ctx := context.Background()
	worker := &SimpleWorker[interface{}]{}
	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{worker})

	whenCompleted := make(chan *retrypool.RequestResponse[interface{}, error])

	dpConfig := &retrypool.DependencyConfig[interface{}]{
		OnGroupCompletedChan: whenCompleted,
		OnGroupCompleted: func(groupID interface{}) {
			fmt.Printf("OnGroupCompleted called for group: %v\n", groupID)
		},
	}
	dp := retrypool.NewDependencyPool(pool, dpConfig)

	// Set up synchronization
	var wg sync.WaitGroup
	wg.Add(3) // We expect 3 tasks to be processed

	// Set up the pool's onTaskSuccess and onTaskFailure handlers
	pool.SetOnTaskSuccess(func(data interface{}) {
		fmt.Println("onTaskSuccess called with data:", data)
		dp.HandlePoolTaskSuccess(data)
		wg.Done()
	})

	pool.SetOnTaskFailure(func(data interface{}, err error) retrypool.TaskAction {
		fmt.Println("onTaskFailure called with data:", data, "error:", err)
		dp.HandlePoolTaskFailure(data, err)
		wg.Done()
		return retrypool.TaskActionRemove
	})

	// Define tasks with dependencies
	task1a := &DependentTaskImpl{
		TaskID:  "task1",
		GroupID: "group1",
	}

	task2a := &DependentTaskImpl{
		TaskID:       "task2",
		GroupID:      "group1",
		Dependencies: []string{"task1"},
	}

	task3a := &DependentTaskImpl{
		TaskID:       "task3",
		GroupID:      "group1",
		Dependencies: []string{"task2"},
	}

	task1b := &DependentTaskImpl{
		TaskID:  "task1",
		GroupID: "group2",
	}

	task2b := &DependentTaskImpl{
		TaskID:       "task2",
		GroupID:      "group2",
		Dependencies: []string{"task1"},
	}

	task3b := &DependentTaskImpl{
		TaskID:       "task3",
		GroupID:      "group2",
		Dependencies: []string{"task2"},
	}

	// Submit tasks (order of submission should not affect execution order)
	err := dp.Submit(task3a)
	if err != nil {
		t.Fatalf("Failed to submit task3: %v", err)
	}
	err = dp.Submit(task1a)
	if err != nil {
		t.Fatalf("Failed to submit task1: %v", err)
	}
	err = dp.Submit(task1b)
	if err != nil {
		t.Fatalf("Failed to submit task1: %v", err)
	}
	err = dp.Submit(task2b)
	if err != nil {
		t.Fatalf("Failed to submit task1: %v", err)
	}
	err = dp.Submit(task3b)
	if err != nil {
		t.Fatalf("Failed to submit task1: %v", err)
	}
	err = dp.Submit(task2a)
	if err != nil {
		t.Fatalf("Failed to submit task2: %v", err)
	}

	// Wait for tasks to complete
	wg.Wait()

	// Safely copy the processed tasks
	worker.Lock()
	processedTasks := make([]interface{}, len(worker.Processed))
	copy(processedTasks, worker.Processed)
	worker.Unlock()

	if len(processedTasks) != 3 {
		t.Fatalf("Expected 3 tasks to be processed, got %d", len(processedTasks))
	}

	// Verify execution order based on dependencies
	expectedOrder := []string{"task1", "task2", "task3"}
	for i, processedTask := range processedTasks {
		// dependentTask, ok := processedTask.(*DependentTaskImpl)
		// if !ok {
		// 	t.Fatalf("Processed task is not of type *DependentTaskImpl")
		// }
		// if dependentTask.TaskID != expectedOrder[i] {
		// 	t.Errorf("Expected task ID %s at index %d, got %s", expectedOrder[i], i, dependentTask.TaskID)
		// }
		fmt.Println("\tProcessed task:", processedTask, "expected:", expectedOrder[i])
	}

	if err := dp.Close(); err != nil {
		t.Fatalf("Failed to close dependency pool: %v", err)
	}
}

// TestDependencyPool_TaskFailureHandling tests how the DependencyPool handles task failures
func TestDependencyPool_TaskFailureHandling(t *testing.T) {
	ctx := context.Background()
	worker := &SimpleWorker[interface{}]{
		ProcessErr: errors.New("intentional error"),
	}
	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{worker}, retrypool.WithAttempts[interface{}](1))

	var dependencyFailureCalled bool
	dpConfig := &retrypool.DependencyConfig[interface{}]{
		OnDependencyFailure: func(groupID interface{}, taskID interface{}, dependentIDs []interface{}, reason string) retrypool.TaskAction {
			dependencyFailureCalled = true
			return retrypool.TaskActionAddToDeadTasks
		},
		OnGroupCompleted: func(groupID interface{}) {
			fmt.Printf("OnGroupCompleted called for group: %v\n", groupID)
		},
	}
	dp := retrypool.NewDependencyPool(pool, dpConfig)

	task1 := &DependentTaskImpl{
		TaskID:  "task1",
		GroupID: "group1",
	}

	task2 := &DependentTaskImpl{
		TaskID:       "task2",
		GroupID:      "group1",
		Dependencies: []string{"task1"},
	}

	err := dp.Submit(task1)
	if err != nil {
		t.Fatalf("Failed to submit task1: %v", err)
	}

	err = dp.Submit(task2)
	if err != nil {
		t.Fatalf("Failed to submit task2: %v", err)
	}

	// Wait for tasks to process
	time.Sleep(200 * time.Millisecond)

	if !dependencyFailureCalled {
		t.Errorf("Expected OnDependencyFailure to be called due to task1 failure")
	}

	worker.Lock()
	defer worker.Unlock()
	if len(worker.Processed) != 1 {
		t.Fatalf("Expected 1 task to be processed (task1), got %d", len(worker.Processed))
	}

	// Verify that task2 was not processed due to dependency on failed task1
}

// TestDependencyPool_GroupCompletion tests the OnGroupCompleted callback
func TestDependencyPool_GroupCompletion(t *testing.T) {
	ctx := context.Background()
	worker := &SimpleWorker[interface{}]{}
	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{worker})

	var groupCompleted bool
	dpConfig := &retrypool.DependencyConfig[interface{}]{
		OnGroupCompleted: func(groupID interface{}) {
			groupCompleted = true
			if groupID != "group1" {
				t.Errorf("Expected group1, got %v", groupID)
			}
		},
	}
	dp := retrypool.NewDependencyPool(pool, dpConfig)

	task1 := &DependentTaskImpl{
		TaskID:  "task1",
		GroupID: "group1",
	}

	task2 := &DependentTaskImpl{
		TaskID:  "task2",
		GroupID: "group1",
	}

	err := dp.Submit(task1)
	if err != nil {
		t.Fatalf("Failed to submit task1: %v", err)
	}

	err = dp.Submit(task2)
	if err != nil {
		t.Fatalf("Failed to submit task2: %v", err)
	}

	// Wait for tasks to process
	time.Sleep(100 * time.Millisecond)

	if !groupCompleted {
		t.Errorf("Expected OnGroupCompleted to be called")
	}

	if err := dp.Close(); err != nil {
		t.Fatalf("Failed to close dependency pool: %v", err)
	}
}

// TestDependencyPool_WaitingTasks tests that tasks wait for their dependencies
func TestDependencyPool_WaitingTasks(t *testing.T) {
	ctx := context.Background()
	worker := &SimpleWorker[interface{}]{}
	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{worker})

	dpConfig := &retrypool.DependencyConfig[interface{}]{}
	dp := retrypool.NewDependencyPool(pool, dpConfig)

	task1 := &DependentTaskImpl{
		TaskID:  "task1",
		GroupID: "group1",
	}

	task2 := &DependentTaskImpl{
		TaskID:       "task2",
		GroupID:      "group1",
		Dependencies: []string{"task1"},
	}

	err := dp.Submit(task2)
	if err != nil {
		t.Fatalf("Failed to submit task2: %v", err)
	}

	// Wait a bit to see if task2 is incorrectly processed before task1
	time.Sleep(50 * time.Millisecond)

	worker.Lock()
	if len(worker.Processed) != 0 {
		t.Fatalf("Expected no tasks to be processed yet, got %d", len(worker.Processed))
	}
	worker.Unlock()

	// Now submit task1
	err = dp.Submit(task1)
	if err != nil {
		t.Fatalf("Failed to submit task1: %v", err)
	}

	// Wait for tasks to process
	time.Sleep(100 * time.Millisecond)

	worker.Lock()
	defer worker.Unlock()
	if len(worker.Processed) != 2 {
		t.Fatalf("Expected 2 tasks to be processed, got %d", len(worker.Processed))
	}

	// Verify the order
	taskIDs := []string{
		worker.Processed[0].(*DependentTaskImpl).TaskID,
		worker.Processed[1].(*DependentTaskImpl).TaskID,
	}
	if taskIDs[0] != "task1" || taskIDs[1] != "task2" {
		t.Errorf("Tasks processed in wrong order: %v", taskIDs)
	}
}

func TestDependencyPool_DynamicWorkerScaling(t *testing.T) {
	ctx := context.Background()
	var workerCreationCount int32

	// Worker factory that counts the number of workers created
	workerFactory := func() retrypool.Worker[interface{}] {
		atomic.AddInt32(&workerCreationCount, 1)
		return &SimpleWorker[interface{}]{}
	}

	// Initialize the pool with one worker
	initialWorkers := 1
	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{&SimpleWorker[interface{}]{}}, retrypool.WithLogger[interface{}](retrypool.NewLogger(slog.LevelInfo)))

	dpConfig := &retrypool.DependencyConfig[interface{}]{
		AutoCreateWorkers: true,
		MaxDynamicWorkers: 2,
		WorkerFactory:     workerFactory,
	}

	dp := retrypool.NewDependencyPool(pool, dpConfig)

	// Number of tasks to submit
	taskCount := 5
	var wg sync.WaitGroup
	wg.Add(taskCount)

	// Channel to track tasks being processed
	var processedTasks int32

	// Set up the pool's onTaskSuccess handler
	pool.SetOnTaskSuccess(func(data interface{}) {
		atomic.AddInt32(&processedTasks, 1)
		wg.Done()
	})

	// Submit tasks
	for i := 0; i < taskCount; i++ {
		task := &DependentTaskImpl{
			TaskID:  fmt.Sprintf("task%d", i),
			GroupID: fmt.Sprintf("group%d", i%2), // Alternate between two groups
		}
		err := dp.Submit(task)
		if err != nil {
			t.Fatalf("Failed to submit task %d: %v", i, err)
		}
	}

	// Wait for all tasks to be processed
	wg.Wait()

	// Verify that dynamic workers were added
	expectedTotalWorkers := initialWorkers + dpConfig.MaxDynamicWorkers
	actualWorkers := int(atomic.LoadInt32(&workerCreationCount)) + initialWorkers
	if actualWorkers != expectedTotalWorkers {
		t.Errorf("Expected total workers to be %d, but got %d", expectedTotalWorkers, actualWorkers)
	}

	// Verify that all tasks were processed
	if int(atomic.LoadInt32(&processedTasks)) != taskCount {
		t.Errorf("Expected %d tasks to be processed, but got %d", taskCount, processedTasks)
	}

	// Close the pool
	if err := dp.Close(); err != nil {
		t.Fatalf("Failed to close dependency pool: %v", err)
	}
}

func TestDependencyPool_TaskProcessingWithDynamicWorkers(t *testing.T) {
	ctx := context.Background()
	var workerCreationCount int32

	workerFactory := func() retrypool.Worker[interface{}] {
		id := atomic.AddInt32(&workerCreationCount, 1)
		return &SimpleWorker[interface{}]{ID: int(id)}
	}

	// Initialize the pool with initialWorkers
	initialWorkers := 2
	initialWorkerList := make([]retrypool.Worker[interface{}], initialWorkers)
	for i := 0; i < initialWorkers; i++ {
		initialWorkerList[i] = &SimpleWorker[interface{}]{ID: i}
	}
	pool := retrypool.New(ctx, initialWorkerList, retrypool.WithLogger[interface{}](retrypool.NewLogger(slog.LevelInfo)))

	dpConfig := &retrypool.DependencyConfig[interface{}]{
		AutoCreateWorkers: true,
		MaxDynamicWorkers: 3,
		WorkerFactory:     workerFactory,
	}

	dp := retrypool.NewDependencyPool(pool, dpConfig)

	// Number of tasks to submit
	taskCount := 10
	var wg sync.WaitGroup
	wg.Add(taskCount)

	// Map to store processed tasks
	var processedTasks sync.Map

	// Set up the pool's onTaskSuccess handler
	pool.SetOnTaskSuccess(func(data interface{}) {
		if dt, ok := data.(*DependentTaskImpl); ok {
			processedTasks.Store(dt.TaskID, true)
		}
		wg.Done()
	})

	// Submit tasks
	for i := 0; i < taskCount; i++ {
		task := &DependentTaskImpl{
			TaskID:  fmt.Sprintf("task%d", i),
			GroupID: "group1",
		}
		err := dp.Submit(task)
		if err != nil {
			t.Fatalf("Failed to submit task %d: %v", i, err)
		}
	}

	// Wait for all tasks to be processed
	wg.Wait()

	// Verify that all tasks were processed
	for i := 0; i < taskCount; i++ {
		taskID := fmt.Sprintf("task%d", i)
		if _, ok := processedTasks.Load(taskID); !ok {
			t.Errorf("Task %s was not processed", taskID)
		}
	}

	// Verify that dynamic workers were added
	totalWorkers := initialWorkers + int(workerCreationCount)
	expectedMaxWorkers := initialWorkers + dpConfig.MaxDynamicWorkers
	if totalWorkers > expectedMaxWorkers {
		t.Errorf("Expected total workers to be at most %d, but got %d", expectedMaxWorkers, totalWorkers)
	}

	// Close the pool
	if err := dp.Close(); err != nil {
		t.Fatalf("Failed to close dependency pool: %v", err)
	}
}

func TestDependencyPool_GroupPrioritization(t *testing.T) {
	ctx := context.Background()
	worker := &SimpleWorker[interface{}]{}
	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{worker}, retrypool.WithLogger[interface{}](retrypool.NewLogger(slog.LevelInfo)))

	dpConfig := &retrypool.DependencyConfig[interface{}]{
		PrioritizeStartedGroups: true,
	}

	dp := retrypool.NewDependencyPool(pool, dpConfig)

	// Variables to track task execution order
	var mu sync.Mutex
	var executionOrder []string

	// Set up the pool's onTaskSuccess handler
	pool.SetOnTaskSuccess(func(data interface{}) {
		if dt, ok := data.(*DependentTaskImpl); ok {
			mu.Lock()
			executionOrder = append(executionOrder, dt.TaskID)
			mu.Unlock()
		}
	})

	// Submit tasks from Group 1
	taskCountGroup1 := 5
	for i := 0; i < taskCountGroup1; i++ {
		task := &DependentTaskImpl{
			TaskID:  fmt.Sprintf("G1_task%d", i),
			GroupID: "group1",
		}
		err := dp.Submit(task)
		if err != nil {
			t.Fatalf("Failed to submit task %s: %v", task.TaskID, err)
		}
	}

	// Simulate a slight delay before submitting tasks from Group 2
	time.Sleep(50 * time.Millisecond)

	// Submit tasks from Group 2
	taskCountGroup2 := 5
	for i := 0; i < taskCountGroup2; i++ {
		task := &DependentTaskImpl{
			TaskID:  fmt.Sprintf("G2_task%d", i),
			GroupID: "group2",
		}
		err := dp.Submit(task)
		if err != nil {
			t.Fatalf("Failed to submit task %s: %v", task.TaskID, err)
		}
	}

	// Wait for all tasks to be processed
	totalTaskCount := taskCountGroup1 + taskCountGroup2
	for {
		mu.Lock()
		if len(executionOrder) == totalTaskCount {
			mu.Unlock()
			break
		}
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}

	// Verify that tasks from Group 1 were processed before Group 2
	mu.Lock()
	defer mu.Unlock()
	group1Completed := false
	for _, taskID := range executionOrder {
		if !group1Completed && taskID[:2] == "G2" {
			group1Completed = true
			t.Errorf("Task from Group 2 processed before Group 1 completed")
		}
		if group1Completed && taskID[:2] == "G1" {
			t.Errorf("Task from Group 1 processed after Group 2 started")
		}
	}

	// Close the pool
	if err := dp.Close(); err != nil {
		t.Fatalf("Failed to close dependency pool: %v", err)
	}
}
