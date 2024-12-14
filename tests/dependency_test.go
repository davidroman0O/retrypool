package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	task1 := &DependentTaskImpl{
		TaskID:  "task1",
		GroupID: "group1",
	}

	task2 := &DependentTaskImpl{
		TaskID:       "task2",
		GroupID:      "group1",
		Dependencies: []string{"task1"},
	}

	task3 := &DependentTaskImpl{
		TaskID:       "task3",
		GroupID:      "group1",
		Dependencies: []string{"task2"},
	}

	// Submit tasks (order of submission should not affect execution order)
	err := dp.Submit(task3)
	if err != nil {
		t.Fatalf("Failed to submit task3: %v", err)
	}
	err = dp.Submit(task1)
	if err != nil {
		t.Fatalf("Failed to submit task1: %v", err)
	}
	err = dp.Submit(task2)
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
