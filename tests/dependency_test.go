package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davidroman0O/retrypool"
)

// SimpleWorker represents a basic worker for testing
type SimpleWorker[T any] struct {
	sync.Mutex
	ID           int
	Processed    []T
	ProcessErr   error
	ProcessDelay time.Duration
}

func (w *SimpleWorker[T]) Run(ctx context.Context, data T) error {
	w.Lock()
	defer w.Unlock()
	if w.ProcessDelay > 0 {
		time.Sleep(w.ProcessDelay)
	}
	if w.ProcessErr != nil {
		return w.ProcessErr
	}
	fmt.Printf("Worker %d processing data: %v\n", w.ID, data)
	w.Processed = append(w.Processed, data)
	return nil
}

// DependentTaskImpl implements DependentTask for testing
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

func (d *DependentTaskImpl) GetGroupID() interface{} { return d.GroupID }
func (d *DependentTaskImpl) GetTaskID() interface{}  { return d.TaskID }
func (d *DependentTaskImpl) HashID() uint64          { return 0 }

// TestDependencyPool_Configuration tests configuration validation
func TestDependencyPool_Configuration(t *testing.T) {
	ctx := context.Background()
	worker := &SimpleWorker[interface{}]{}
	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{worker}, retrypool.WithRoundRobinDistribution[interface{}]())

	tests := []struct {
		name        string
		config      *retrypool.DependencyConfig[interface{}]
		expectError bool
	}{
		{
			name: "Valid configuration",
			config: &retrypool.DependencyConfig[interface{}]{
				EqualsTaskID:  func(a, b interface{}) bool { return a == b },
				EqualsGroupID: func(a, b interface{}) bool { return a == b },
			},
			expectError: false,
		},
		{
			name: "Missing EqualsTaskID",
			config: &retrypool.DependencyConfig[interface{}]{
				EqualsGroupID: func(a, b interface{}) bool { return a == b },
			},
			expectError: true,
		},
		{
			name: "Missing EqualsGroupID",
			config: &retrypool.DependencyConfig[interface{}]{
				EqualsTaskID: func(a, b interface{}) bool { return a == b },
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := retrypool.NewDependencyPool(pool, tt.config)
			if (err != nil) != tt.expectError {
				t.Errorf("NewDependencyPool() error = %v, expectError %v", err, tt.expectError)
			}
		})
	}
}

// TestDependencyPool_DependencyOrder tests basic dependency ordering
func TestDependencyPool_DependencyOrder(t *testing.T) {
	ctx := context.Background()
	worker := &SimpleWorker[interface{}]{}
	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{worker}, retrypool.WithRoundRobinDistribution[interface{}]())

	var executionOrder []string
	var executionMu sync.Mutex
	var wg sync.WaitGroup

	config := &retrypool.DependencyConfig[interface{}]{
		EqualsTaskID:  func(a, b interface{}) bool { return a == b },
		EqualsGroupID: func(a, b interface{}) bool { return a == b },
		Strategy:      retrypool.DependencyStrategyOrder,
		OnTaskProcessed: func(groupID, taskID interface{}) {
			executionMu.Lock()
			executionOrder = append(executionOrder, taskID.(string))
			executionMu.Unlock()
			wg.Done()
		},
	}

	dp, err := retrypool.NewDependencyPool(pool, config)
	if err != nil {
		t.Fatalf("Failed to create dependency pool: %v", err)
	}

	// Create a dependency chain: A -> B -> C
	tasks := []*DependentTaskImpl{
		{TaskID: "C", GroupID: "group1", Dependencies: []string{"B"}},
		{TaskID: "A", GroupID: "group1"},
		{TaskID: "B", GroupID: "group1", Dependencies: []string{"A"}},
	}

	wg.Add(len(tasks))
	for _, task := range tasks {
		if err := dp.Submit(task); err != nil {
			t.Fatalf("Failed to submit task %s: %v", task.TaskID, err)
		}
	}

	wg.Wait()

	// Verify order
	expected := []string{"A", "B", "C"}
	executionMu.Lock()
	if !sliceEqual(executionOrder, expected) {
		t.Errorf("Wrong execution order. Expected %v, got %v", expected, executionOrder)
	}
	executionMu.Unlock()
}

// TestDependencyPool_GroupPrioritization tests group prioritization
func TestDependencyPool_GroupPrioritization(t *testing.T) {
	ctx := context.Background()
	worker := &SimpleWorker[interface{}]{ProcessDelay: 10 * time.Millisecond}
	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{worker}, retrypool.WithRoundRobinDistribution[interface{}]())

	var executionOrder []string
	var executionMu sync.Mutex
	var wg sync.WaitGroup

	config := &retrypool.DependencyConfig[interface{}]{
		EqualsTaskID:  func(a, b interface{}) bool { return a == b },
		EqualsGroupID: func(a, b interface{}) bool { return a == b },
		Strategy:      retrypool.DependencyStrategyPriority,
		OnTaskProcessed: func(groupID, taskID interface{}) {
			executionMu.Lock()
			executionOrder = append(executionOrder, fmt.Sprintf("%s:%s", groupID, taskID))
			executionMu.Unlock()
			wg.Done()
		},
	}

	dp, err := retrypool.NewDependencyPool(pool, config)
	if err != nil {
		t.Fatalf("Failed to create dependency pool: %v", err)
	}

	// Create tasks in two groups
	tasksGroup1 := []*DependentTaskImpl{
		{TaskID: "1A", GroupID: "group1"},
		{TaskID: "1B", GroupID: "group1"},
	}

	tasksGroup2 := []*DependentTaskImpl{
		{TaskID: "2A", GroupID: "group2"},
		{TaskID: "2B", GroupID: "group2"},
	}

	// Submit all tasks
	wg.Add(len(tasksGroup1) + len(tasksGroup2))

	// Submit group1 tasks first
	for _, task := range tasksGroup1 {
		if err := dp.Submit(task); err != nil {
			t.Fatalf("Failed to submit task %s: %v", task.TaskID, err)
		}
	}

	// Small delay to ensure group1 starts processing
	time.Sleep(5 * time.Millisecond)

	// Submit group2 tasks
	for _, task := range tasksGroup2 {
		if err := dp.Submit(task); err != nil {
			t.Fatalf("Failed to submit task %s: %v", task.TaskID, err)
		}
	}

	wg.Wait()

	// Verify all group1 tasks were processed before group2 tasks
	executionMu.Lock()
	var group1Done bool
	var group2Started bool
	for _, execution := range executionOrder {
		if !group1Done && execution[:6] == "group2" {
			group2Started = true
		}
		if group2Started && execution[:6] == "group1" {
			t.Error("Group 2 task executed before Group 1 was complete")
		}
	}
	executionMu.Unlock()
}

// TestDependencyPool_FailureHandling tests how failures are handled
func TestDependencyPool_FailureHandling(t *testing.T) {
	ctx := context.Background()
	worker := &SimpleWorker[interface{}]{}
	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{worker}, retrypool.WithRoundRobinDistribution[interface{}]())

	var failureHandled bool
	var failureMu sync.Mutex
	var wg sync.WaitGroup

	config := &retrypool.DependencyConfig[interface{}]{
		EqualsTaskID:  func(a, b interface{}) bool { return a == b },
		EqualsGroupID: func(a, b interface{}) bool { return a == b },
		OnDependencyFailure: func(groupID, taskID interface{}, deps []interface{}, reason string) retrypool.TaskAction {
			failureMu.Lock()
			failureHandled = true
			failureMu.Unlock()
			return retrypool.TaskActionAddToDeadTasks
		},
	}

	dp, err := retrypool.NewDependencyPool(pool, config)
	if err != nil {
		t.Fatalf("Failed to create dependency pool: %v", err)
	}

	// Create a failing task
	worker.ProcessErr = errors.New("simulated failure")
	task := &DependentTaskImpl{
		TaskID:  "failing",
		GroupID: "group1",
	}

	wg.Add(1)
	if err := dp.Submit(task); err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	// Wait for failure handling
	time.Sleep(100 * time.Millisecond)

	failureMu.Lock()
	if !failureHandled {
		t.Error("Failure was not handled")
	}
	failureMu.Unlock()
}

// TestDependencyPool_DynamicWorkers tests dynamic worker creation
func TestDependencyPool_DynamicWorkers(t *testing.T) {
	ctx := context.Background()
	var workerCount atomic.Int32

	workerFactory := func() retrypool.Worker[interface{}] {
		id := workerCount.Add(1)
		return &SimpleWorker[interface{}]{ID: int(id)}
	}

	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{workerFactory()})

	config := &retrypool.DependencyConfig[interface{}]{
		EqualsTaskID:      func(a, b interface{}) bool { return a == b },
		EqualsGroupID:     func(a, b interface{}) bool { return a == b },
		AutoCreateWorkers: true,
		MaxDynamicWorkers: 3,
		WorkerFactory:     workerFactory,
	}

	dp, err := retrypool.NewDependencyPool(pool, config)
	if err != nil {
		t.Fatalf("Failed to create dependency pool: %v", err)
	}

	// Submit enough tasks to trigger worker creation
	for i := 0; i < 10; i++ {
		task := &DependentTaskImpl{
			TaskID:  fmt.Sprintf("task%d", i),
			GroupID: "group1",
		}
		if err := dp.Submit(task); err != nil {
			t.Fatalf("Failed to submit task: %v", err)
		}
	}

	// Wait for worker creation
	time.Sleep(100 * time.Millisecond)

	finalCount := workerCount.Load()
	if finalCount > int32(config.MaxDynamicWorkers)+1 {
		t.Errorf("Too many workers created. Got %d, expected <= %d",
			finalCount, config.MaxDynamicWorkers+1)
	}
}

// Helper function to compare slices
func sliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
