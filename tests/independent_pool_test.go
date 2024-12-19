package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"math/rand"

	"github.com/davidroman0O/retrypool"
)

// TestTask implements DependentTask for testing
type TestTask struct {
	ID           int
	GroupID      string
	Dependencies []int
	ExecuteFunc  func() error
}

func (t TestTask) GetDependencies() []int { return t.Dependencies }
func (t TestTask) GetGroupID() string     { return t.GroupID }
func (t TestTask) GetTaskID() int         { return t.ID }

// IndependentTestWorker implements Worker for testing
type IndependentTestWorker struct {
	executionTimes map[int]time.Time
	mu             sync.Mutex
}

func (w *IndependentTestWorker) Run(ctx context.Context, task TestTask) error {
	w.mu.Lock()
	w.executionTimes[task.ID] = time.Now()
	w.mu.Unlock()

	if task.ExecuteFunc != nil {
		return task.ExecuteFunc()
	}
	return nil
}

func TestIndependentPool_Basic(t *testing.T) {
	ctx := context.Background()
	worker := &IndependentTestWorker{executionTimes: make(map[int]time.Time)}

	pool, err := retrypool.NewIndependentPool[TestTask, string, int](
		ctx,
		retrypool.WithWorkerFactory(func() retrypool.Worker[TestTask] { return worker }),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Simple task without dependencies
	task := TestTask{
		ID:      1,
		GroupID: "group1",
	}

	if err := pool.Submit(task); err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	// Wait for completion
	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 100*time.Millisecond)

	if err != nil {
		t.Fatalf("Error waiting for tasks: %v", err)
	}

	// Verify task was executed
	if _, exists := worker.executionTimes[1]; !exists {
		t.Error("Task was not executed")
	}
}

func TestIndependentPool_Dependencies(t *testing.T) {
	ctx := context.Background()
	worker := &IndependentTestWorker{executionTimes: make(map[int]time.Time)}

	pool, err := retrypool.NewIndependentPool[TestTask, string, int](
		ctx,
		retrypool.WithWorkerFactory(func() retrypool.Worker[TestTask] { return worker }),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Create tasks with dependencies: 3 depends on 2, which depends on 1
	tasks := []TestTask{
		{ID: 3, GroupID: "group1", Dependencies: []int{2}},
		{ID: 1, GroupID: "group1", Dependencies: []int{}},
		{ID: 2, GroupID: "group1", Dependencies: []int{1}},
	}

	for _, task := range tasks {
		if err := pool.Submit(task); err != nil {
			t.Fatalf("Failed to submit task %d: %v", task.ID, err)
		}
	}

	// Wait for completion
	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 100*time.Millisecond)

	if err != nil {
		t.Fatalf("Error waiting for tasks: %v", err)
	}

	// Verify execution order
	time1 := worker.executionTimes[1]
	time2 := worker.executionTimes[2]
	time3 := worker.executionTimes[3]

	if time2.Before(time1) {
		t.Error("Task 2 executed before its dependency (Task 1)")
	}
	if time3.Before(time2) {
		t.Error("Task 3 executed before its dependency (Task 2)")
	}
}

func TestIndependentPool_MultipleGroups(t *testing.T) {
	ctx := context.Background()
	worker := &IndependentTestWorker{executionTimes: make(map[int]time.Time)}

	pool, err := retrypool.NewIndependentPool[TestTask, string, int](
		ctx,
		retrypool.WithWorkerFactory(func() retrypool.Worker[TestTask] { return worker }),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Create tasks in different groups
	tasks := []TestTask{
		{ID: 1, GroupID: "group1", Dependencies: []int{}},
		{ID: 2, GroupID: "group2", Dependencies: []int{}},
		{ID: 3, GroupID: "group1", Dependencies: []int{1}},
		{ID: 4, GroupID: "group2", Dependencies: []int{2}},
	}

	for _, task := range tasks {
		if err := pool.Submit(task); err != nil {
			t.Fatalf("Failed to submit task %d: %v", task.ID, err)
		}
	}

	// Wait for completion
	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 100*time.Millisecond)

	if err != nil {
		t.Fatalf("Error waiting for tasks: %v", err)
	}

	// Verify execution order within groups
	if worker.executionTimes[3].Before(worker.executionTimes[1]) {
		t.Error("Task 3 executed before its dependency (Task 1) in group1")
	}
	if worker.executionTimes[4].Before(worker.executionTimes[2]) {
		t.Error("Task 4 executed before its dependency (Task 2) in group2")
	}
}

func TestIndependentPool_ErrorHandling(t *testing.T) {
	ctx := context.Background()
	worker := &IndependentTestWorker{executionTimes: make(map[int]time.Time)}

	pool, err := retrypool.NewIndependentPool[TestTask, string, int](
		ctx,
		retrypool.WithWorkerFactory(func() retrypool.Worker[TestTask] { return worker }),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	errTask := fmt.Errorf("task error")
	task := TestTask{
		ID:      1,
		GroupID: "group1",
		ExecuteFunc: func() error {
			return errTask
		},
	}

	if err := pool.Submit(task); err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	// Wait for completion
	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 100*time.Millisecond)

	if err != nil {
		t.Fatalf("Error waiting for tasks: %v", err)
	}
}

func TestIndependentPool_StressTest(t *testing.T) {
	ctx := context.Background()
	worker := &IndependentTestWorker{executionTimes: make(map[int]time.Time)}

	numWorkers := 10
	numTasksPerGroup := 100
	numGroups := 5

	pool, err := retrypool.NewIndependentPool[TestTask, string, int](
		ctx,
		retrypool.WithWorkerFactory(func() retrypool.Worker[TestTask] { return worker }),
		retrypool.WithWorkerLimits[TestTask](numWorkers, numWorkers),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	var wg sync.WaitGroup
	errorChan := make(chan error, numGroups*numTasksPerGroup)

	// Submit tasks across multiple groups with complex dependencies
	for g := 0; g < numGroups; g++ {
		groupID := fmt.Sprintf("group%d", g)
		wg.Add(1)

		go func(gID string) {
			defer wg.Done()

			// Create tasks with chain dependencies
			for i := 0; i < numTasksPerGroup; i++ {
				taskID := i
				var deps []int
				if i > 0 {
					// Each task depends on the previous 1-3 tasks
					for d := 1; d <= min(3, i); d++ {
						deps = append(deps, taskID-d)
					}
				}

				task := TestTask{
					ID:           taskID,
					GroupID:      gID,
					Dependencies: deps,
					ExecuteFunc: func() error {
						// Simulate some work
						time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))
						return nil
					},
				}

				if err := pool.Submit(task); err != nil {
					errorChan <- fmt.Errorf("failed to submit task %d in group %s: %v", taskID, gID, err)
				}
			}
		}(groupID)
	}

	// Wait for all submissions to complete
	wg.Wait()
	close(errorChan)

	// Check for submission errors
	for err := range errorChan {
		t.Errorf("Submission error: %v", err)
	}

	// Wait for all tasks to complete
	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 100*time.Millisecond)

	if err != nil {
		t.Fatalf("Error waiting for tasks: %v", err)
	}

	// Verify execution order for each group
	worker.mu.Lock()
	defer worker.mu.Unlock()

	for g := 0; g < numGroups; g++ {
		groupID := fmt.Sprintf("group%d", g)
		for i := 0; i < numTasksPerGroup; i++ {
			taskTime, exists := worker.executionTimes[i]
			if !exists {
				t.Errorf("Task %d in group %s was not executed", i, groupID)
				continue
			}

			// Verify dependency order
			if i > 0 {
				for d := 1; d <= min(3, i); d++ {
					depTime, exists := worker.executionTimes[i-d]
					if !exists {
						t.Errorf("Dependency task %d for task %d in group %s was not executed", i-d, i, groupID)
						continue
					}
					if taskTime.Before(depTime) {
						t.Errorf("Task %d executed before its dependency %d in group %s", i, i-d, groupID)
					}
				}
			}
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
