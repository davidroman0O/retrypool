package tests

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

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
		retrypool.WithIndependentWorkerFactory(func() retrypool.Worker[TestTask] { return worker }),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Simple task without dependencies
	tasks := []TestTask{
		{
			ID:           1,
			GroupID:      "group1",
			Dependencies: []int{},
		},
	}

	if err := pool.SubmitTaskGroup(tasks); err != nil {
		t.Fatalf("Failed to submit task group: %v", err)
	}

	// Wait for completion
	err = pool.WaitForGroup(ctx, "group1")
	if err != nil {
		t.Fatalf("Error waiting for group: %v", err)
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
		retrypool.WithIndependentWorkerFactory(func() retrypool.Worker[TestTask] { return worker }),
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

	if err := pool.SubmitTaskGroup(tasks); err != nil {
		t.Fatalf("Failed to submit task group: %v", err)
	}

	// Wait for completion
	err = pool.WaitForGroup(ctx, "group1")
	if err != nil {
		t.Fatalf("Error waiting for group: %v", err)
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
		retrypool.WithIndependentWorkerFactory(func() retrypool.Worker[TestTask] { return worker }),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Submit first group
	group1Tasks := []TestTask{
		{ID: 1, GroupID: "group1", Dependencies: []int{}},
		{ID: 3, GroupID: "group1", Dependencies: []int{1}},
	}
	if err := pool.SubmitTaskGroup(group1Tasks); err != nil {
		t.Fatalf("Failed to submit group1: %v", err)
	}

	// Submit second group
	group2Tasks := []TestTask{
		{ID: 2, GroupID: "group2", Dependencies: []int{}},
		{ID: 4, GroupID: "group2", Dependencies: []int{2}},
	}
	if err := pool.SubmitTaskGroup(group2Tasks); err != nil {
		t.Fatalf("Failed to submit group2: %v", err)
	}

	// Wait for both groups
	err = pool.WaitForGroup(ctx, "group1")
	if err != nil {
		t.Fatalf("Error waiting for group1: %v", err)
	}
	err = pool.WaitForGroup(ctx, "group2")
	if err != nil {
		t.Fatalf("Error waiting for group2: %v", err)
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var taskFailed bool
	worker := &IndependentTestWorker{executionTimes: make(map[int]time.Time)}

	pool, err := retrypool.NewIndependentPool[TestTask, string, int](
		ctx,
		retrypool.WithIndependentWorkerFactory(func() retrypool.Worker[TestTask] { return worker }),
		retrypool.WithIndependentOnTaskFailed[TestTask](func(task TestTask, err error) {
			taskFailed = true
		}),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	errTask := fmt.Errorf("task error")
	tasks := []TestTask{
		{
			ID:      1,
			GroupID: "group1",
			ExecuteFunc: func() error {
				return errTask
			},
		},
	}

	if err := pool.SubmitTaskGroup(tasks); err != nil {
		t.Fatalf("Failed to submit task group: %v", err)
	}

	// Wait for completion with timeout
	err = pool.WaitForGroup(ctx, "group1")
	if err == nil {
		t.Error("Expected error from WaitForGroup, got nil")
	}

	if !taskFailed {
		t.Error("Task failure callback was not triggered")
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
		retrypool.WithIndependentWorkerFactory(func() retrypool.Worker[TestTask] { return worker }),
		retrypool.WithIndependentWorkerLimits[TestTask](numWorkers, numWorkers),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	var wg sync.WaitGroup
	errorChan := make(chan error, numGroups)

	// Submit task groups concurrently
	for g := 0; g < numGroups; g++ {
		groupID := fmt.Sprintf("group%d", g)
		wg.Add(1)

		go func(gID string) {
			defer wg.Done()

			// Create all tasks for this group
			tasks := make([]TestTask, numTasksPerGroup)
			for i := 0; i < numTasksPerGroup; i++ {
				var deps []int
				if i > 0 {
					// Each task depends on the previous 1-3 tasks
					for d := 1; d <= min(3, i); d++ {
						deps = append(deps, i-d)
					}
				}

				tasks[i] = TestTask{
					ID:           i,
					GroupID:      gID,
					Dependencies: deps,
					ExecuteFunc: func() error {
						time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))
						return nil
					},
				}
			}

			// Submit the entire group
			if err := pool.SubmitTaskGroup(tasks); err != nil {
				errorChan <- fmt.Errorf("failed to submit group %s: %v", gID, err)
				return
			}

			// Wait for group completion
			if err := pool.WaitForGroup(ctx, gID); err != nil {
				errorChan <- fmt.Errorf("error waiting for group %s: %v", gID, err)
				return
			}
		}(groupID)
	}

	// Wait for all groups to be submitted and completed
	wg.Wait()
	close(errorChan)

	// Check for submission errors
	for err := range errorChan {
		t.Errorf("Group error: %v", err)
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

func TestIndependentPool_CyclicDependencies(t *testing.T) {
	ctx := context.Background()
	worker := &IndependentTestWorker{executionTimes: make(map[int]time.Time)}

	pool, err := retrypool.NewIndependentPool[TestTask, string, int](
		ctx,
		retrypool.WithIndependentWorkerFactory(func() retrypool.Worker[TestTask] { return worker }),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Create tasks with cyclic dependencies
	tasks := []TestTask{
		{ID: 1, GroupID: "group1", Dependencies: []int{3}},
		{ID: 2, GroupID: "group1", Dependencies: []int{1}},
		{ID: 3, GroupID: "group1", Dependencies: []int{2}},
	}

	// Submit should fail due to cyclic dependencies
	err = pool.SubmitTaskGroup(tasks)
	if err == nil {
		t.Error("Expected error due to cyclic dependencies, got nil")
	}
}

func TestIndependentPool_MissingDependencies(t *testing.T) {
	ctx := context.Background()
	worker := &IndependentTestWorker{executionTimes: make(map[int]time.Time)}

	pool, err := retrypool.NewIndependentPool[TestTask, string, int](
		ctx,
		retrypool.WithIndependentWorkerFactory(func() retrypool.Worker[TestTask] { return worker }),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Create tasks with missing dependency
	tasks := []TestTask{
		{ID: 1, GroupID: "group1", Dependencies: []int{99}}, // 99 doesn't exist
		{ID: 2, GroupID: "group1", Dependencies: []int{1}},
	}

	// Submit should fail due to missing dependency
	err = pool.SubmitTaskGroup(tasks)
	if err == nil {
		t.Error("Expected error due to missing dependency, got nil")
	}
}
