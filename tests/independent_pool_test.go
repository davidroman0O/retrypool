package tests

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
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

	if err := pool.Submit(tasks); err != nil {
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

	if err := pool.Submit(tasks); err != nil {
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
	if err := pool.Submit(group1Tasks); err != nil {
		t.Fatalf("Failed to submit group1: %v", err)
	}

	// Submit second group
	group2Tasks := []TestTask{
		{ID: 2, GroupID: "group2", Dependencies: []int{}},
		{ID: 4, GroupID: "group2", Dependencies: []int{2}},
	}
	if err := pool.Submit(group2Tasks); err != nil {
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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

	if err := pool.Submit(tasks); err != nil {
		t.Fatalf("Failed to submit task group: %v", err)
	}

	// Wait for completion with timeout
	err = pool.WaitForGroup(ctx, "group1")
	if err != nil {
		t.Fatalf("Error waiting for group: %v", err)
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
			if err := pool.Submit(tasks); err != nil {
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
	err = pool.Submit(tasks)
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
	err = pool.Submit(tasks)
	if err == nil {
		t.Error("Expected error due to missing dependency, got nil")
	}
}

func TestIndependentPool_WorkerScaling(t *testing.T) {
	ctx := context.Background()
	worker := &IndependentTestWorker{executionTimes: make(map[int]time.Time)}

	workerAddedCount := 0
	workerRemovedCount := 0

	pool, err := retrypool.NewIndependentPool[TestTask, string, int](
		ctx,
		retrypool.WithIndependentWorkerFactory(func() retrypool.Worker[TestTask] { return worker }),
		retrypool.WithIndependentWorkerLimits[TestTask](1, 5),
		retrypool.WithIndependentOnWorkerAdded[TestTask](func(workerID int) {
			workerAddedCount++
		}),
		retrypool.WithIndependentOnWorkerRemoved[TestTask](func(workerID int) {
			workerRemovedCount++
		}),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Create tasks that will force worker scaling
	tasks := make([]TestTask, 10)
	for i := 0; i < 10; i++ {
		tasks[i] = TestTask{
			ID:      i,
			GroupID: "group1",
			ExecuteFunc: func() error {
				time.Sleep(100 * time.Millisecond)
				return nil
			},
		}
	}

	if err := pool.Submit(tasks); err != nil {
		t.Fatalf("Failed to submit tasks: %v", err)
	}

	err = pool.WaitForGroup(ctx, "group1")
	if err != nil {
		t.Fatalf("Error waiting for group: %v", err)
	}

	if workerAddedCount == 0 {
		t.Error("No workers were added during scaling")
	}

	if err := pool.Close(); err != nil {
		t.Fatalf("Failed to close pool: %v", err)
	}
}

func TestIndependentPool_GroupCallbacks(t *testing.T) {
	ctx := context.Background()
	worker := &IndependentTestWorker{executionTimes: make(map[int]time.Time)}

	var groupsCreated, groupsCompleted int
	var mu sync.Mutex

	pool, err := retrypool.NewIndependentPool[TestTask, string, int](
		ctx,
		retrypool.WithIndependentWorkerFactory(func() retrypool.Worker[TestTask] { return worker }),
		retrypool.WithIndependentOnGroupCreated[TestTask](func(groupID any) {
			mu.Lock()
			groupsCreated++
			mu.Unlock()
		}),
		retrypool.WithIndependentOnGroupCompleted[TestTask](func(groupID any) {
			mu.Lock()
			groupsCompleted++
			mu.Unlock()
		}),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Submit multiple groups
	groups := []string{"group1", "group2", "group3"}
	for _, gid := range groups {
		tasks := []TestTask{
			{ID: 1, GroupID: gid},
			{ID: 2, GroupID: gid, Dependencies: []int{1}},
		}

		if err := pool.Submit(tasks); err != nil {
			t.Fatalf("Failed to submit group %s: %v", gid, err)
		}
	}

	// Wait for all groups
	for _, gid := range groups {
		err = pool.WaitForGroup(ctx, gid)
		if err != nil {
			t.Fatalf("Error waiting for group %s: %v", gid, err)
		}
	}

	mu.Lock()
	if groupsCreated != len(groups) {
		t.Errorf("Expected %d groups created, got %d", len(groups), groupsCreated)
	}
	if groupsCompleted != len(groups) {
		t.Errorf("Expected %d groups completed, got %d", len(groups), groupsCompleted)
	}
	mu.Unlock()
}

func TestIndependentPool_TaskFailureHandling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var failureCount atomic.Int32
	var completionCount atomic.Int32
	var deadTaskCount atomic.Int32

	var pool *retrypool.IndependentPool[TestTask, string, int]

	pool, err := retrypool.NewIndependentPool[TestTask, string, int](
		ctx,
		retrypool.WithIndependentWorkerFactory(func() retrypool.Worker[TestTask] {
			return &IndependentTestWorker{executionTimes: make(map[int]time.Time)}
		}),
		retrypool.WithIndependentOnTaskFailed[TestTask](func(task TestTask, err error) {
			failureCount.Add(1)
			t.Logf("Task %d failed and moved to dead tasks: %v", task.ID, err)
		}),
		retrypool.WithIndependentOnTaskCompleted[TestTask](func(task TestTask) {
			completionCount.Add(1)
			t.Logf("Task %d completed", task.ID)
		}),
		retrypool.WithIndependentOnDeadTask[TestTask](func(deadTaskIndex int) {
			deadTaskCount.Add(1)
			task, err := pool.PullDeadTask(deadTaskIndex)
			if err != nil {
				t.Errorf("Failed to pull dead task: %v", err)
				return
			}
			t.Logf("dead task %d", task.Data.ID)
		}),
	)

	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	expectedError := errors.New("intentional failure")

	tasks := []TestTask{
		{
			ID:      1,
			GroupID: "group1",
			ExecuteFunc: func() error {
				return expectedError
			},
		},
		{
			ID:           2,
			GroupID:      "group1",
			Dependencies: []int{1},
			ExecuteFunc: func() error {
				t.Error("Task 2 should never execute since task 1 failed")
				return nil
			},
		},
	}

	if err := pool.Submit(tasks); err != nil {
		t.Fatalf("Failed to submit tasks: %v", err)
	}

	err = pool.WaitWithCallback(
		ctx,
		func(queueSize, processingCount, deadTaskCount int) bool {
			t.Logf("Status: Queue=%d Processing=%d Dead=%d Failures=%d Completions=%d",
				queueSize, processingCount, deadTaskCount,
				failureCount.Load(), completionCount.Load())
			return queueSize > 0 || processingCount > 0
		},
		100*time.Millisecond,
	)

	if err != nil {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	if failures := failureCount.Load(); failures != 1 {
		t.Errorf("Expected 1 failure, got %d", failures)
	}

	if dead := deadTaskCount.Load(); dead != 1 {
		t.Errorf("Expected 1 dead task, got %d", dead)
	}

	if completions := completionCount.Load(); completions != 0 {
		t.Errorf("Expected 0 completions (all tasks should fail), got %d", completions)
	}
}

// FailureTask implements DependentTask and can be configured to fail
type FailureTask struct {
	ID           int
	GroupID      string
	Dependencies []int
	ShouldFail   bool
}

func (t FailureTask) GetDependencies() []int { return t.Dependencies }
func (t FailureTask) GetGroupID() string     { return t.GroupID }
func (t FailureTask) GetTaskID() int         { return t.ID }

// FailureWorker tracks task execution and failures
type FailureWorker struct {
	executedTasks atomic.Int32
	failedTasks   atomic.Int32
}

func (w *FailureWorker) Run(ctx context.Context, task FailureTask) error {
	w.executedTasks.Add(1)
	if task.ShouldFail {
		w.failedTasks.Add(1)
		return errors.New("task failed as configured")
	}
	return nil
}

func TestIndependentPool_TaskFailureRemoveHandling(t *testing.T) {
	ctx := context.Background()
	worker := &FailureWorker{}

	var taskFailures atomic.Int32
	var deadTasks atomic.Int32
	var groupRemovals atomic.Int32

	pool, err := retrypool.NewIndependentPool[FailureTask, string, int](
		ctx,
		retrypool.WithIndependentWorkerFactory(func() retrypool.Worker[FailureTask] { return worker }),
		retrypool.WithIndependentOnTaskFailed[FailureTask](func(task FailureTask, err error) {
			taskFailures.Add(1)
		}),
		retrypool.WithIndependentOnDeadTask[FailureTask](func(deadTaskIndex int) {
			deadTasks.Add(1)
		}),
		retrypool.WithIndependentOnGroupRemoved[FailureTask](func(groupID any, tasks []FailureTask) {
			groupRemovals.Add(1)
		}),
	)

	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Create a chain of tasks where Task 1 fails and Task 2 depends on it
	tasks := []FailureTask{
		{
			ID:         1,
			GroupID:    "group1",
			ShouldFail: true,
		},
		{
			ID:           2,
			GroupID:      "group1",
			Dependencies: []int{1}, // Depends on the failing task
		},
	}

	if err := pool.Submit(tasks); err != nil {
		t.Fatalf("Failed to submit tasks: %v", err)
	}

	// Wait for completion with timeout
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = pool.WaitWithCallback(
		ctx,
		func(queueSize, processingCount, deadTaskCount int) bool {
			t.Logf("Queue: %d, Processing: %d, Dead: %d", queueSize, processingCount, deadTaskCount)
			return queueSize > 0 || processingCount > 0
		},
		100*time.Millisecond,
	)

	if err != nil {
		t.Fatalf("Error waiting for tasks: %v", err)
	}

	// Verify outcomes
	if executed := worker.executedTasks.Load(); executed != 1 {
		t.Errorf("Expected 1 task execution (failing task), got %d", executed)
	}

	if failed := taskFailures.Load(); failed != 1 {
		t.Errorf("Expected 1 task failure callback, got %d", failed)
	}

	if dead := deadTasks.Load(); dead != 1 {
		t.Errorf("Expected 1 dead task, got %d", dead)
	}

	if removals := groupRemovals.Load(); removals != 1 {
		t.Errorf("Expected 1 group removal, got %d", removals)
	}

	if err := pool.Close(); err != nil {
		t.Fatalf("Failed to close pool: %v", err)
	}
}
