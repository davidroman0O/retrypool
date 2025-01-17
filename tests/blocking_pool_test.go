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
	"github.com/k0kubun/pp/v3"
)

// BlockingTask properly implements the blocking pattern where tasks can spawn other tasks
type BlockingTask struct {
	ID           int
	GroupID      string
	Dependencies []int
	Pool         *retrypool.BlockingPool[BlockingTask, string, int]
	done         chan struct{}
	sleep        time.Duration
}

func (t BlockingTask) GetDependencies() []int { return t.Dependencies }
func (t BlockingTask) GetGroupID() string     { return t.GroupID }
func (t BlockingTask) GetTaskID() int         { return t.ID }

// BlockingWorker demonstrates the proper blocking pattern
type BlockingWorker struct {
	executionTimes map[string]map[int]int64 // groupID -> taskID -> time
	mu             sync.Mutex
}

func (w *BlockingWorker) Run(ctx context.Context, task BlockingTask) error {
	fmt.Printf("\t\t Running Task %s-%d with deps %v\n", task.GroupID, task.ID, task.Dependencies)

	// Simulate work
	if task.sleep > 0 {
		time.Sleep(task.sleep)
	}

	// For tasks 1 and 2, create and submit next task
	if task.ID < 3 {
		nextID := task.ID + 1
		nextDone := make(chan struct{})

		pp.Println("worker task::", "groupID", task.GroupID, "taskID", task.ID)

		nextTask := BlockingTask{
			ID:           nextID,
			GroupID:      task.GroupID,
			Dependencies: []int{task.ID},
			Pool:         task.Pool,
			done:         nextDone,
			sleep:        task.sleep,
		}

		fmt.Printf("\t\t\t Creating Next Task %s-%d with deps %v\n", task.GroupID, nextID, nextTask.Dependencies)

		// Record our execution time AFTER work but BEFORE submitting next task
		w.mu.Lock()
		if w.executionTimes[task.GroupID] == nil {
			w.executionTimes[task.GroupID] = make(map[int]int64)
		}
		w.executionTimes[task.GroupID][task.ID] = time.Now().UnixNano()
		w.mu.Unlock()

		// Submit next task
		if err := task.Pool.Submit(nextTask); err != nil {
			return err
		}

		// Wait for next task
		<-nextDone

		// Signal our completion
		close(task.done)
	} else {
		// For last task, record time then signal completion
		w.mu.Lock()
		if w.executionTimes[task.GroupID] == nil {
			w.executionTimes[task.GroupID] = make(map[int]int64)
		}
		w.executionTimes[task.GroupID][task.ID] = time.Now().UnixNano()
		w.mu.Unlock()

		close(task.done)
	}

	return nil
}

func TestBlockingPool_ProperBlocking(t *testing.T) {
	ctx := context.Background()
	worker := &BlockingWorker{executionTimes: make(map[string]map[int]int64)}

	pool, err := retrypool.NewBlockingPool[BlockingTask, string, int](
		ctx,
		retrypool.WithBlockingWorkerFactory(func() retrypool.Worker[BlockingTask] { return worker }),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Create and submit only the initial task
	done := make(chan struct{})
	task1 := BlockingTask{
		ID:      1,
		GroupID: "groupA",
		Pool:    pool,
		done:    done,
		sleep:   100 * time.Millisecond,
	}

	if err := pool.Submit(task1); err != nil {
		t.Fatalf("Failed to submit initial task: %v", err)
	}

	// Wait for initial task with timeout
	select {
	case <-done:
		t.Log("Initial task completed")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for initial task")
	}

	// Wait for all tasks with timeout
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		t.Logf("Status - Queue: %d, Processing: %d, Dead: %d", queueSize, processingCount, deadTaskCount)
		return queueSize > 0 || processingCount > 0
	}, 100*time.Millisecond)

	if err != nil {
		t.Fatalf("Error waiting for tasks: %v", err)
	}

	// Verify execution times
	worker.mu.Lock()
	times := worker.executionTimes["groupA"] // We only have groupA in this test
	worker.mu.Unlock()

	// Verify all three tasks were executed
	for i := 1; i <= 3; i++ {
		if _, exists := times[i]; !exists {
			t.Errorf("Task %d was not executed", i)
		}
	}

	// Verify execution order
	if times[2] < times[1] {
		t.Error("Task 2 executed before Task 1")
	}
	if times[3] < times[2] {
		t.Error("Task 3 executed before Task 2")
	}
}

func TestBlockingPool_MultipleGroups(t *testing.T) {
	ctx := context.Background()
	worker := &BlockingWorker{executionTimes: make(map[string]map[int]int64)}

	pool, err := retrypool.NewBlockingPool[BlockingTask, string, int](
		ctx,
		retrypool.WithBlockingWorkerFactory(func() retrypool.Worker[BlockingTask] { return worker }),
		retrypool.WithBlockingMaxActivePools[BlockingTask](2), // Allow both groups to run
		retrypool.WithBlockingOnTaskStarted(func(data BlockingTask) {
			t.Logf("Task groupID:%v taskID:%d started", data.GroupID, data.ID)
		}),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Start two independent groups
	groups := []string{"groupA", "groupB"}
	doneChans := make([]chan struct{}, len(groups))

	for i, group := range groups {
		doneChans[i] = make(chan struct{})
		task := BlockingTask{
			ID:      1,
			GroupID: group,
			Pool:    pool,
			done:    doneChans[i],
			sleep:   50 * time.Millisecond,
		}

		if err := pool.Submit(task); err != nil {
			t.Fatalf("Failed to submit initial task for group %s: %v", group, err)
		}
	}

	// Wait for initial tasks with timeout
	for i, done := range doneChans {
		select {
		case <-done:
			t.Logf("Group %s initial task completed", groups[i])
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for group %s initial task", groups[i])
		}
	}

	// Wait for all tasks with timeout
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		t.Logf("Status - Queue: %d, Processing: %d, Dead: %d", queueSize, processingCount, deadTaskCount)
		return queueSize > 0 || processingCount > 0
	}, 100*time.Millisecond)

	if err != nil {
		t.Fatalf("Error waiting for tasks: %v", err)
	}

	// Verify each group had all three tasks execute
	worker.mu.Lock()
	groupTimes := worker.executionTimes
	worker.mu.Unlock()

	for _, group := range groups {
		times := groupTimes[group]
		// Check all three tasks for this group
		for i := 1; i <= 3; i++ {
			if _, exists := times[i]; !exists {
				t.Errorf("Task %d was not executed for group %s", i, group)
			}
		}

		// Verify execution order within group
		if times[2] < times[1] {
			t.Errorf("Group %s: Task 2 executed before Task 1", group)
		}
		if times[3] < times[2] {
			t.Errorf("Group %s: Task 3 executed before Task 2", group)
		}
	}

	pp.Println("result::", groups, worker.executionTimes)
}

// StressTestWorker properly implements the Worker interface for stress testing
type StressTestWorker struct {
	executionTimes map[int]time.Time
	taskCount      atomic.Int32
	mu             sync.Mutex
	t              *testing.T
}

func NewStressTestWorker(t *testing.T) *StressTestWorker {
	return &StressTestWorker{
		executionTimes: make(map[int]time.Time),
		t:              t,
	}
}

func (w *StressTestWorker) Run(ctx context.Context, task BlockingTask) error {
	w.mu.Lock()
	w.executionTimes[task.ID] = time.Now()
	currentCount := w.taskCount.Add(1)
	w.mu.Unlock()

	w.t.Logf("Running task %d in group %s (total tasks: %d)", task.ID, task.GroupID, currentCount)

	// Simulate work
	time.Sleep(task.sleep)

	// Calculate task's depth based on ID
	baseID := (task.ID / 1000) * 1000
	depth := task.ID - baseID

	// Spawn children if not at max depth
	if depth < maxDepth {
		numChildren := rand.Intn(maxChildren) + 1
		childWg := &sync.WaitGroup{}
		childWg.Add(numChildren)

		for i := 0; i < numChildren; i++ {
			childID := task.ID*10 + i + 1
			childDone := make(chan struct{})

			childTask := BlockingTask{
				ID:           childID,
				GroupID:      task.GroupID,
				Dependencies: []int{task.ID},
				Pool:         task.Pool,
				done:         childDone,
				sleep:        time.Duration(20+rand.Intn(30)) * time.Millisecond,
			}

			// Submit child task
			if err := task.Pool.Submit(childTask); err != nil {
				return fmt.Errorf("failed to submit child task %d: %v", childID, err)
			}

			// Signal completion before waiting for children
			close(task.done)

			// Wait for child completion in goroutine
			go func(id int) {
				defer childWg.Done()
				select {
				case <-childDone:
					w.t.Logf("Child task %d completed", id)
				case <-ctx.Done():
					w.t.Logf("Context cancelled while waiting for child task %d", id)
				}
			}(childID)
		}

		// Wait for all children to complete
		childWg.Wait()
	} else {
		// For leaf tasks, just signal completion
		close(task.done)
	}

	return nil
}

const (
	maxDepth    = 3 // How deep the task tree goes
	maxChildren = 3 // Maximum children per task
)

func TestBlockingPool_StressTest(t *testing.T) {
	ctx := context.Background()
	worker := NewStressTestWorker(t)

	pool, err := retrypool.NewBlockingPool[BlockingTask, string, int](
		ctx,
		retrypool.WithBlockingWorkerFactory(func() retrypool.Worker[BlockingTask] { return worker }),
		retrypool.WithBlockingMaxActivePools[BlockingTask](5),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Create tracking channels for root tasks
	numGroups := 5
	groups := make([]struct {
		id   string
		done chan struct{}
	}, numGroups)

	for i := range groups {
		groups[i].id = fmt.Sprintf("group%d", i)
		groups[i].done = make(chan struct{})
	}

	// Submit root tasks for each group
	for i, group := range groups {
		rootTask := BlockingTask{
			ID:      i*1000 + 1, // Use separate ID space per group
			GroupID: group.id,
			Pool:    pool,
			done:    group.done,
			sleep:   50 * time.Millisecond,
		}

		if err := pool.Submit(rootTask); err != nil {
			t.Fatalf("Failed to submit root task for group %s: %v", group.id, err)
		}
	}

	// Wait for root tasks with timeout
	rootTimeout := time.After(5 * time.Second)
	for _, group := range groups {
		select {
		case <-group.done:
			t.Logf("Root task for group %s completed", group.id)
		case <-rootTimeout:
			t.Fatalf("Timeout waiting for root tasks")
		}
	}

	// Wait for all tasks with timeout
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		t.Logf("Status - Queue: %d, Processing: %d, Dead: %d", queueSize, processingCount, deadTaskCount)
		return queueSize > 0 || processingCount > 0
	}, 100*time.Millisecond)

	if err != nil {
		t.Fatalf("Error waiting for tasks: %v", err)
	}

	// Verify execution order
	worker.mu.Lock()
	totalTasks := len(worker.executionTimes)
	times := make(map[string]map[int]time.Time)

	// Sort tasks by group
	for id, timestamp := range worker.executionTimes {
		groupIndex := id / 1000
		groupID := fmt.Sprintf("group%d", groupIndex)
		if times[groupID] == nil {
			times[groupID] = make(map[int]time.Time)
		}
		times[groupID][id] = timestamp
	}
	worker.mu.Unlock()

	t.Logf("Total tasks executed: %d", totalTasks)

	// Verify parent-child relationships in each group
	for groupID, groupTimes := range times {
		for childID, childTime := range groupTimes {
			if childID%10 != 1 { // Not a root task
				parentID := childID / 10
				if parentTime, exists := groupTimes[parentID]; exists {
					if childTime.Before(parentTime) {
						t.Errorf("Group %s: Child task %d executed before parent task %d", groupID, childID, parentID)
					}
				}
			}
		}
	}

	t.Logf("Stress test completed successfully")
}

// blockingTaskCallback for testing callbacks
type blockingTaskCallback struct {
	ID      int
	GroupID string
}

func (t blockingTaskCallback) GetDependencies() []int { return nil }
func (t blockingTaskCallback) GetGroupID() string     { return t.GroupID }
func (t blockingTaskCallback) GetTaskID() int         { return t.ID }

// blockingSimpleWorkerCalblack just executes tasks without blocking behavior
type blockingSimpleWorkerCalblack struct {
	executionCount atomic.Int32
}

func (w *blockingSimpleWorkerCalblack) Run(ctx context.Context, task blockingTaskCallback) error {
	w.executionCount.Add(1)
	time.Sleep(50 * time.Millisecond) // Small delay to simulate work
	return nil
}

func TestBlockingPool_TaskCallbacks(t *testing.T) {
	ctx := context.Background()
	worker := &blockingSimpleWorkerCalblack{}

	var submitted, started, completed atomic.Int32

	pool, err := retrypool.NewBlockingPool[blockingTaskCallback, string, int](
		ctx,
		retrypool.WithBlockingWorkerFactory(func() retrypool.Worker[blockingTaskCallback] { return worker }),
		retrypool.WithBlockingOnTaskSubmitted[blockingTaskCallback](func(task blockingTaskCallback) {
			submitted.Add(1)
			t.Logf("Task %d submitted", task.ID)
		}),
		retrypool.WithBlockingOnTaskStarted[blockingTaskCallback](func(task blockingTaskCallback) {
			started.Add(1)
			t.Logf("Task %d started", task.ID)
		}),
		retrypool.WithBlockingOnTaskCompleted[blockingTaskCallback](func(task blockingTaskCallback) {
			completed.Add(1)
			t.Logf("Task %d completed", task.ID)
		}),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	task := blockingTaskCallback{
		ID:      1,
		GroupID: "group1",
	}

	if err := pool.Submit(task); err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	// Wait for task completion with monitoring
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = pool.WaitWithCallback(ctx,
		func(queueSize, processingCount, deadTaskCount int) bool {
			t.Logf("Status: Queue=%d Processing=%d Dead=%d Submitted=%d Started=%d Completed=%d",
				queueSize, processingCount, deadTaskCount,
				submitted.Load(), started.Load(), completed.Load())
			return queueSize > 0 || processingCount > 0
		},
		100*time.Millisecond,
	)

	if err != nil {
		t.Fatalf("Error waiting for task: %v", err)
	}

	// Verify callback counts
	if s := submitted.Load(); s != 1 {
		t.Errorf("Expected 1 submission, got %d", s)
	}
	if s := started.Load(); s != 1 {
		t.Errorf("Expected 1 start, got %d", s)
	}
	if c := completed.Load(); c != 1 {
		t.Errorf("Expected 1 completion, got %d", c)
	}

	// Verify worker actually executed the task
	if e := worker.executionCount.Load(); e != 1 {
		t.Errorf("Expected worker to execute task once, got %d executions", e)
	}
}

func TestBlockingPool_PoolEvents(t *testing.T) {
	ctx := context.Background()
	worker := &BlockingWorker{executionTimes: make(map[string]map[int]int64)}

	var poolsCreated int
	var mu sync.Mutex

	pool, err := retrypool.NewBlockingPool[BlockingTask, string, int](
		ctx,
		retrypool.WithBlockingWorkerFactory(func() retrypool.Worker[BlockingTask] { return worker }),
		retrypool.WithBlockingMaxActivePools[BlockingTask](2), // Allow 2 pools to run concurrently
		retrypool.WithBlockingOnPoolCreated[BlockingTask](func(groupID any) {
			mu.Lock()
			poolsCreated++
			mu.Unlock()
		}),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Create tasks for multiple groups to trigger pool creation/destruction
	groups := []string{"group1", "group2"}
	doneChans := make([]chan struct{}, len(groups))

	for i, gid := range groups {
		doneChans[i] = make(chan struct{})
		task := BlockingTask{
			ID:      i + 1,
			GroupID: gid,
			Pool:    pool,
			done:    doneChans[i],
			sleep:   50 * time.Millisecond,
		}

		if err := pool.Submit(task); err != nil {
			t.Fatalf("Failed to submit task for group %s: %v", gid, err)
		}
	}

	// Wait for all tasks
	for i, done := range doneChans {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for group %s", groups[i])
		}
	}

	mu.Lock()
	if poolsCreated == 0 {
		t.Error("No pools were created")
	}
	mu.Unlock()
}

// BlockingFailureTask implements DependentTask and tracks failure state
type BlockingFailureTask struct {
	ID           int
	GroupID      string
	Dependencies []int
	ShouldFail   bool
	Pool         *retrypool.BlockingPool[BlockingFailureTask, string, int]
	Done         chan struct{}
}

func (t BlockingFailureTask) GetDependencies() []int { return t.Dependencies }
func (t BlockingFailureTask) GetGroupID() string     { return t.GroupID }
func (t BlockingFailureTask) GetTaskID() int         { return t.ID }

// BlockingFailureWorker tracks task execution and failures
type BlockingFailureWorker struct {
	executedTasks atomic.Int32
	failedTasks   atomic.Int32
}

func (w *BlockingFailureWorker) Run(ctx context.Context, task BlockingFailureTask) error {
	w.executedTasks.Add(1)

	// Check for failure condition first
	if task.ShouldFail {
		w.failedTasks.Add(1)
		close(task.Done) // Signal completion before failing
		return errors.New("task failed as configured")
	}

	// For tasks 1, create and submit next task
	if task.ID == 1 {
		nextID := task.ID + 1
		nextDone := make(chan struct{})

		nextTask := BlockingFailureTask{
			ID:           nextID,
			GroupID:      task.GroupID,
			Dependencies: []int{task.ID},
			Pool:         task.Pool,
			Done:         nextDone,
			ShouldFail:   true, // Task 2 will fail
		}

		// Submit next task first
		if err := task.Pool.Submit(nextTask); err != nil {
			return err
		}

		// Wait for next task
		<-nextDone

		// Signal our completion
		close(task.Done)

		// Propagate failure from task 2
		return errors.New("propagated failure from task 2")
	}

	// For any other tasks, just signal completion
	close(task.Done)
	return nil
}

func TestBlockingPool_ChainFailureHandling(t *testing.T) {
	ctx := context.Background()
	worker := &BlockingFailureWorker{}

	var taskFailures atomic.Int32
	var groupRemovals atomic.Int32

	pool, err := retrypool.NewBlockingPool[BlockingFailureTask, string, int](
		ctx,
		retrypool.WithBlockingWorkerFactory(func() retrypool.Worker[BlockingFailureTask] { return worker }),
		retrypool.WithBlockingMaxActivePools[BlockingFailureTask](1),
		retrypool.WithBlockingOnTaskFailed[BlockingFailureTask](func(task BlockingFailureTask, err error) {
			taskFailures.Add(1)
			t.Logf("Task %d failed: %v", task.ID, err)
		}),
		retrypool.WithBlockingOnTaskCompleted(func(task BlockingFailureTask) {
			t.Logf("Task %d completed", task.ID)
		}),
		retrypool.WithBlockingOnGroupRemoved[BlockingFailureTask](func(groupID any, tasks []BlockingFailureTask) {
			groupRemovals.Add(1)
			t.Logf("Group %v removed", groupID)
		}),
	)

	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Create first task that will trigger the chain
	done := make(chan struct{})
	firstTask := BlockingFailureTask{
		ID:         1,
		GroupID:    "group1",
		Pool:       pool,
		Done:       done,
		ShouldFail: false,
	}

	if err := pool.Submit(firstTask); err != nil {
		t.Fatalf("Failed to submit first task: %v", err)
	}

	// Wait for chain completion/failure with timeout
	select {
	case <-done:
		t.Log("Task chain completed/failed")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for task chain")
	}

	// Wait for cleanup
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
	if executed := worker.executedTasks.Load(); executed != 2 {
		t.Errorf("Expected 2 task executions (tasks 1 and 2), got %d", executed)
	}

	if failed := taskFailures.Load(); failed != 2 {
		t.Errorf("Expected 2 task failure callback, got %d", failed)
	}

	if removals := groupRemovals.Load(); removals != 1 {
		t.Errorf("Expected 1 group removal, got %d", removals)
	}

	// Close pool
	if err := pool.Close(); err != nil {
		t.Fatalf("Failed to close pool: %v", err)
	}
}

// TreeTask implements BlockingDependentTask for testing
type TreeTask struct {
	ID           int
	GroupID      string
	Dependencies []int
	Done         chan struct{}
}

func (t TreeTask) GetDependencies() []int { return t.Dependencies }
func (t TreeTask) GetGroupID() string     { return t.GroupID }
func (t TreeTask) GetTaskID() int         { return t.ID }

// TreeWorker implements Worker
type TreeWorker struct {
	executionTimes map[int]time.Time
	mu             sync.Mutex
	pool           *retrypool.BlockingPool[TreeTask, string, int]
}

func (w *TreeWorker) Run(ctx context.Context, task TreeTask) error {
	w.mu.Lock()
	w.executionTimes[task.ID] = time.Now()
	w.mu.Unlock()

	fmt.Printf("Running Task %d\n", task.ID)

	// If this is the root task, submit all dependent tasks
	if task.ID == 1 {
		childrenChn := []chan struct{}{}
		// Submit 10 dependent tasks
		for i := 2; i <= 11; i++ {
			childDone := make(chan struct{})
			childTask := TreeTask{
				ID:           i,
				GroupID:      task.GroupID,
				Dependencies: []int{1}, // All depend on task 1
				Done:         childDone,
			}

			if err := w.pool.Submit(childTask); err != nil {
				return fmt.Errorf("failed to submit dependent task %d: %v", i, err)
			}

			childrenChn = append(childrenChn, childDone)
		}

		// Wait for each dependent task to complete
		for _, v := range childrenChn {
			<-v
		}
	}

	close(task.Done)
	fmt.Printf("Task %d completed\n", task.ID)

	return nil
}

func TestBlockingPool_TreeTasks(t *testing.T) {
	ctx := context.Background()

	var pool *retrypool.BlockingPool[TreeTask, string, int]

	var err error
	pool, err = retrypool.NewBlockingPool[TreeTask, string, int](
		ctx,
		retrypool.WithBlockingWorkerFactory(func() retrypool.Worker[TreeTask] {
			return &TreeWorker{pool: pool, executionTimes: make(map[int]time.Time)}
		}),
		retrypool.WithBlockingMaxWorkersPerPool[TreeTask](2), // we should have 1 for the root and 1 for the children
		retrypool.WithBlockingMaxActivePools[TreeTask](1),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Submit root task
	done := make(chan struct{})
	rootTask := TreeTask{
		ID:      1,
		GroupID: "tree1",
		Done:    done,
	}

	if err := pool.Submit(rootTask); err != nil {
		t.Fatalf("Failed to submit root task: %v", err)
	}

	// Wait for completion
	select {
	case <-done:
		t.Log("All tasks completed")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for tasks")
	}

	// // Verify execution times
	// worker.mu.Lock()
	// rootTime := worker.executionTimes[1]

	// // All dependent tasks must execute after root
	// for id := 2; id <= 11; id++ {
	// 	childTime, exists := worker.executionTimes[id]
	// 	if !exists {
	// 		t.Errorf("Task %d was not executed", id)
	// 		continue
	// 	}
	// 	if childTime.Before(rootTime) {
	// 		t.Errorf("Task %d executed before root task", id)
	// 	}
	// }
	// worker.mu.Unlock()

	pp.Println(pool.GetSnapshot())

	if err := pool.Close(); err != nil {
		t.Fatalf("Failed to close pool: %v", err)
	}
}

// TreeNonLinearTask implements BlockingDependentTask for testing
type TreeNonLinearTask struct {
	ID           int
	GroupID      string
	Dependencies []int
	Done         chan struct{}
}

func (t TreeNonLinearTask) GetDependencies() []int { return t.Dependencies }
func (t TreeNonLinearTask) GetGroupID() string     { return t.GroupID }
func (t TreeNonLinearTask) GetTaskID() int         { return t.ID }

// TreeNonLinearWorker implements Worker
type TreeNonLinearWorker struct {
	executionTimes map[int]time.Time
	mu             sync.Mutex
	pool           *retrypool.BlockingPool[TreeNonLinearTask, string, int]
}

func (w *TreeNonLinearWorker) Run(ctx context.Context, task TreeNonLinearTask) error {
	w.mu.Lock()
	w.executionTimes[task.ID] = time.Now()
	w.mu.Unlock()

	fmt.Printf("Running Task %d\n", task.ID)

	// If this is the root task, submit all dependent tasks
	if task.ID == 1 {
		// Submit 10 dependent tasks
		for i := 2; i <= 11; i++ {
			childDone := make(chan struct{})
			childTask := TreeNonLinearTask{
				ID:           i,
				GroupID:      task.GroupID,
				Dependencies: []int{1}, // All depend on task 1
				Done:         childDone,
			}
			if err := w.pool.Submit(childTask); err != nil {
				return fmt.Errorf("failed to submit dependent task %d: %v", i, err)
			}
		}
		// we won't wait for it
	}

	close(task.Done)
	fmt.Printf("Task %d completed\n", task.ID)

	return nil
}

func TestBlockingPool_TreeNonLinearTasks(t *testing.T) {
	ctx := context.Background()

	var pool *retrypool.BlockingPool[TreeNonLinearTask, string, int]

	var err error
	pool, err = retrypool.NewBlockingPool[TreeNonLinearTask, string, int](
		ctx,
		retrypool.WithBlockingWorkerFactory(func() retrypool.Worker[TreeNonLinearTask] {
			return &TreeNonLinearWorker{pool: pool, executionTimes: make(map[int]time.Time)}
		}),
		retrypool.WithBlockingMaxWorkersPerPool[TreeNonLinearTask](2), // we should have 1 for the root and 1 for the children
		retrypool.WithBlockingMaxActivePools[TreeNonLinearTask](1),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Submit root task
	done := make(chan struct{})
	rootTask := TreeNonLinearTask{
		ID:      1,
		GroupID: "tree1",
		Done:    done,
	}

	if err := pool.Submit(rootTask); err != nil {
		t.Fatalf("Failed to submit root task: %v", err)
	}

	// Wait for completion
	select {
	case <-done:
		t.Log("All tasks completed")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for tasks")
	}

	// // Verify execution times
	// worker.mu.Lock()
	// rootTime := worker.executionTimes[1]

	// // All dependent tasks must execute after root
	// for id := 2; id <= 11; id++ {
	// 	childTime, exists := worker.executionTimes[id]
	// 	if !exists {
	// 		t.Errorf("Task %d was not executed", id)
	// 		continue
	// 	}
	// 	if childTime.Before(rootTime) {
	// 		t.Errorf("Task %d executed before root task", id)
	// 	}
	// }
	// worker.mu.Unlock()

	pp.Println(pool.GetSnapshot())

	if err := pool.Close(); err != nil {
		t.Fatalf("Failed to close pool: %v", err)
	}
}
