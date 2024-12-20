package tests

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davidroman0O/retrypool"
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
	executionTimes map[int]time.Time
	mu             sync.Mutex
}

func (w *BlockingWorker) Run(ctx context.Context, task BlockingTask) error {
	w.mu.Lock()
	w.executionTimes[task.ID] = time.Now()
	w.mu.Unlock()

	fmt.Printf("\t\t Running Task %s-%d with deps %v\n", task.GroupID, task.ID, task.Dependencies)

	// Simulate some work
	if task.sleep > 0 {
		time.Sleep(task.sleep)
	}

	// For tasks 1 and 2, create and submit next task
	if task.ID < 3 {
		nextID := task.ID + 1
		nextDone := make(chan struct{})

		nextTask := BlockingTask{
			ID:           nextID,
			GroupID:      task.GroupID,
			Dependencies: []int{task.ID},
			Pool:         task.Pool,
			done:         nextDone,
			sleep:        task.sleep,
		}

		fmt.Printf("\t\t\t Creating Next Task %s-%d with deps %v\n", task.GroupID, nextID, nextTask.Dependencies)

		// Submit next task first
		if err := task.Pool.Submit(nextTask); err != nil {
			return err
		}

		// Signal our completion before waiting for next task
		close(task.done)

		// Now wait for next task
		<-nextDone
	} else {
		// For the last task, just signal completion
		close(task.done)
	}

	return nil
}

func TestBlockingPool_ProperBlocking(t *testing.T) {
	ctx := context.Background()
	worker := &BlockingWorker{executionTimes: make(map[int]time.Time)}

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
	times := make(map[int]time.Time)
	for id, t := range worker.executionTimes {
		times[id] = t
	}
	worker.mu.Unlock()

	// Verify all three tasks were executed
	for i := 1; i <= 3; i++ {
		if _, exists := times[i]; !exists {
			t.Errorf("Task %d was not executed", i)
		}
	}

	// Verify execution order
	if times[2].Before(times[1]) {
		t.Error("Task 2 executed before Task 1")
	}
	if times[3].Before(times[2]) {
		t.Error("Task 3 executed before Task 2")
	}
}

func TestBlockingPool_MultipleGroups(t *testing.T) {
	ctx := context.Background()
	worker := &BlockingWorker{executionTimes: make(map[int]time.Time)}

	pool, err := retrypool.NewBlockingPool[BlockingTask, string, int](
		ctx,
		retrypool.WithBlockingWorkerFactory(func() retrypool.Worker[BlockingTask] { return worker }),
		retrypool.WithBlockingMaxActivePools[BlockingTask](2), // Allow both groups to run
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
	times := worker.executionTimes
	worker.mu.Unlock()

	for _, group := range groups {
		// Check all three tasks for this group
		for i := 1; i <= 3; i++ {
			if _, exists := times[i]; !exists {
				t.Errorf("Task %d was not executed for group %s", i, group)
			}
		}

		// Verify execution order within group
		if times[2].Before(times[1]) {
			t.Errorf("Group %s: Task 2 executed before Task 1", group)
		}
		if times[3].Before(times[2]) {
			t.Errorf("Group %s: Task 3 executed before Task 2", group)
		}
	}
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
