package tests

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/retrypool"
)

// MemoryLeakTask represents a task that can be used to test for memory leaks
type MemoryLeakTask struct {
	ID           int
	GroupID      string
	Dependencies []int
	Data         []byte // Large data to make memory leaks more obvious
	Pool         *retrypool.BlockingPool[MemoryLeakTask, string, int]
	done         chan struct{}
}

func (t MemoryLeakTask) GetDependencies() []int { return t.Dependencies }
func (t MemoryLeakTask) GetGroupID() string     { return t.GroupID }
func (t MemoryLeakTask) GetTaskID() int         { return t.ID }

type MemoryLeakWorkerBlockingPool struct {
	executionTimes map[int]time.Time
	mu             sync.Mutex
}

func NewMemoryLeakWorkerBlockingPool() *MemoryLeakWorkerBlockingPool {
	return &MemoryLeakWorkerBlockingPool{
		executionTimes: make(map[int]time.Time),
	}
}

func (w *MemoryLeakWorkerBlockingPool) Run(ctx context.Context, task MemoryLeakTask) error {
	w.mu.Lock()
	w.executionTimes[task.ID] = time.Now()
	w.mu.Unlock()

	// Simulate some work
	time.Sleep(10 * time.Millisecond)

	// For tasks 1 and 2, create and submit next task
	if task.ID < 3 {
		nextID := task.ID + 1
		nextDone := make(chan struct{})

		nextTask := MemoryLeakTask{
			ID:           nextID,
			GroupID:      task.GroupID,
			Dependencies: []int{task.ID},
			Pool:         task.Pool,
			done:         nextDone,
			Data:         make([]byte, len(task.Data)), // Same size data
		}

		// Submit next task first
		if err := task.Pool.Submit(nextTask); err != nil {
			return err
		}

		// Now wait for next task
		<-nextDone

		// Signal our completion before returning
		close(task.done)
	} else {
		// For the last task, just signal completion
		close(task.done)
	}

	return nil
}

func getMemStats() runtime.MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

// TestBlockingPoolMemoryLeak tests the BlockingPool for memory leaks
func TestBlockingPoolMemoryLeak(t *testing.T) {
	ctx := context.Background()

	// Track removed groups to verify cleanup
	var removedGroups sync.Map
	var processedTasks sync.Map
	var pool *retrypool.BlockingPool[MemoryLeakTask, string, int]
	var err error

	pool, err = retrypool.NewBlockingPool[MemoryLeakTask, string, int](
		ctx,
		retrypool.WithBlockingWorkerFactory(func() retrypool.Worker[MemoryLeakTask] { return NewMemoryLeakWorkerBlockingPool() }),
		retrypool.WithBlockingMaxActivePools[MemoryLeakTask](2),
		retrypool.WithBlockingOnTaskCompleted[MemoryLeakTask](func(task MemoryLeakTask) {
			t.Logf("Task completed - Group: %s, ID: %d", task.GroupID, task.ID)
			processedTasks.Store(fmt.Sprintf("%s-%d", task.GroupID, task.ID), true)
		}),
		retrypool.WithBlockingOnGroupRemoved[MemoryLeakTask](func(groupID any, tasks []MemoryLeakTask) {
			if _, exists := removedGroups.LoadOrStore(groupID, true); !exists {
				t.Logf("Group %v removed for the first time", groupID)
			} else {
				t.Logf("WARNING: Group %v removed again!", groupID)
			}
		}),
		retrypool.WithBlockingOnPoolCreated[MemoryLeakTask](func(groupID any) {
			t.Logf("Pool created for group %v", groupID)
		}),
		retrypool.WithBlockingOnPoolDestroyed[MemoryLeakTask](func(groupID any) {
			t.Logf("Pool destroyed for group %v", groupID)
		}),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Record initial memory state
	initialStats := getMemStats()

	// Number of groups to create and submit
	const (
		numGroups = 100
		dataSize  = 1024 * 1024 // 1MB of data per task
	)

	// Submit initial task for each group
	var wg sync.WaitGroup
	for i := 0; i < numGroups; i++ {
		wg.Add(1)
		go func(groupNum int) {
			defer wg.Done()
			groupID := fmt.Sprintf("group%d", groupNum)
			done := make(chan struct{})

			task := MemoryLeakTask{
				ID:      1, // Start with task 1
				GroupID: groupID,
				Data:    make([]byte, dataSize),
				Pool:    pool,
				done:    done,
			}

			if err := pool.Submit(task); err != nil {
				t.Errorf("Failed to submit initial task for group %s: %v", groupID, err)
				return
			}

			// Optionally wait for task completion
			<-done
		}(i)
	}
	wg.Wait()

	var removedCount int

	// Wait for all groups to complete
	deadline := time.Now().Add(30 * time.Second)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	completed := false
	for !completed && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			t.Fatal("Context cancelled")
			return

		case <-ticker.C:
			removedCount = 0
			removedGroups.Range(func(k, _ interface{}) bool {
				removedCount++
				return true
			})

			processedCount := 0
			processedTasks.Range(func(k, _ interface{}) bool {
				processedCount++
				return true
			})

			t.Logf("Status - Removed groups: %d/%d, Processed tasks: %d/300",
				removedCount, numGroups, processedCount)

			if removedCount == numGroups {
				completed = true
				break
			}
		}
	}

	// Force garbage collection
	runtime.GC()

	if !completed {
		t.Errorf("Timeout reached. Only %d/%d groups removed", removedCount, numGroups)
	}

	// Get final memory stats
	finalStats := getMemStats()

	// Check internal pool state
	if err := pool.Close(); err != nil {
		t.Fatalf("Failed to close pool: %v", err)
	}

	// Force final GC
	runtime.GC()

	// Log memory metrics
	t.Logf("Memory growth: %d bytes", finalStats.HeapAlloc-initialStats.HeapAlloc)
}

type MemoryLeakWorker struct {
	executedTasks     map[string]bool
	executedTasksLock sync.RWMutex
}

func NewMemoryLeakWorker() *MemoryLeakWorker {
	return &MemoryLeakWorker{
		executedTasks: make(map[string]bool),
	}
}

func (w *MemoryLeakWorker) Run(ctx context.Context, task MemoryLeakTask) error {
	// Create task key for tracking
	taskKey := fmt.Sprintf("%s-%d", task.GroupID, task.ID)

	// Safely track execution
	w.executedTasksLock.Lock()
	w.executedTasks[taskKey] = true
	w.executedTasksLock.Unlock()

	// Simulate some work with the data to ensure it's properly allocated
	if len(task.Data) > 0 {
		// Do some trivial work with the data to ensure it's properly handled
		_ = task.Data[0]
	}

	// Return nil to indicate success
	return nil
}

func TestIndependentPoolMemoryLeak(t *testing.T) {
	ctx := context.Background()

	var removedGroups sync.Map
	var processedTasks sync.Map
	var groupsWithDeadTasks sync.Map
	var pool *retrypool.IndependentPool[MemoryLeakTask, string, int]
	var err error

	pool, err = retrypool.NewIndependentPool[MemoryLeakTask, string, int](
		ctx,
		retrypool.WithIndependentWorkerFactory(func() retrypool.Worker[MemoryLeakTask] { return NewMemoryLeakWorker() }),
		retrypool.WithIndependentOnTaskSubmitted[MemoryLeakTask](func(task MemoryLeakTask) {
			t.Logf("Task submitted - Group: %s, ID: %d", task.GroupID, task.ID)
		}),
		retrypool.WithIndependentOnTaskStarted[MemoryLeakTask](func(task MemoryLeakTask) {
			t.Logf("Task started - Group: %s, ID: %d", task.GroupID, task.ID)
		}),
		retrypool.WithIndependentOnTaskCompleted[MemoryLeakTask](func(task MemoryLeakTask) {
			t.Logf("Task completed - Group: %s, ID: %d", task.GroupID, task.ID)
			processedTasks.Store(fmt.Sprintf("%s-%d", task.GroupID, task.ID), true)
		}),
		retrypool.WithIndependentOnTaskFailed[MemoryLeakTask](func(task MemoryLeakTask, err error) {
			t.Logf("Task failed - Group: %s, ID: %d, Error: %v", task.GroupID, task.ID, err)
		}),
		retrypool.WithIndependentOnGroupCreated[MemoryLeakTask](func(groupID any) {
			t.Logf("Group created: %v", groupID)
		}),
		retrypool.WithIndependentOnGroupRemoved[MemoryLeakTask](func(groupID any, tasks []MemoryLeakTask) {
			if _, exists := removedGroups.LoadOrStore(groupID, true); !exists {
				t.Logf("Group %v removed for the first time", groupID)
			} else {
				t.Logf("WARNING: Group %v removed again!", groupID)
			}
		}),
		retrypool.WithIndependentOnDeadTask[MemoryLeakTask](func(deadTaskIndex int) {
			task, err := pool.PullDeadTask(deadTaskIndex)
			if err == nil {
				t.Logf("Dead task found - Group: %s, ID: %d", task.Data.GroupID, task.Data.ID)
				groupsWithDeadTasks.Store(task.Data.GroupID, true)
			}
		}),
		retrypool.WithIndependentOnWorkerAdded[MemoryLeakTask](func(workerID int) {
			t.Logf("Worker added: %d", workerID)
		}),
		retrypool.WithIndependentOnWorkerRemoved[MemoryLeakTask](func(workerID int) {
			t.Logf("Worker removed: %d", workerID)
		}),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	initialStats := getMemStats()
	t.Logf("Initial memory state: %+v", initialStats)

	const numGroups = 100
	const tasksPerGroup = 10
	const dataSize = 1024 * 1024 // 1MB of data per task

	t.Logf("Starting task submission - Groups: %d, Tasks per group: %d, Data size: %d bytes",
		numGroups, tasksPerGroup, dataSize)

	// Submit groups with tasks
	for i := 0; i < numGroups; i++ {
		groupID := fmt.Sprintf("group%d", i)
		tasks := make([]MemoryLeakTask, tasksPerGroup)

		for j := 0; j < tasksPerGroup; j++ {
			data := make([]byte, dataSize)
			tasks[j] = MemoryLeakTask{
				ID:      j,
				GroupID: groupID,
				Data:    data,
			}
			if j > 0 {
				tasks[j].Dependencies = []int{j - 1}
			}
		}

		if err := pool.Submit(tasks); err != nil {
			t.Fatalf("Failed to submit tasks for group %s: %v", groupID, err)
		}
		t.Logf("Submitted group %s with %d tasks", groupID, len(tasks))
	}

	// Wait for all groups with progress updates
	deadline := time.Now().Add(30 * time.Second)
	lastUpdate := time.Now()
	updateInterval := time.Second
	for time.Now().Before(deadline) {
		removedCount := 0
		removedGroups.Range(func(_, _ interface{}) bool {
			removedCount++
			return true
		})

		processedCount := 0
		processedTasks.Range(func(_, _ interface{}) bool {
			processedCount++
			return true
		})

		deadTaskCount := 0
		groupsWithDeadTasks.Range(func(_, _ interface{}) bool {
			deadTaskCount++
			return true
		})

		if time.Since(lastUpdate) >= updateInterval {
			t.Logf("Progress - Removed groups: %d/%d, Processed tasks: %d/%d, Groups with dead tasks: %d",
				removedCount, numGroups, processedCount, numGroups*tasksPerGroup, deadTaskCount)
			lastUpdate = time.Now()
		}

		if removedCount == numGroups {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Force garbage collection
	t.Log("Forcing garbage collection...")
	runtime.GC()

	finalStats := getMemStats()

	removedCount := 0
	removedGroups.Range(func(_, _ interface{}) bool {
		removedCount++
		return true
	})

	if removedCount != numGroups {
		t.Errorf("Expected %d groups to be removed, but got %d", numGroups, removedCount)
	}

	memoryGrowth := finalStats.HeapAlloc - initialStats.HeapAlloc
	t.Logf("Memory growth before close: %d bytes", memoryGrowth)

	t.Log("Closing pool...")
	if err := pool.Close(); err != nil {
		t.Fatalf("Failed to close pool: %v", err)
	}

	t.Log("Final garbage collection...")
	runtime.GC()
	postCloseStats := getMemStats()

	t.Logf("Memory metrics:")
	t.Logf("  Initial heap: %d bytes", initialStats.HeapAlloc)
	t.Logf("  Final heap: %d bytes", finalStats.HeapAlloc)
	t.Logf("  Post-close heap: %d bytes", postCloseStats.HeapAlloc)
	t.Logf("  Total growth: %d bytes", postCloseStats.HeapAlloc-initialStats.HeapAlloc)
}
