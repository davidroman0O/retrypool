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

// BlockingTestTask implements DependentTask and includes pool reference for spawning tasks
type BlockingTestTask struct {
	ID           int
	GroupID      string
	Dependencies []int
	ExecuteFunc  func() error
	Pool         *retrypool.BlockingPool[BlockingTestTask, string, int]
}

func (t BlockingTestTask) GetDependencies() []int { return t.Dependencies }
func (t BlockingTestTask) GetGroupID() string     { return t.GroupID }
func (t BlockingTestTask) GetTaskID() int         { return t.ID }
func (t *BlockingTestTask) SetPool(p *retrypool.BlockingPool[BlockingTestTask, string, int]) {
	t.Pool = p
}

// BlockingTestWorker implements Worker for testing
type BlockingTestWorker struct {
	executionTimes map[int]time.Time
	mu             sync.Mutex
}

func (w *BlockingTestWorker) Run(ctx context.Context, task BlockingTestTask) error {
	w.mu.Lock()
	w.executionTimes[task.ID] = time.Now()
	w.mu.Unlock()

	if task.ExecuteFunc != nil {
		return task.ExecuteFunc()
	}
	return nil
}

func TestBlockingPool_Basic(t *testing.T) {
	ctx := context.Background()
	worker := &BlockingTestWorker{executionTimes: make(map[int]time.Time)}

	pool, err := retrypool.NewBlockingPool[BlockingTestTask, string, int](
		ctx,
		retrypool.WithBlockingWorkerFactory(func() retrypool.Worker[BlockingTestTask] { return worker }),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Simple task without dependencies
	task := BlockingTestTask{
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

func TestBlockingPool_Dependencies(t *testing.T) {
	ctx := context.Background()
	worker := &BlockingTestWorker{executionTimes: make(map[int]time.Time)}

	pool, err := retrypool.NewBlockingPool[BlockingTestTask, string, int](
		ctx,
		retrypool.WithBlockingWorkerFactory(func() retrypool.Worker[BlockingTestTask] { return worker }),
	)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Create tasks that will spawn other tasks
	task1 := BlockingTestTask{
		ID:      1,
		GroupID: "group1",
		ExecuteFunc: func() error {
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	}

	task2 := BlockingTestTask{
		ID:           2,
		GroupID:      "group1",
		Dependencies: []int{1},
		ExecuteFunc: func() error {
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	}

	task3 := BlockingTestTask{
		ID:           3,
		GroupID:      "group1",
		Dependencies: []int{2},
		ExecuteFunc: func() error {
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	}

	// Submit tasks in reverse order
	if err := pool.Submit(task3); err != nil {
		t.Fatalf("Failed to submit task 3: %v", err)
	}
	if err := pool.Submit(task2); err != nil {
		t.Fatalf("Failed to submit task 2: %v", err)
	}
	if err := pool.Submit(task1); err != nil {
		t.Fatalf("Failed to submit task 1: %v", err)
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

func TestBlockingPool_StressTest(t *testing.T) {
	ctx := context.Background()
	worker := &BlockingTestWorker{executionTimes: make(map[int]time.Time)}

	numWorkers := 10
	numTasksPerGroup := 100
	numGroups := 5

	pool, err := retrypool.NewBlockingPool[BlockingTestTask, string, int](
		ctx,
		retrypool.WithBlockingWorkerFactory(func() retrypool.Worker[BlockingTestTask] { return worker }),
		retrypool.WithBlockingWorkerLimits[BlockingTestTask](numWorkers, numWorkers),
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
				currentID := i
				currentGroupID := gID

				executeFunc := func() error {
					// Simulate some work
					time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))

					// If not the last task, create next task with dependency
					if currentID < numTasksPerGroup-1 {
						nextTask := BlockingTestTask{
							ID:           currentID + 1,
							GroupID:      currentGroupID,
							Dependencies: []int{currentID},
							ExecuteFunc: func() error {
								time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))
								return nil
							},
						}
						if err := pool.Submit(nextTask); err != nil {
							return fmt.Errorf("failed to submit next task: %v", err)
						}
					}
					return nil
				}

				task := BlockingTestTask{
					ID:          currentID,
					GroupID:     currentGroupID,
					ExecuteFunc: executeFunc,
				}

				if err := pool.Submit(task); err != nil {
					errorChan <- fmt.Errorf("failed to submit task %d in group %s: %v", currentID, currentGroupID, err)
					return
				}

				// Only submit the first task of each group, others will be created dynamically
				break
			}
		}(groupID)
	}

	// Wait for all initial submissions to complete
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
		var lastTime time.Time
		for i := 0; i < numTasksPerGroup; i++ {
			taskTime, exists := worker.executionTimes[i]
			if !exists {
				t.Errorf("Task %d in group %s was not executed", i, groupID)
				continue
			}

			if i > 0 && taskTime.Before(lastTime) {
				t.Errorf("Task %d executed before its dependency in group %s", i, groupID)
			}
			lastTime = taskTime
		}
	}
}
