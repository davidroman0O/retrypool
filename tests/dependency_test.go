package tests

// import (
// 	"context"
// 	"errors"
// 	"fmt"
// 	"math/rand"

// 	"sync"
// 	"sync/atomic"
// 	"testing"
// 	"time"

// 	"github.com/davidroman0O/retrypool"
// )

// /// TODO: make test using OnGroupCompletedChan to manually end the group while using OnGroupCompleted
// /// TODO: make a test with failures and retries
// /// TODO: make a critical failure test

// // SimpleWorker represents a basic worker for testing
// type SimpleWorker[T any] struct {
// 	sync.Mutex
// 	ID           int
// 	Processed    []T
// 	ProcessErr   error
// 	ProcessDelay time.Duration
// }

// func (w *SimpleWorker[T]) Run(ctx context.Context, data T) error {
// 	w.Lock()
// 	defer w.Unlock()
// 	if w.ProcessDelay > 0 {
// 		time.Sleep(w.ProcessDelay)
// 	}
// 	if w.ProcessErr != nil {
// 		return w.ProcessErr
// 	}
// 	fmt.Printf("Worker %d processing data: %v\n", w.ID, data)
// 	w.Processed = append(w.Processed, data)
// 	return nil
// }

// // DependentTaskImpl implements DependentTask for testing
// type DependentTaskImpl struct {
// 	TaskID       string
// 	GroupID      string
// 	Dependencies []string
// }

// func (d *DependentTaskImpl) GetDependencies() []interface{} {
// 	deps := make([]interface{}, len(d.Dependencies))
// 	for i, dep := range d.Dependencies {
// 		deps[i] = dep
// 	}
// 	return deps
// }

// func (d *DependentTaskImpl) GetGroupID() interface{} { return d.GroupID }
// func (d *DependentTaskImpl) GetTaskID() interface{}  { return d.TaskID }
// func (d *DependentTaskImpl) HashID() uint64          { return 0 }

// // TestDependencyPool_Configuration tests configuration validation
// func TestDependencyPool_Configuration(t *testing.T) {
// 	ctx := context.Background()
// 	worker := &SimpleWorker[interface{}]{}
// 	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{worker}, retrypool.WithRoundRobinDistribution[interface{}]())

// 	tests := []struct {
// 		name        string
// 		config      *retrypool.DependencyConfig[interface{}]
// 		expectError bool
// 	}{
// 		{
// 			name: "Valid configuration",
// 			config: &retrypool.DependencyConfig[interface{}]{
// 				EqualsTaskID:  func(a, b interface{}) bool { return a == b },
// 				EqualsGroupID: func(a, b interface{}) bool { return a == b },
// 			},
// 			expectError: false,
// 		},
// 		{
// 			name: "Missing EqualsTaskID",
// 			config: &retrypool.DependencyConfig[interface{}]{
// 				EqualsGroupID: func(a, b interface{}) bool { return a == b },
// 			},
// 			expectError: true,
// 		},
// 		{
// 			name: "Missing EqualsGroupID",
// 			config: &retrypool.DependencyConfig[interface{}]{
// 				EqualsTaskID: func(a, b interface{}) bool { return a == b },
// 			},
// 			expectError: true,
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			_, err := retrypool.NewDependencyPool(pool, tt.config)
// 			if (err != nil) != tt.expectError {
// 				t.Errorf("NewDependencyPool() error = %v, expectError %v", err, tt.expectError)
// 			}
// 		})
// 	}
// }

// // TestDependencyPool_DependencyOrder tests basic dependency ordering
// func TestDependencyPool_DependencyOrder(t *testing.T) {
// 	ctx := context.Background()
// 	worker := &SimpleWorker[interface{}]{}
// 	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{worker}, retrypool.WithRoundRobinDistribution[interface{}]())

// 	var executionOrder []string
// 	var executionMu sync.Mutex
// 	var wg sync.WaitGroup

// 	config := &retrypool.DependencyConfig[interface{}]{
// 		EqualsTaskID:  func(a, b interface{}) bool { return a == b },
// 		EqualsGroupID: func(a, b interface{}) bool { return a == b },
// 		Strategy:      retrypool.DependencyStrategyOrder,
// 		OnTaskProcessed: func(groupID, taskID interface{}) {
// 			executionMu.Lock()
// 			executionOrder = append(executionOrder, taskID.(string))
// 			executionMu.Unlock()
// 			wg.Done()
// 		},

// 		OnGroupRemoved: func(groupID interface{}) {
// 			fmt.Println("Group removed:", groupID)
// 		},
// 	}

// 	dp, err := retrypool.NewDependencyPool(pool, config)
// 	if err != nil {
// 		t.Fatalf("Failed to create dependency pool: %v", err)
// 	}

// 	// Create a dependency chain: A -> B -> C
// 	tasks := []*DependentTaskImpl{
// 		{TaskID: "C", GroupID: "group1", Dependencies: []string{"B"}},
// 		{TaskID: "A", GroupID: "group1"},
// 		{TaskID: "B", GroupID: "group1", Dependencies: []string{"A"}},
// 	}

// 	wg.Add(len(tasks))
// 	for _, task := range tasks {
// 		if err := dp.Submit(task); err != nil {
// 			t.Fatalf("Failed to submit task %s: %v", task.TaskID, err)
// 		}
// 	}

// 	wg.Wait()

// 	// Verify order
// 	expected := []string{"A", "B", "C"}
// 	executionMu.Lock()
// 	if !sliceEqual(executionOrder, expected) {
// 		t.Errorf("Wrong execution order. Expected %v, got %v", expected, executionOrder)
// 	}
// 	executionMu.Unlock()
// }

// // TestDependencyPool_GroupPrioritization tests group prioritization
// func TestDependencyPool_GroupPrioritization(t *testing.T) {
// 	ctx := context.Background()
// 	worker := &SimpleWorker[interface{}]{ProcessDelay: 10 * time.Millisecond}
// 	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{worker}, retrypool.WithRoundRobinDistribution[interface{}]())

// 	var executionOrder []string
// 	var executionMu sync.Mutex
// 	var wg sync.WaitGroup

// 	config := &retrypool.DependencyConfig[interface{}]{
// 		EqualsTaskID:  func(a, b interface{}) bool { return a == b },
// 		EqualsGroupID: func(a, b interface{}) bool { return a == b },
// 		Strategy:      retrypool.DependencyStrategyPriority,
// 		OnTaskProcessed: func(groupID, taskID interface{}) {
// 			executionMu.Lock()
// 			executionOrder = append(executionOrder, fmt.Sprintf("%s:%s", groupID, taskID))
// 			executionMu.Unlock()
// 			wg.Done()
// 		},
// 	}

// 	dp, err := retrypool.NewDependencyPool(pool, config)
// 	if err != nil {
// 		t.Fatalf("Failed to create dependency pool: %v", err)
// 	}

// 	// Create tasks in two groups
// 	tasksGroup1 := []*DependentTaskImpl{
// 		{TaskID: "1A", GroupID: "group1"},
// 		{TaskID: "1B", GroupID: "group1"},
// 	}

// 	tasksGroup2 := []*DependentTaskImpl{
// 		{TaskID: "2A", GroupID: "group2"},
// 		{TaskID: "2B", GroupID: "group2"},
// 	}

// 	// Submit all tasks
// 	wg.Add(len(tasksGroup1) + len(tasksGroup2))

// 	// Submit group1 tasks first
// 	for _, task := range tasksGroup1 {
// 		if err := dp.Submit(task); err != nil {
// 			t.Fatalf("Failed to submit task %s: %v", task.TaskID, err)
// 		}
// 	}

// 	// Small delay to ensure group1 starts processing
// 	time.Sleep(5 * time.Millisecond)

// 	// Submit group2 tasks
// 	for _, task := range tasksGroup2 {
// 		if err := dp.Submit(task); err != nil {
// 			t.Fatalf("Failed to submit task %s: %v", task.TaskID, err)
// 		}
// 	}

// 	wg.Wait()

// 	// Verify all group1 tasks were processed before group2 tasks
// 	executionMu.Lock()
// 	var group1Done bool
// 	var group2Started bool
// 	for _, execution := range executionOrder {
// 		if !group1Done && execution[:6] == "group2" {
// 			group2Started = true
// 		}
// 		if group2Started && execution[:6] == "group1" {
// 			t.Error("Group 2 task executed before Group 1 was complete")
// 		}
// 	}
// 	executionMu.Unlock()
// }

// // TestDependencyPool_FailureHandling tests how failures are handled
// func TestDependencyPool_FailureHandling(t *testing.T) {
// 	ctx := context.Background()
// 	worker := &SimpleWorker[interface{}]{}
// 	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{worker}, retrypool.WithRoundRobinDistribution[interface{}]())

// 	var failureHandled bool
// 	var failureMu sync.Mutex
// 	var wg sync.WaitGroup

// 	config := &retrypool.DependencyConfig[interface{}]{
// 		EqualsTaskID:  func(a, b interface{}) bool { return a == b },
// 		EqualsGroupID: func(a, b interface{}) bool { return a == b },
// 		OnDependencyFailure: func(groupID, taskID interface{}, deps []interface{}, reason string) retrypool.TaskAction {
// 			failureMu.Lock()
// 			failureHandled = true
// 			failureMu.Unlock()
// 			return retrypool.TaskActionAddToDeadTasks
// 		},
// 	}

// 	dp, err := retrypool.NewDependencyPool(pool, config)
// 	if err != nil {
// 		t.Fatalf("Failed to create dependency pool: %v", err)
// 	}

// 	// Create a failing task
// 	worker.ProcessErr = errors.New("simulated failure")
// 	task := &DependentTaskImpl{
// 		TaskID:  "failing",
// 		GroupID: "group1",
// 	}

// 	wg.Add(1)
// 	if err := dp.Submit(task); err != nil {
// 		t.Fatalf("Failed to submit task: %v", err)
// 	}

// 	// Wait for failure handling
// 	time.Sleep(100 * time.Millisecond)

// 	failureMu.Lock()
// 	if !failureHandled {
// 		t.Error("Failure was not handled")
// 	}
// 	failureMu.Unlock()
// }

// // TestDependencyPool_DynamicWorkers tests dynamic worker creation
// func TestDependencyPool_DynamicWorkers(t *testing.T) {
// 	ctx := context.Background()
// 	var workerCount atomic.Int32

// 	workerFactory := func() retrypool.Worker[interface{}] {
// 		id := workerCount.Add(1)
// 		return &SimpleWorker[interface{}]{ID: int(id)}
// 	}

// 	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{workerFactory()}, retrypool.WithRoundRobinDistribution[interface{}]())

// 	config := &retrypool.DependencyConfig[interface{}]{
// 		EqualsTaskID:      func(a, b interface{}) bool { return a == b },
// 		EqualsGroupID:     func(a, b interface{}) bool { return a == b },
// 		AutoCreateWorkers: true,
// 		MaxDynamicWorkers: 3,
// 		WorkerFactory:     workerFactory,
// 	}

// 	dp, err := retrypool.NewDependencyPool(pool, config)
// 	if err != nil {
// 		t.Fatalf("Failed to create dependency pool: %v", err)
// 	}

// 	// Submit enough tasks to trigger worker creation
// 	for i := 0; i < 10; i++ {
// 		task := &DependentTaskImpl{
// 			TaskID:  fmt.Sprintf("task%d", i),
// 			GroupID: "group1",
// 		}
// 		if err := dp.Submit(task); err != nil {
// 			t.Fatalf("Failed to submit task: %v", err)
// 		}
// 	}

// 	// Wait for worker creation
// 	time.Sleep(100 * time.Millisecond)

// 	finalCount := workerCount.Load()
// 	if finalCount > int32(config.MaxDynamicWorkers)+1 {
// 		t.Errorf("Too many workers created. Got %d, expected <= %d",
// 			finalCount, config.MaxDynamicWorkers+1)
// 	}
// }

// // Helper function to compare slices
// func sliceEqual(a, b []string) bool {
// 	if len(a) != len(b) {
// 		return false
// 	}
// 	for i := range a {
// 		if a[i] != b[i] {
// 			return false
// 		}
// 	}
// 	return true
// }

// // TestDependencyPool_ManualGroupCompletion tests manual group completion using OnGroupCompletedChan
// func TestDependencyPool_ManualGroupCompletion(t *testing.T) {
// 	ctx := context.Background()
// 	worker := &SimpleWorker[interface{}]{}
// 	pool := retrypool.New(ctx, []retrypool.Worker[interface{}]{worker}, retrypool.WithRoundRobinDistribution[interface{}]())

// 	completionChan := make(chan *retrypool.RequestResponse[interface{}, error])
// 	var groupCompletedCalled bool
// 	var mu sync.Mutex

// 	config := &retrypool.DependencyConfig[interface{}]{
// 		EqualsTaskID:  func(a, b interface{}) bool { return a == b },
// 		EqualsGroupID: func(a, b interface{}) bool { return a == b },
// 		OnGroupCompleted: func(groupID interface{}) {
// 			mu.Lock()
// 			groupCompletedCalled = true
// 			mu.Unlock()
// 			fmt.Println("Group completed:", groupID)
// 		},
// 		OnGroupCompletedChan: completionChan,
// 		OnGroupRemoved: func(groupID interface{}) {
// 			fmt.Println("Group removed:", groupID)
// 		},
// 	}

// 	dp, err := retrypool.NewDependencyPool(pool, config)
// 	if err != nil {
// 		t.Fatalf("Failed to create dependency pool: %v", err)
// 	}

// 	// Submit tasks
// 	tasks := []*DependentTaskImpl{
// 		{TaskID: "A", GroupID: "group1"},
// 		{TaskID: "B", GroupID: "group1", Dependencies: []string{"A"}},
// 	}

// 	for _, task := range tasks {
// 		if err := dp.Submit(task); err != nil {
// 			t.Fatalf("Failed to submit task %s: %v", task.TaskID, err)
// 		}
// 	}

// 	// Wait for tasks to complete
// 	time.Sleep(1 * time.Second)

// 	// Request group completion
// 	req := retrypool.NewRequestResponse[interface{}, error]("group1")
// 	completionChan <- req

// 	// Wait for completion response
// 	select {
// 	case <-req.Done():
// 		if err := req.Err(); err != nil {
// 			t.Errorf("Group completion failed: %v", err)
// 		}
// 	case <-time.After(time.Second):
// 		t.Error("Group completion timed out")
// 	}

// 	mu.Lock()
// 	if !groupCompletedCalled {
// 		t.Error("OnGroupCompleted was not called")
// 	}
// 	mu.Unlock()
// }

// // StressWorker is a worker that can simulate various conditions
// type StressWorker struct {
// 	ID               int
// 	processedCount   atomic.Int64
// 	failureRate      float64          // 0.0 to 1.0
// 	processingTime   [2]time.Duration // min and max processing time
// 	panicProbability float64          // 0.0 to 1.0
// }

// func (w *StressWorker) Run(ctx context.Context, data interface{}) error {
// 	w.processedCount.Add(1)

// 	// Simulate random processing time
// 	processingTime := w.processingTime[0] + time.Duration(rand.Float64())*
// 		(w.processingTime[1]-w.processingTime[0])

// 	select {
// 	case <-ctx.Done():
// 		return ctx.Err()
// 	case <-time.After(processingTime):
// 	}

// 	// Simulate random panic
// 	if rand.Float64() < w.panicProbability {
// 		panic(fmt.Sprintf("Simulated panic in worker %d", w.ID))
// 	}

// 	// Simulate random failure
// 	if rand.Float64() < w.failureRate {
// 		return fmt.Errorf("simulated failure in worker %d", w.ID)
// 	}

// 	return nil
// }

// // ComplexTask represents a task with multiple dependencies
// type ComplexTask struct {
// 	TaskID       string
// 	GroupID      string
// 	Dependencies []string
// 	Priority     int
// 	Size         int // Simulated workload size
// }

// func (t *ComplexTask) GetDependencies() []interface{} {
// 	deps := make([]interface{}, len(t.Dependencies))
// 	for i, dep := range t.Dependencies {
// 		deps[i] = dep
// 	}
// 	return deps
// }

// func (t *ComplexTask) GetGroupID() interface{} { return t.GroupID }
// func (t *ComplexTask) GetTaskID() interface{}  { return t.TaskID }
// func (t *ComplexTask) HashID() uint64          { return 0 }

// func TestDependencyPool_StressTest(t *testing.T) {
// 	// Test configuration
// 	const (
// 		numGroups         = 20
// 		tasksPerGroup     = 15
// 		maxDependencies   = 5
// 		numWorkers        = 4
// 		testDuration      = 30 * time.Second
// 		workerFailureRate = 0.1
// 		workerPanicRate   = 0.01
// 	)

// 	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
// 	defer cancel()

// 	// Create workers with varying characteristics
// 	workers := make([]retrypool.Worker[interface{}], numWorkers)
// 	for i := range workers {
// 		workers[i] = &StressWorker{
// 			ID:          i,
// 			failureRate: workerFailureRate,
// 			processingTime: [2]time.Duration{
// 				50 * time.Millisecond,  // min processing time
// 				200 * time.Millisecond, // max processing time
// 			},
// 			panicProbability: workerPanicRate,
// 		}
// 	}

// 	// Create pool with round-robin distribution
// 	pool := retrypool.New(ctx, workers, retrypool.WithRoundRobinDistribution[interface{}]())

// 	// Metrics tracking
// 	var (
// 		tasksSubmitted  atomic.Int64
// 		tasksCompleted  atomic.Int64
// 		tasksFailed     atomic.Int64
// 		groupsCompleted atomic.Int64
// 		deadTasks       atomic.Int64
// 	)

// 	// Create dependency pool configuration
// 	config := &retrypool.DependencyConfig[interface{}]{
// 		EqualsTaskID:  func(a, b interface{}) bool { return a == b },
// 		EqualsGroupID: func(a, b interface{}) bool { return a == b },
// 		Strategy:      retrypool.DependencyStrategyPriority,
// 		// MaxDynamicWorkers: 2, // Allow dynamic worker creation
// 		AutoCreateWorkers: true,
// 		WorkerFactory: func() retrypool.Worker[interface{}] {
// 			return &StressWorker{
// 				ID:          numWorkers + int(rand.Int31n(1000)),
// 				failureRate: workerFailureRate,
// 				processingTime: [2]time.Duration{
// 					50 * time.Millisecond,
// 					200 * time.Millisecond,
// 				},
// 				panicProbability: workerPanicRate,
// 			}
// 		},
// 		OnTaskProcessed: func(groupID, taskID interface{}) {
// 			tasksCompleted.Add(1)
// 		},
// 		OnGroupCompleted: func(groupID interface{}) {
// 			groupsCompleted.Add(1)
// 		},
// 		OnDependencyFailure: func(groupID, taskID interface{}, deps []interface{}, reason string) retrypool.TaskAction {
// 			tasksFailed.Add(1)
// 			if rand.Float64() < 0.5 {
// 				return retrypool.TaskActionRetry
// 			}
// 			deadTasks.Add(1)
// 			return retrypool.TaskActionAddToDeadTasks
// 		},
// 	}

// 	dp, err := retrypool.NewDependencyPool(pool, config)
// 	if err != nil {
// 		t.Fatalf("Failed to create dependency pool: %v", err)
// 	}

// 	// Channel to signal test completion
// 	done := make(chan struct{})
// 	var wg sync.WaitGroup

// 	// Start task submission goroutine
// 	go func() {
// 		defer close(done)

// 		for groupNum := 0; groupNum < numGroups; groupNum++ {
// 			select {
// 			case <-ctx.Done():
// 				return
// 			default:
// 			}

// 			// Create tasks for this group with random dependencies
// 			tasks := make([]*ComplexTask, tasksPerGroup)
// 			for i := 0; i < tasksPerGroup; i++ {
// 				task := &ComplexTask{
// 					TaskID:   fmt.Sprintf("g%d-t%d", groupNum, i),
// 					GroupID:  fmt.Sprintf("group-%d", groupNum),
// 					Priority: rand.Intn(5),
// 					Size:     rand.Intn(100) + 50,
// 				}

// 				// Add random dependencies to previous tasks in the same group
// 				maxDeps := min(i, maxDependencies)
// 				if maxDeps > 0 {
// 					numDeps := rand.Intn(maxDeps + 1)
// 					task.Dependencies = make([]string, numDeps)
// 					for j := 0; j < numDeps; j++ {
// 						depIndex := rand.Intn(i)
// 						task.Dependencies[j] = fmt.Sprintf("g%d-t%d", groupNum, depIndex)
// 					}
// 				}

// 				tasks[i] = task
// 			}

// 			// Submit tasks in random order
// 			indices := rand.Perm(len(tasks))
// 			for _, idx := range indices {
// 				wg.Add(1)
// 				task := tasks[idx]
// 				go func(t *ComplexTask) {
// 					defer wg.Done()
// 					if err := dp.Submit(t); err != nil {
// 						fmt.Printf("Failed to submit task %s: %v\n", t.TaskID, err)
// 						return
// 					}
// 					tasksSubmitted.Add(1)
// 				}(task)
// 			}

// 			// Add random delays between groups
// 			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
// 		}
// 	}()

// 	// Wait for completion or timeout
// 	select {
// 	case <-ctx.Done():
// 		t.Log("Test timed out")
// 	case <-done:
// 		wg.Wait()
// 		t.Log("All tasks submitted")
// 	}

// 	// Log final metrics
// 	t.Logf("Test Results:")
// 	t.Logf("Tasks Submitted: %d", tasksSubmitted.Load())
// 	t.Logf("Tasks Completed: %d", tasksCompleted.Load())
// 	t.Logf("Tasks Failed: %d", tasksFailed.Load())
// 	t.Logf("Groups Completed: %d", groupsCompleted.Load())
// 	t.Logf("Dead Tasks: %d", deadTasks.Load())

// 	// Verify results
// 	if tasksSubmitted.Load() == 0 {
// 		t.Error("No tasks were submitted")
// 	}

// 	if tasksCompleted.Load() == 0 {
// 		t.Error("No tasks were completed")
// 	}

// 	expectedGroups := min(numGroups, int(float64(testDuration)/float64(time.Second)))
// 	if groupsCompleted.Load() < int64(expectedGroups/2) {
// 		t.Errorf("Too few groups completed. Expected at least %d, got %d",
// 			expectedGroups/2, groupsCompleted.Load())
// 	}

// 	// Check worker stats using the correct Workers() signature
// 	workerIDs, err := pool.Workers()
// 	if err != nil {
// 		t.Errorf("Failed to get worker IDs: %v", err)
// 	} else {
// 		for _, workerID := range workerIDs {
// 			activeCount, totalProcessed, err := dp.GetWorkerStats(workerID)
// 			if err != nil {
// 				t.Errorf("Failed to get worker stats for worker %d: %v", workerID, err)
// 				continue
// 			}
// 			t.Logf("Worker %d - Active: %d, Total Processed: %d",
// 				workerID, activeCount, totalProcessed)
// 		}
// 	}
// }

// func min(a, b int) int {
// 	if a < b {
// 		return a
// 	}
// 	return b
// }
