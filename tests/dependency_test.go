package tests

// import (
// 	"context"
// 	"fmt"
// 	"sync"
// 	"testing"
// 	"time"

// 	"github.com/davidroman0O/retrypool"
// )

// // TestTask implements the DependentTask interface
// type TestTask struct {
// 	ID           int
// 	Group        string
// 	Dependencies []int
// 	dp           *retrypool.DependencyPool[TestTask, string, int] // For inside tests
// 	log          *ExecutionRecorder
// 	done         chan struct{} // For synchronization in inside tests
// }

// func (t TestTask) GetDependencies() []int { return t.Dependencies }
// func (t TestTask) GetGroupID() string     { return t.Group }
// func (t TestTask) GetTaskID() int         { return t.ID }

// // ExecutionRecorder tracks task execution order
// type ExecutionRecorder struct {
// 	mu     sync.Mutex
// 	events []ExecutionEvent
// }

// type ExecutionEvent struct {
// 	Group     string
// 	ID        int
// 	Type      string
// 	Timestamp time.Time
// }

// func NewExecutionRecorder() *ExecutionRecorder {
// 	return &ExecutionRecorder{
// 		events: make([]ExecutionEvent, 0),
// 	}
// }

// func (r *ExecutionRecorder) RecordStart(group string, id int) {
// 	r.mu.Lock()
// 	defer r.mu.Unlock()
// 	r.events = append(r.events, ExecutionEvent{
// 		Group:     group,
// 		ID:        id,
// 		Type:      "start",
// 		Timestamp: time.Now(),
// 	})
// }

// func (r *ExecutionRecorder) RecordComplete(group string, id int) {
// 	r.mu.Lock()
// 	defer r.mu.Unlock()
// 	r.events = append(r.events, ExecutionEvent{
// 		Group:     group,
// 		ID:        id,
// 		Type:      "complete",
// 		Timestamp: time.Now(),
// 	})
// }

// func (r *ExecutionRecorder) GetEvents() []ExecutionEvent {
// 	r.mu.Lock()
// 	defer r.mu.Unlock()
// 	result := make([]ExecutionEvent, len(r.events))
// 	copy(result, r.events)
// 	return result
// }

// // Worker implementations
// type OutsideWorker struct {
// 	recorder *ExecutionRecorder
// }

// func (w *OutsideWorker) Run(ctx context.Context, data TestTask) error {
// 	w.recorder.RecordStart(data.Group, data.ID)
// 	time.Sleep(100 * time.Millisecond)
// 	w.recorder.RecordComplete(data.Group, data.ID)
// 	return nil
// }

// type InsideWorker struct {
// 	recorder *ExecutionRecorder
// }

// func (w *InsideWorker) Run(ctx context.Context, data TestTask) error {
// 	w.recorder.RecordStart(data.Group, data.ID)

// 	// For tasks that need to create next tasks
// 	if data.dp != nil && data.ID < 3 {
// 		nextID := data.ID + 1
// 		nextDone := make(chan struct{})

// 		nextTask := TestTask{
// 			ID:           nextID,
// 			Group:        data.Group,
// 			Dependencies: []int{data.ID},
// 			dp:           data.dp,
// 			log:          data.log,
// 			done:         nextDone,
// 		}

// 		if err := data.dp.Submit(nextTask); err != nil {
// 			return err
// 		}

// 		<-nextDone // Wait for child task to complete
// 	}

// 	time.Sleep(100 * time.Millisecond)
// 	w.recorder.RecordComplete(data.Group, data.ID)

// 	if data.done != nil {
// 		close(data.done)
// 	}

// 	return nil
// }

// // Helper function to verify execution order
// func verifyExecutionOrder(t *testing.T, events []ExecutionEvent, expectedOrder []struct {
// 	ID    int
// 	Type  string
// 	Group string
// }) {
// 	t.Helper()

// 	if len(events) != len(expectedOrder) {
// 		t.Errorf("Execution order length mismatch: expected %d events, got %d events", len(expectedOrder), len(events))
// 		return
// 	}

// 	// First check if we have any mismatch
// 	hasMismatch := false
// 	for i, expected := range expectedOrder {
// 		if events[i].Group != expected.Group ||
// 			events[i].ID != expected.ID ||
// 			events[i].Type != expected.Type {
// 			hasMismatch = true
// 			break
// 		}
// 	}

// 	if hasMismatch {
// 		t.Errorf("Execution order mismatch\nExpected sequence:")
// 		for i, exp := range expectedOrder {
// 			t.Errorf("  %d. %s-%d (%s)", i, exp.Group, exp.ID, exp.Type)
// 		}
// 		t.Errorf("\nActual sequence:")
// 		for i, got := range events {
// 			t.Errorf("  %d. %s-%d (%s)", i, got.Group, got.ID, got.Type)
// 		}

// 		for i, expected := range expectedOrder {
// 			if events[i].Group != expected.Group ||
// 				events[i].ID != expected.ID ||
// 				events[i].Type != expected.Type {
// 				t.Errorf("\nMismatch at position %d:\n  Expected: %s-%d (%s)\n  Got:      %s-%d (%s)",
// 					i, expected.Group, expected.ID, expected.Type,
// 					events[i].Group, events[i].ID, events[i].Type)
// 			}
// 		}
// 	}
// }

// // Test Blocking from Outside (tasks submitted externally)
// func TestDependency_BlockingOutsideForward(t *testing.T) {
// 	recorder := NewExecutionRecorder()
// 	workers := []retrypool.Worker[TestTask]{
// 		&OutsideWorker{recorder},
// 		&OutsideWorker{recorder},
// 	}

// 	pool := retrypool.New(context.Background(), workers)
// 	dp, err := retrypool.NewDependencyPool[TestTask, string, int](
// 		pool,
// 		func() retrypool.Worker[TestTask] {
// 			return &OutsideWorker{recorder}
// 		},
// 		retrypool.DependencyConfig[TestTask, string, int]{
// 			MaxWorkers:     5,
// 			MinWorkers:     1,
// 			ExecutionOrder: retrypool.ExecutionOrderForward,
// 			TaskMode:       retrypool.TaskModeBlocking,
// 		},
// 	)
// 	if err != nil {
// 		t.Fatalf("Failed to create dependency pool: %v", err)
// 	}

// 	tasks := []TestTask{
// 		TestTask{
// 			ID:           2,
// 			Group:        "groupA",
// 			Dependencies: []int{1},
// 		},
// 		TestTask{
// 			ID:           1,
// 			Group:        "groupA",
// 			Dependencies: []int{},
// 		},
// 		TestTask{
// 			ID:           3,
// 			Group:        "groupA",
// 			Dependencies: []int{2},
// 		},
// 	}

// 	for _, task := range tasks {
// 		if err := dp.Submit(task); err != nil {
// 			t.Fatalf("Failed to submit task: %v", err)
// 		}
// 	}

// 	dp.WaitWithCallback(context.Background(), func(queueSize, processingCount, deadTaskCount int) bool {
// 		return queueSize > 0 || processingCount > 0
// 	}, time.Second)

// 	expectedOrder := []struct {
// 		ID    int
// 		Type  string
// 		Group string
// 	}{
// 		{1, "start", "groupA"},
// 		{2, "start", "groupA"},
// 		{3, "start", "groupA"},
// 		{3, "complete", "groupA"},
// 		{2, "complete", "groupA"},
// 		{1, "complete", "groupA"},
// 	}

// 	<-time.After(time.Second)
// 	verifyExecutionOrder(t, recorder.GetEvents(), expectedOrder)
// }

// func TestDependency_BlockingOutsideReverse(t *testing.T) {
// 	recorder := NewExecutionRecorder()
// 	workers := []retrypool.Worker[TestTask]{
// 		&OutsideWorker{recorder},
// 		&OutsideWorker{recorder},
// 	}

// 	pool := retrypool.New(context.Background(), workers)
// 	dp, err := retrypool.NewDependencyPool[TestTask, string, int](
// 		pool,
// 		func() retrypool.Worker[TestTask] {
// 			return &OutsideWorker{recorder}
// 		},
// 		retrypool.DependencyConfig[TestTask, string, int]{
// 			MaxWorkers:     5,
// 			MinWorkers:     1,
// 			ExecutionOrder: retrypool.ExecutionOrderReverse,
// 			TaskMode:       retrypool.TaskModeBlocking,
// 		},
// 	)
// 	if err != nil {
// 		t.Fatalf("Failed to create dependency pool: %v", err)
// 	}

// 	tasks := []TestTask{
// 		{
// 			ID:           3,
// 			Group:        "groupB",
// 			Dependencies: []int{},
// 		},
// 		{
// 			ID:           2,
// 			Group:        "groupB",
// 			Dependencies: []int{3},
// 		},
// 		{
// 			ID:           1,
// 			Group:        "groupB",
// 			Dependencies: []int{2},
// 		},
// 	}

// 	for _, task := range tasks {
// 		if err := dp.Submit(task); err != nil {
// 			t.Fatalf("Failed to submit task: %v", err)
// 		}
// 	}

// 	dp.WaitWithCallback(context.Background(), func(queueSize, processingCount, deadTaskCount int) bool {
// 		return queueSize > 0 || processingCount > 0
// 	}, time.Second)

// 	expectedOrder := []struct {
// 		ID    int
// 		Type  string
// 		Group string
// 	}{
// 		{3, "start", "groupB"},
// 		{2, "start", "groupB"},
// 		{1, "start", "groupB"},
// 		{1, "complete", "groupB"},
// 		{2, "complete", "groupB"},
// 		{3, "complete", "groupB"},
// 	}

// 	<-time.After(time.Second)
// 	verifyExecutionOrder(t, recorder.GetEvents(), expectedOrder)
// }

// // Test Blocking from Inside (tasks create other tasks)
// func TestDependency_BlockingInsideForward(t *testing.T) {
// 	recorder := NewExecutionRecorder()
// 	workers := []retrypool.Worker[TestTask]{
// 		&InsideWorker{recorder},
// 		&InsideWorker{recorder},
// 	}

// 	pool := retrypool.New(context.Background(), workers)
// 	dp, err := retrypool.NewDependencyPool[TestTask, string, int](
// 		pool,
// 		func() retrypool.Worker[TestTask] {
// 			return &InsideWorker{recorder}
// 		},
// 		retrypool.DependencyConfig[TestTask, string, int]{
// 			MaxWorkers:     5,
// 			MinWorkers:     1,
// 			ExecutionOrder: retrypool.ExecutionOrderForward,
// 			TaskMode:       retrypool.TaskModeBlocking,
// 		},
// 	)
// 	if err != nil {
// 		t.Fatalf("Failed to create dependency pool: %v", err)
// 	}

// 	// Start with task 1, it will create tasks 2 and 3
// 	doneChan := make(chan struct{})
// 	initialTask := TestTask{
// 		ID:           1,
// 		Group:        "groupA",
// 		Dependencies: []int{},
// 		dp:           dp,
// 		log:          recorder,
// 		done:         doneChan,
// 	}

// 	if err := dp.Submit(initialTask); err != nil {
// 		t.Fatalf("Failed to submit initial task: %v", err)
// 	}

// 	<-doneChan // Wait for all tasks to complete

// 	expectedOrder := []struct {
// 		ID    int
// 		Type  string
// 		Group string
// 	}{
// 		{1, "start", "groupA"},
// 		{2, "start", "groupA"},
// 		{3, "start", "groupA"},
// 		{3, "complete", "groupA"},
// 		{2, "complete", "groupA"},
// 		{1, "complete", "groupA"},
// 	}

// 	<-time.After(time.Second)
// 	verifyExecutionOrder(t, recorder.GetEvents(), expectedOrder)
// }

// func TestDependency_BlockingInsideReverse(t *testing.T) {
// 	recorder := NewExecutionRecorder()
// 	workers := []retrypool.Worker[TestTask]{
// 		&InsideWorker{recorder},
// 		&InsideWorker{recorder},
// 	}

// 	pool := retrypool.New(context.Background(), workers)
// 	dp, err := retrypool.NewDependencyPool[TestTask, string, int](
// 		pool,
// 		func() retrypool.Worker[TestTask] {
// 			return &InsideWorker{recorder}
// 		},
// 		retrypool.DependencyConfig[TestTask, string, int]{
// 			MaxWorkers:     5,
// 			MinWorkers:     1,
// 			ExecutionOrder: retrypool.ExecutionOrderReverse,
// 			TaskMode:       retrypool.TaskModeBlocking,
// 		},
// 	)
// 	if err != nil {
// 		t.Fatalf("Failed to create dependency pool: %v", err)
// 	}

// 	// Start with task 3, it will create tasks 2 and 1
// 	doneChan := make(chan struct{})
// 	initialTask := TestTask{
// 		ID:           3,
// 		Group:        "groupB",
// 		Dependencies: []int{},
// 		dp:           dp,
// 		log:          recorder,
// 		done:         doneChan,
// 	}

// 	if err := dp.Submit(initialTask); err != nil {
// 		t.Fatalf("Failed to submit initial task: %v", err)
// 	}

// 	<-doneChan // Wait for all tasks to complete

// 	expectedOrder := []struct {
// 		ID    int
// 		Type  string
// 		Group string
// 	}{
// 		{3, "start", "groupB"},
// 		{2, "start", "groupB"},
// 		{1, "start", "groupB"},
// 		{1, "complete", "groupB"},
// 		{2, "complete", "groupB"},
// 		{3, "complete", "groupB"},
// 	}

// 	<-time.After(time.Second)
// 	verifyExecutionOrder(t, recorder.GetEvents(), expectedOrder)
// }

// // Test Non-Blocking modes
// func TestDependency_NonBlockingOutsideForward(t *testing.T) {
// 	recorder := NewExecutionRecorder()
// 	workers := []retrypool.Worker[TestTask]{
// 		&OutsideWorker{recorder},
// 		&OutsideWorker{recorder},
// 	}

// 	pool := retrypool.New(context.Background(), workers)
// 	dp, err := retrypool.NewDependencyPool[TestTask, string, int](
// 		pool,
// 		func() retrypool.Worker[TestTask] {
// 			return &OutsideWorker{recorder}
// 		},
// 		retrypool.DependencyConfig[TestTask, string, int]{
// 			MaxWorkers:     5,
// 			MinWorkers:     1,
// 			ExecutionOrder: retrypool.ExecutionOrderForward,
// 			TaskMode:       retrypool.TaskModeIndependent,
// 		},
// 	)
// 	if err != nil {
// 		t.Fatalf("Failed to create dependency pool: %v", err)
// 	}

// 	tasks := []TestTask{
// 		TestTask{
// 			ID:           2,
// 			Group:        "groupA",
// 			Dependencies: []int{1},
// 		},
// 		TestTask{
// 			ID:           1,
// 			Group:        "groupA",
// 			Dependencies: []int{},
// 		},
// 		TestTask{
// 			ID:           3,
// 			Group:        "groupA",
// 			Dependencies: []int{2},
// 		},
// 	}

// 	for _, task := range tasks {
// 		if err := dp.Submit(task); err != nil {
// 			t.Fatalf("Failed to submit task: %v", err)
// 		}
// 	}

// 	dp.WaitWithCallback(context.Background(), func(queueSize, processingCount, deadTaskCount int) bool {
// 		return queueSize > 0 || processingCount > 0
// 	}, time.Second)

// 	expectedOrder := []struct {
// 		ID    int
// 		Type  string
// 		Group string
// 	}{
// 		{1, "start", "groupA"},
// 		{1, "complete", "groupA"},
// 		{2, "start", "groupA"},
// 		{2, "complete", "groupA"},
// 		{3, "start", "groupA"},
// 		{3, "complete", "groupA"},
// 	}

// 	<-time.After(time.Second)
// 	verifyExecutionOrder(t, recorder.GetEvents(), expectedOrder)
// }

// func TestDependency_NonBlockingOutsideReverse(t *testing.T) {
// 	recorder := NewExecutionRecorder()
// 	workers := []retrypool.Worker[TestTask]{
// 		&OutsideWorker{recorder},
// 		&OutsideWorker{recorder},
// 	}

// 	pool := retrypool.New(context.Background(), workers)
// 	dp, err := retrypool.NewDependencyPool[TestTask, string, int](
// 		pool,
// 		func() retrypool.Worker[TestTask] {
// 			return &OutsideWorker{recorder}
// 		},
// 		retrypool.DependencyConfig[TestTask, string, int]{
// 			MaxWorkers:     5,
// 			MinWorkers:     1,
// 			ExecutionOrder: retrypool.ExecutionOrderReverse,
// 			TaskMode:       retrypool.TaskModeIndependent,
// 		},
// 	)
// 	if err != nil {
// 		t.Fatalf("Failed to create dependency pool: %v", err)
// 	}

// 	tasks := []TestTask{
// 		{
// 			ID:           3,
// 			Group:        "groupB",
// 			Dependencies: []int{},
// 		},
// 		{
// 			ID:           2,
// 			Group:        "groupB",
// 			Dependencies: []int{3},
// 		},
// 		{
// 			ID:           1,
// 			Group:        "groupB",
// 			Dependencies: []int{2},
// 		},
// 	}

// 	for _, task := range tasks {
// 		if err := dp.Submit(task); err != nil {
// 			t.Fatalf("Failed to submit task: %v", err)
// 		}
// 	}

// 	dp.WaitWithCallback(context.Background(), func(queueSize, processingCount, deadTaskCount int) bool {
// 		return queueSize > 0 || processingCount > 0
// 	}, time.Second)

// 	expectedOrder := []struct {
// 		ID    int
// 		Type  string
// 		Group string
// 	}{
// 		{3, "start", "groupB"},
// 		{3, "complete", "groupB"},
// 		{2, "start", "groupB"},
// 		{2, "complete", "groupB"},
// 		{1, "start", "groupB"},
// 		{1, "complete", "groupB"},
// 	}

// 	<-time.After(time.Second)
// 	verifyExecutionOrder(t, recorder.GetEvents(), expectedOrder)
// }

// type IndependentInsideWorker struct {
// 	recorder *ExecutionRecorder
// }

// func (w *IndependentInsideWorker) Run(ctx context.Context, data TestTask) error {
// 	w.recorder.RecordStart(data.Group, data.ID)

// 	// For tasks that need to create next tasks
// 	if data.dp != nil && data.ID < 3 {
// 		nextID := data.ID + 1
// 		nextTask := TestTask{
// 			ID:           nextID,
// 			Group:        data.Group,
// 			Dependencies: []int{data.ID},
// 			dp:           data.dp,
// 			log:          data.log,
// 		}

// 		// Just submit the next task and continue - don't wait!
// 		if err := data.dp.Submit(nextTask); err != nil {
// 			return err
// 		}
// 	}

// 	time.Sleep(100 * time.Millisecond)
// 	w.recorder.RecordComplete(data.Group, data.ID)

// 	if data.done != nil {
// 		close(data.done)
// 	}

// 	return nil
// }

// func TestDependency_NonBlockingInsideForward(t *testing.T) {
// 	recorder := NewExecutionRecorder()
// 	workers := []retrypool.Worker[TestTask]{
// 		&IndependentInsideWorker{recorder},
// 		&IndependentInsideWorker{recorder},
// 	}

// 	pool := retrypool.New(context.Background(), workers)
// 	dp, err := retrypool.NewDependencyPool[TestTask, string, int](
// 		pool,
// 		func() retrypool.Worker[TestTask] {
// 			return &IndependentInsideWorker{recorder}
// 		},
// 		retrypool.DependencyConfig[TestTask, string, int]{
// 			MaxWorkers:     5,
// 			MinWorkers:     1,
// 			ExecutionOrder: retrypool.ExecutionOrderForward,
// 			TaskMode:       retrypool.TaskModeIndependent,
// 		},
// 	)
// 	if err != nil {
// 		t.Fatalf("Failed to create dependency pool: %v", err)
// 	}

// 	// Start with task 1, it will create tasks 2 and 3
// 	doneChan := make(chan struct{})
// 	initialTask := TestTask{
// 		ID:           1,
// 		Group:        "groupA",
// 		Dependencies: []int{},
// 		dp:           dp,
// 		log:          recorder,
// 		done:         doneChan,
// 	}

// 	if err := dp.Submit(initialTask); err != nil {
// 		t.Fatalf("Failed to submit initial task: %v", err)
// 	}

// 	<-doneChan // Wait for all tasks to complete

// 	expectedOrder := []struct {
// 		ID    int
// 		Type  string
// 		Group string
// 	}{
// 		{1, "start", "groupA"},
// 		{1, "complete", "groupA"},
// 		{2, "start", "groupA"},
// 		{2, "complete", "groupA"},
// 		{3, "start", "groupA"},
// 		{3, "complete", "groupA"},
// 	}

// 	<-time.After(time.Second)
// 	verifyExecutionOrder(t, recorder.GetEvents(), expectedOrder)
// }

// func TestDependency_NonBlockingInsideReverse(t *testing.T) {
// 	recorder := NewExecutionRecorder()
// 	workers := []retrypool.Worker[TestTask]{
// 		&IndependentInsideWorker{recorder},
// 		&IndependentInsideWorker{recorder},
// 	}

// 	pool := retrypool.New(context.Background(), workers)
// 	dp, err := retrypool.NewDependencyPool[TestTask, string, int](
// 		pool,
// 		func() retrypool.Worker[TestTask] {
// 			return &IndependentInsideWorker{recorder}
// 		},
// 		retrypool.DependencyConfig[TestTask, string, int]{
// 			MaxWorkers:     5,
// 			MinWorkers:     1,
// 			ExecutionOrder: retrypool.ExecutionOrderReverse,
// 			TaskMode:       retrypool.TaskModeIndependent,
// 		},
// 	)
// 	if err != nil {
// 		t.Fatalf("Failed to create dependency pool: %v", err)
// 	}

// 	// Start with task 3, it will create tasks 2 and 1
// 	doneChan := make(chan struct{})
// 	initialTask := TestTask{
// 		ID:           3,
// 		Group:        "groupB",
// 		Dependencies: []int{},
// 		dp:           dp,
// 		log:          recorder,
// 		done:         doneChan,
// 	}

// 	if err := dp.Submit(initialTask); err != nil {
// 		t.Fatalf("Failed to submit initial task: %v", err)
// 	}

// 	<-doneChan // Wait for all tasks to complete

// 	expectedOrder := []struct {
// 		ID    int
// 		Type  string
// 		Group string
// 	}{
// 		{3, "start", "groupB"},
// 		{3, "complete", "groupB"},
// 		{2, "start", "groupB"},
// 		{2, "complete", "groupB"},
// 		{1, "start", "groupB"},
// 		{1, "complete", "groupB"},
// 	}

// 	<-time.After(time.Second)
// 	verifyExecutionOrder(t, recorder.GetEvents(), expectedOrder)
// }

// // Additional test to verify error handling
// func TestDependency_ErrorHandling(t *testing.T) {
// 	recorder := NewExecutionRecorder()

// 	// Create pool with no workers
// 	pool := retrypool.New(context.Background(), []retrypool.Worker[TestTask]{})

// 	// Try to create dependency pool with invalid config
// 	dp, err := retrypool.NewDependencyPool[TestTask, string, int](
// 		pool,
// 		func() retrypool.Worker[TestTask] {
// 			return &OutsideWorker{recorder}
// 		},
// 		retrypool.DependencyConfig[TestTask, string, int]{
// 			MaxWorkers:     5,
// 			ExecutionOrder: retrypool.ExecutionOrderForward,
// 			TaskMode:       retrypool.TaskModeBlocking,
// 			MinWorkers:     -1, // Invalid configuration
// 		},
// 	)

// 	if err == nil {
// 		t.Error("Expected error for invalid configuration, got nil")
// 	}

// 	if dp != nil {
// 		t.Error("Expected nil dependency pool for invalid configuration")
// 	}
// }

// // Test concurrent task submission
// func TestDependency_ConcurrentSubmission(t *testing.T) {
// 	recorder := NewExecutionRecorder()
// 	workers := []retrypool.Worker[TestTask]{
// 		&OutsideWorker{recorder},
// 		&OutsideWorker{recorder},
// 	}

// 	pool := retrypool.New(context.Background(), workers)
// 	dp, err := retrypool.NewDependencyPool[TestTask, string, int](
// 		pool,
// 		func() retrypool.Worker[TestTask] {
// 			return &OutsideWorker{recorder}
// 		},
// 		retrypool.DependencyConfig[TestTask, string, int]{
// 			MaxWorkers:     5,
// 			MinWorkers:     1,
// 			ExecutionOrder: retrypool.ExecutionOrderForward,
// 			TaskMode:       retrypool.TaskModeIndependent,
// 		},
// 	)
// 	if err != nil {
// 		t.Fatalf("Failed to create dependency pool: %v", err)
// 	}

// 	var wg sync.WaitGroup
// 	concurrentGroups := 3
// 	tasksPerGroup := 3

// 	for g := 0; g < concurrentGroups; g++ {
// 		wg.Add(1)
// 		go func(groupNum int) {
// 			defer wg.Done()
// 			groupName := fmt.Sprintf("group%d", groupNum)

// 			for i := 1; i <= tasksPerGroup; i++ {
// 				task := TestTask{
// 					ID:           i,
// 					Group:        groupName,
// 					Dependencies: []int{},
// 				}
// 				if i > 1 {
// 					task.Dependencies = []int{i - 1}
// 				}
// 				if err := dp.Submit(task); err != nil {
// 					t.Errorf("Failed to submit task: %v", err)
// 				}
// 			}
// 		}(g)
// 	}

// 	wg.Wait()

// 	// Wait for all tasks to complete
// 	dp.WaitWithCallback(context.Background(), func(queueSize, processingCount, deadTaskCount int) bool {
// 		return queueSize > 0 || processingCount > 0
// 	}, time.Second)

// 	events := recorder.GetEvents()

// 	// Verify basic expectations
// 	totalExpectedEvents := concurrentGroups * tasksPerGroup * 2 // 2 events per task (start/complete)
// 	if len(events) != totalExpectedEvents {
// 		t.Errorf("Expected %d events, got %d", totalExpectedEvents, len(events))
// 	}

// 	// Verify order within each group
// 	for g := 0; g < concurrentGroups; g++ {
// 		groupName := fmt.Sprintf("group%d", g)
// 		groupEvents := filterEventsByGroup(events, groupName)

// 		// Check that within each group, tasks are executed in order
// 		lastCompleted := 0
// 		for _, event := range groupEvents {
// 			if event.Type == "start" {
// 				if event.ID <= lastCompleted {
// 					t.Errorf("Task %d in %s started before previous task completed",
// 						event.ID, groupName)
// 				}
// 			} else if event.Type == "complete" {
// 				lastCompleted = event.ID
// 			}
// 		}
// 	}
// }

// // Helper function to filter events by group
// func filterEventsByGroup(events []ExecutionEvent, group string) []ExecutionEvent {
// 	filtered := make([]ExecutionEvent, 0)
// 	for _, event := range events {
// 		if event.Group == group {
// 			filtered = append(filtered, event)
// 		}
// 	}
// 	return filtered
// }
