package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/retrypool"
)

// failingWorker is a worker that always fails tasks
type failingWorker struct {
	ID int
}

func (w *failingWorker) Run(_ context.Context, _ int) error {
	return errors.New("intentional failure")
}

// TestDeadTaskLimit verifies that the dead task limit is enforced
func TestDeadTaskLimit(t *testing.T) {
	ctx := context.Background()

	deadTasksLimit := 5
	totalTasks := 10

	pool := retrypool.New[int](
		ctx,
		[]retrypool.Worker[int]{&failingWorker{}},
		retrypool.WithAttempts[int](1),                    // Only try once
		retrypool.WithDeadTasksLimit[int](deadTasksLimit), // Limit dead tasks
	)

	// Submit more tasks than the dead task limit
	for i := 0; i < totalTasks; i++ {
		err := pool.Submit(i)
		if err != nil {
			t.Fatalf("Failed to submit task %d: %v", i, err)
		}
	}

	// Wait for processing to complete
	err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	// Count actual dead tasks through Range
	actualDeadTasks := 0
	pool.RangeDeadTasks(func(dt *retrypool.DeadTask[int]) bool {
		actualDeadTasks++
		return true
	})

	if actualDeadTasks != deadTasksLimit {
		t.Errorf("Expected %d dead tasks in storage, got %d", deadTasksLimit, actualDeadTasks)
	}

	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestDeadTaskStateHistory verifies that task state transitions are properly recorded
// TestDeadTaskStateHistory verifies that task state transitions follow the exact expected pattern
// TestDeadTaskStateHistory verifies that task state transitions follow the exact expected pattern
func TestDeadTaskStateHistory(t *testing.T) {
	ctx := context.Background()

	pool := retrypool.New[int](
		ctx,
		[]retrypool.Worker[int]{&failingWorker{}},
		retrypool.WithAttempts[int](2), // Try twice to generate more state transitions
	)

	// Submit a task that will fail
	err := pool.Submit(1)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for processing to complete and task to be marked as dead
	err = pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0 || deadTaskCount == 0
	}, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	// Wait a bit to ensure dead task is processed
	time.Sleep(200 * time.Millisecond)

	var deadTask *retrypool.DeadTask[int]
	pool.RangeDeadTasks(func(dt *retrypool.DeadTask[int]) bool {
		deadTask = dt
		return false
	})

	if deadTask == nil {
		t.Fatal("Expected to find a dead task")
	}

	// Define exact expected transitions and reasons
	expectedTransitions := []struct {
		from   retrypool.TaskState
		to     retrypool.TaskState
		reason string
	}{
		{retrypool.TaskStateCreated, retrypool.TaskStatePending, "Submitted"},
		{retrypool.TaskStatePending, retrypool.TaskStateQueued, "Task enqueued"},
		{retrypool.TaskStateQueued, retrypool.TaskStateRunning, "Task processing started"},
		{retrypool.TaskStateRunning, retrypool.TaskStateFailed, "Task failed"},
		{retrypool.TaskStateFailed, retrypool.TaskStateQueued, "Task enqueued"},
		{retrypool.TaskStateQueued, retrypool.TaskStateRunning, "Task processing started"},
		{retrypool.TaskStateRunning, retrypool.TaskStateFailed, "Task failed"},
		{retrypool.TaskStateFailed, retrypool.TaskStateDead, "Task added to dead tasks"},
	}

	// Verify transitions match exactly
	if len(deadTask.StateHistory) != len(expectedTransitions) {
		t.Errorf("Expected %d transitions, got %d", len(expectedTransitions), len(deadTask.StateHistory))
		for i, transition := range deadTask.StateHistory {
			t.Logf("Actual transition %d: %s -> %s (%s)",
				i,
				transition.FromState,
				transition.ToState,
				transition.Reason)
		}
		t.FailNow()
	}

	for i, expected := range expectedTransitions {
		actual := deadTask.StateHistory[i]
		t.Logf("Checking transition %d: %s -> %s (%s)",
			i,
			actual.FromState,
			actual.ToState,
			actual.Reason)

		if actual.FromState != expected.from {
			t.Errorf("Transition %d: expected FromState %s, got %s",
				i, expected.from, actual.FromState)
		}
		if actual.ToState != expected.to {
			t.Errorf("Transition %d: expected ToState %s, got %s",
				i, expected.to, actual.ToState)
		}
		if actual.Reason != expected.reason {
			t.Errorf("Transition %d: expected reason %q, got %q",
				i, expected.reason, actual.Reason)
		}
		if actual.Timestamp.IsZero() {
			t.Errorf("Transition %d has zero timestamp", i)
		}
		if i > 0 {
			prevTimestamp := deadTask.StateHistory[i-1].Timestamp
			if !actual.Timestamp.After(prevTimestamp) {
				t.Errorf("Transition %d timestamp not after previous transition", i)
			}
		}
	}

	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestDeadTaskRetrieval tests the methods for accessing dead tasks
func TestDeadTaskRetrieval(t *testing.T) {
	ctx := context.Background()
	totalTasks := 5

	pool := retrypool.New[int](
		ctx,
		[]retrypool.Worker[int]{&failingWorker{}},
		retrypool.WithAttempts[int](1), // Only try once
	)

	// Submit tasks that will fail
	for i := 0; i < totalTasks; i++ {
		err := pool.Submit(i)
		if err != nil {
			t.Fatalf("Failed to submit task %d: %v", i, err)
		}
	}

	// Wait for processing to complete
	err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0 || int(deadTaskCount) < totalTasks
	}, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	// Test RangeDeadTasks
	var rangeCount int
	pool.RangeDeadTasks(func(dt *retrypool.DeadTask[int]) bool {
		rangeCount++
		return true
	})

	if rangeCount != totalTasks {
		t.Errorf("Expected %d dead tasks in range, got %d", totalTasks, rangeCount)
	}

	// Test PullDeadTask
	dt, err := pool.PullDeadTask(0)
	if err != nil {
		t.Errorf("Failed to pull dead task: %v", err)
	}
	if dt == nil {
		t.Error("Expected dead task, got nil")
	}

	// Verify count decreased after pull
	if count := pool.DeadTaskCount(); count != int64(totalTasks-1) {
		t.Errorf("Expected %d dead tasks after pull, got %d", totalTasks-1, count)
	}

	// Test PullRangeDeadTasks
	remainingTasks := int(pool.DeadTaskCount())
	dts, err := pool.PullRangeDeadTasks(0, remainingTasks)
	if err != nil {
		t.Errorf("Failed to pull range of dead tasks: %v", err)
	}
	if len(dts) != remainingTasks {
		t.Errorf("Expected %d dead tasks in pulled range, got %d", remainingTasks, len(dts))
	}

	// Verify all dead tasks were pulled
	if count := pool.DeadTaskCount(); count != 0 {
		t.Errorf("Expected 0 dead tasks after pull range, got %d", count)
	}

	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestDeadTaskNotifications verifies dead task notifications are delivered
func TestDeadTaskNotifications(t *testing.T) {
	ctx := context.Background()
	totalTasks := 3

	var (
		notifications   []int
		notificationsMu sync.Mutex
	)

	pool := retrypool.New[int](
		ctx,
		[]retrypool.Worker[int]{&failingWorker{}},
		retrypool.WithAttempts[int](1), // Only try once
		retrypool.WithOnDeadTask[int](func(deadTaskIndex int) {
			notificationsMu.Lock()
			notifications = append(notifications, deadTaskIndex)
			notificationsMu.Unlock()
		}),
	)

	// Submit tasks that will fail
	for i := 0; i < totalTasks; i++ {
		err := pool.Submit(i)
		if err != nil {
			t.Fatalf("Failed to submit task %d: %v", i, err)
		}
	}

	// Wait for processing to complete
	err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0 || int(deadTaskCount) < totalTasks
	}, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	// Give time for notifications to be processed
	time.Sleep(200 * time.Millisecond)

	notificationsMu.Lock()
	defer notificationsMu.Unlock()

	if len(notifications) != totalTasks {
		t.Errorf("Expected %d notifications, got %d", totalTasks, len(notifications))
	}

	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
}
