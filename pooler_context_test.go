package retrypool

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// ContextValueWorker checks for specific context values
type ContextValueWorker struct {
	key         any
	valueFound  string
	valueMutex  sync.Mutex
	expectedVal string
}

func NewContextValueWorker(key any, expectedVal string) *ContextValueWorker {
	return &ContextValueWorker{
		key:         key,
		expectedVal: expectedVal,
	}
}

func (w *ContextValueWorker) Run(ctx context.Context, data int) error {
	if val, ok := ctx.Value(w.key).(string); ok {
		fmt.Println("val", val)
		w.valueMutex.Lock()
		w.valueFound = val
		w.valueMutex.Unlock()
	}
	return nil
}

func (w *ContextValueWorker) GetValue() string {
	w.valueMutex.Lock()
	defer w.valueMutex.Unlock()
	return w.valueFound
}

// CancellationWorker tests context cancellation
type CancellationWorker struct {
	started        sync.WaitGroup
	cancelReceived bool
	cancelMutex    sync.Mutex
}

func NewCancellationWorker() *CancellationWorker {
	w := &CancellationWorker{}
	w.started.Add(1)
	return w
}

func (w *CancellationWorker) Run(ctx context.Context, data int) error {
	w.started.Done() // Signal that we're running

	select {
	case <-ctx.Done():
		w.cancelMutex.Lock()
		fmt.Println("cancelMutex")
		w.cancelReceived = true
		w.cancelMutex.Unlock()
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return nil
	}
}

func (w *CancellationWorker) WasCancelled() bool {
	w.cancelMutex.Lock()
	defer w.cancelMutex.Unlock()
	return w.cancelReceived
}

// DeadlineWorker tests deadline propagation
type DeadlineWorker struct {
	deadlineReceived time.Time
	deadlineMutex    sync.Mutex
}

func (w *DeadlineWorker) Run(ctx context.Context, data int) error {
	if deadline, ok := ctx.Deadline(); ok {
		w.deadlineMutex.Lock()
		fmt.Println("deadlineMutex", deadline)
		w.deadlineReceived = deadline
		w.deadlineMutex.Unlock()
	}
	return nil
}

func (w *DeadlineWorker) GetDeadline() time.Time {
	w.deadlineMutex.Lock()
	defer w.deadlineMutex.Unlock()
	return w.deadlineReceived
}

// MultiValueWorker tests multiple context values
type MultiValueWorker struct {
	values      []string
	valuesMutex sync.Mutex
}

func (w *MultiValueWorker) Run(ctx context.Context, data int) error {
	w.valuesMutex.Lock()
	defer w.valuesMutex.Unlock()

	w.values = append(w.values,
		ctx.Value("string-key").(string),
		ctx.Value(42).(string))
	fmt.Println("w.values", w.values)
	return nil
}

func (w *MultiValueWorker) GetValues() []string {
	w.valuesMutex.Lock()
	defer w.valuesMutex.Unlock()
	return append([]string{}, w.values...)
}

func TestWorkerContextValues(t *testing.T) {
	const testKey = "test-key"
	const testValue = "test-value"

	parentCtx := context.WithValue(context.Background(), testKey, testValue)
	worker := NewContextValueWorker(testKey, testValue)
	pool := New(parentCtx, []Worker[int]{worker})

	err := pool.Submit(1)
	if err != nil {
		t.Fatalf("Failed to submit task: %v", err)
	}

	err = pool.WaitWithCallback(parentCtx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitWithCallback failed: %v", err)
	}

	pool.Shutdown()

	if worker.GetValue() != testValue {
		t.Errorf("Expected context value %q, got %q", testValue, worker.GetValue())
	}
}

func TestWorkerContextFeatures(t *testing.T) {
	t.Run("Cancellation propagation", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		worker := NewCancellationWorker()
		pool := New(parentCtx, []Worker[int]{worker})

		err := pool.Submit(1)
		if err != nil {
			t.Fatalf("Failed to submit task: %v", err)
		}

		// Wait for worker to start
		worker.started.Wait()
		time.Sleep(50 * time.Millisecond) // Small buffer to ensure worker is in select

		cancel()

		time.Sleep(100 * time.Millisecond) // Give time for cancellation to propagate

		if !worker.WasCancelled() {
			t.Error("Worker did not receive context cancellation")
		}

		pool.Shutdown()
	})

	t.Run("Deadline propagation", func(t *testing.T) {
		deadline := time.Now().Add(100 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()

		worker := &DeadlineWorker{}
		pool := New(ctx, []Worker[int]{worker})

		err := pool.Submit(1)
		if err != nil {
			t.Fatalf("Failed to submit task: %v", err)
		}

		pool.WaitWithCallback(ctx, func(q, p, d int) bool {
			return q > 0 || p > 0
		}, 10*time.Millisecond)

		if !worker.GetDeadline().Equal(deadline) {
			t.Errorf("Expected deadline %v, got %v", deadline, worker.GetDeadline())
		}

		pool.Shutdown()
	})

	t.Run("Multiple context values", func(t *testing.T) {
		ctx := context.Background()
		ctx = context.WithValue(ctx, "string-key", "value1")
		ctx = context.WithValue(ctx, 42, "value2")

		worker := &MultiValueWorker{}
		pool := New(ctx, []Worker[int]{worker})

		err := pool.Submit(1)
		if err != nil {
			t.Fatalf("Failed to submit task: %v", err)
		}

		pool.WaitWithCallback(ctx, func(q, p, d int) bool {
			return q > 0 || p > 0
		}, 10*time.Millisecond)

		values := worker.GetValues()
		if len(values) != 2 || values[0] != "value1" || values[1] != "value2" {
			t.Errorf("Expected values [value1 value2], got %v", values)
		}

		pool.Shutdown()
	})
}

type contextKey string

const testWorkerValueKey = contextKey("worker-value")

type contextTestWorker struct {
	id     string
	mu     sync.Mutex
	values []string
}

func (w *contextTestWorker) Run(ctx context.Context, data int) error {
	time.Sleep(50 * time.Millisecond)

	w.mu.Lock()
	defer w.mu.Unlock()

	// Debug: print context chain
	fmt.Printf("Worker %s executing with context...\n", w.id)
	fmt.Printf("Context value for worker %s: %v\n", w.id, ctx.Value(testWorkerValueKey))

	if value, ok := ctx.Value(testWorkerValueKey).(string); ok {
		fmt.Printf("Worker %s got value: %s\n", w.id, value)
		w.values = append(w.values, value)
	} else {
		fmt.Printf("Worker %s got no value\n", w.id)
		w.values = append(w.values, "no-value")
	}
	return nil
}

func TestWorkerSpecificContext(t *testing.T) {
	ctx := context.Background()

	worker1 := &contextTestWorker{id: "1"}
	worker2 := &contextTestWorker{id: "2"}
	worker3 := &contextTestWorker{id: "3"}

	pool := New[int](ctx, nil)

	// Add workers and verify their contexts immediately
	w1Ctx := context.WithValue(ctx, testWorkerValueKey, "worker1")
	pool.AddWorker(worker1, WithWorkerContext[int](func(ctx context.Context) context.Context {
		return w1Ctx
	}))

	// Verify worker1's context is stored correctly
	if v := pool.workers[0].ctx.Value(testWorkerValueKey); v != "worker1" {
		t.Errorf("Worker1 context not set correctly, got %v", v)
	}

	w2Ctx := context.WithValue(ctx, testWorkerValueKey, "worker2")
	pool.AddWorker(worker2, WithWorkerContext[int](func(ctx context.Context) context.Context {
		return w2Ctx
	}))

	// Verify worker2's context is stored correctly
	if v := pool.workers[1].ctx.Value(testWorkerValueKey); v != "worker2" {
		t.Errorf("Worker2 context not set correctly, got %v", v)
	}

	pool.AddWorker(worker3)

	// Let's also verify contexts right before submitting tasks
	t.Log("Verifying contexts before task submission:")
	for id, ws := range pool.workers {
		t.Logf("Worker %d context value: %v", id, ws.ctx.Value(testWorkerValueKey))
	}

	// Submit tasks
	for i := 0; i < 6; i++ {
		err := pool.Submit(i)
		if err != nil {
			t.Fatalf("Failed to submit task %d: %v", i, err)
		}
	}

	// Wait for tasks to complete
	err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, 100*time.Millisecond)

	if err != nil {
		t.Fatalf("Error waiting for tasks: %v", err)
	}

	pool.Shutdown()

	// Collect all unique values seen by all workers
	allValues := make(map[string]bool)

	worker1.mu.Lock()
	for _, v := range worker1.values {
		allValues[v] = true
	}
	worker1.mu.Unlock()

	worker2.mu.Lock()
	for _, v := range worker2.values {
		allValues[v] = true
	}
	worker2.mu.Unlock()

	worker3.mu.Lock()
	for _, v := range worker3.values {
		allValues[v] = true
	}
	worker3.mu.Unlock()

	// Convert to slice for sorting
	var results []string
	for v := range allValues {
		results = append(results, v)
	}
	sort.Strings(results)

	// Verify we saw all expected values
	expected := []string{"no-value", "worker1", "worker2"}
	if len(results) != 3 {
		t.Errorf("Expected 3 different context values, got %d: %v", len(results), results)
	}

	for i, exp := range expected {
		if i >= len(results) || results[i] != exp {
			t.Errorf("Expected value %s at position %d, got %v", exp, i, results)
		}
	}

	// Additional verification
	t.Logf("Worker1 values: %v", worker1.values)
	t.Logf("Worker2 values: %v", worker2.values)
	t.Logf("Worker3 values: %v", worker3.values)
}
