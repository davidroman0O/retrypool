package tests

import (
	"context"
	"testing"

	"github.com/davidroman0O/retrypool"
)

type mockWorkerReflect struct {
	ID       int
	onStart  bool
	onStop   bool
	onPause  bool
	onResume bool
	onRemove bool
}

func (w *mockWorkerReflect) Run(ctx context.Context, data int) error {
	return nil
}

func (w *mockWorkerReflect) OnStart() {
	w.onStart = true
}

func (w *mockWorkerReflect) OnStop() {
	w.onStop = true
}

func (w *mockWorkerReflect) OnPause() {
	w.onPause = true
}

func (w *mockWorkerReflect) OnResume() {
	w.onResume = true
}

func (w *mockWorkerReflect) OnRemove() {
	w.onRemove = true
}

func TestDynamicMethods(t *testing.T) {

	ctx := context.Background()

	worker := &mockWorkerReflect{ID: -1}

	pool := retrypool.New(ctx, []retrypool.Worker[int]{worker})

	if err := pool.Pause(0); err != nil {
		t.Fatal(err)
	}
	if err := pool.Resume(0); err != nil {
		t.Fatal(err)
	}
	if err := pool.Remove(0); err != nil {
		t.Fatal(err)
	}

	if worker.ID != 0 {
		t.Error("Worker ID was not set")
	}

	if !worker.onStart {
		t.Error("OnStart was not called")
	}

	if !worker.onStop {
		t.Error("OnStop was not called")
	}

	if !worker.onPause {
		t.Error("OnPause was not called")
	}

	if !worker.onResume {
		t.Error("OnResume was not called")
	}

	if !worker.onRemove {
		t.Error("OnRemove was not called")
	}
}
