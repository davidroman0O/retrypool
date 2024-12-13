package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/davidroman0O/retrypool"
)

type testWorkerInt struct {
	ID int
}

func (w *testWorkerInt) Run(ctx context.Context, data int) error {
	time.Sleep(time.Duration(data) * time.Millisecond)
	fmt.Printf("worker %d processed: %d\n", w.ID, data)
	return nil
}

func TestSubmit(t *testing.T) {
	tests := []struct {
		name     string
		syncMode bool
	}{
		{"Asynchronous Mode", false},
		{"Synchronous Mode", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			var pool *retrypool.Pool[int]
			if tt.syncMode {
				pool = retrypool.New(
					ctx,
					[]retrypool.Worker[int]{&testWorkerInt{}},
					retrypool.WithSynchronousMode[int](),
				)
			} else {
				pool = retrypool.New(
					ctx,
					[]retrypool.Worker[int]{&testWorkerInt{}},
				)
			}

			if err := pool.Submit(1); err != nil {
				t.Fatal(err)
			}

			if err := pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
				return queueSize > 0 && processingCount > 0 && deadTaskCount > 0
			}, time.Second); err != nil {
				t.Fatal(err)
			}

			if err := pool.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
