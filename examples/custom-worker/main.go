package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/davidroman0O/retrypool"
)

// StringTask represents the data we'll process.
type StringTask struct {
	Text string
}

// StringWorker implements the retrypool.Worker interface.
type StringWorker struct {
	ID int // this is automatically set by the pool
}

// Run processes a task by converting the text to uppercase.
func (w *StringWorker) Run(ctx context.Context, task StringTask) error {
	upperText := strings.ToUpper(task.Text)
	fmt.Printf("Worker %d processed text: %s\n", w.ID, upperText)
	return nil
}

func main() {
	ctx := context.Background()

	// Create workers.
	workers := []retrypool.Worker[StringTask]{}
	for i := 0; i < 3; i++ {
		workers = append(workers, &StringWorker{})
	}

	// Create the pool.
	pool := retrypool.New(ctx, workers)
	defer pool.Close()

	// Submit tasks.
	texts := []string{"hello", "world", "this", "is", "a", "test"}
	for _, text := range texts {
		task := StringTask{Text: text}
		err := pool.Submit(task)
		if err != nil {
			fmt.Printf("Error submitting task: %v\n", err)
		}
	}

	pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, time.Second/4)

	pool.Close()
}
