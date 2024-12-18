// blocking/outside/reversed.go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/davidroman0O/retrypool"
)

// Task definition with reversed mode
type RevTask struct {
	id    int
	group string
	deps  []int
}

// Implement DependentTask interface
func (t RevTask) GetDependencies() []int { return t.deps }
func (t RevTask) GetGroupID() string     { return t.group }
func (t RevTask) GetTaskID() int         { return t.id }

// ExecutionLog tracks task execution for verification
type ExecutionLog struct {
	mu     sync.Mutex
	events []ExecutionEvent
}

type ExecutionEvent struct {
	group     string
	id        int
	eventType string // "start" or "complete"
	timestamp time.Time
}

func NewExecutionLog() *ExecutionLog {
	return &ExecutionLog{
		events: make([]ExecutionEvent, 0),
	}
}

func (l *ExecutionLog) AddStart(group string, id int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, ExecutionEvent{
		group:     group,
		id:        id,
		eventType: "start",
		timestamp: time.Now(),
	})
	fmt.Printf("Task %s-%d started\n", group, id)
}

func (l *ExecutionLog) AddComplete(group string, id int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, ExecutionEvent{
		group:     group,
		id:        id,
		eventType: "complete",
		timestamp: time.Now(),
	})
	fmt.Printf("Task %s-%d completed\n", group, id)
}

func (l *ExecutionLog) Verify() {
	l.mu.Lock()
	defer l.mu.Unlock()

	fmt.Println("\nExecution Order:")
	for i, event := range l.events {
		fmt.Printf("%d. %s-%d (%s)\n", i+1, event.group, event.id, event.eventType)
	}
}

// Worker implementation
type RevWorker struct {
	id  int
	log *ExecutionLog
}

func (w *RevWorker) Run(ctx context.Context, data RevTask) error {
	w.log.AddStart(data.group, data.id)

	time.Sleep(500 * time.Millisecond) // Simulate work

	w.log.AddComplete(data.group, data.id)
	return nil
}

func main() {
	executionLog := NewExecutionLog()

	// Create workers
	workers := []retrypool.Worker[RevTask]{
		&RevWorker{1, executionLog},
		&RevWorker{2, executionLog},
	}

	// Initialize the pool with workers
	pool := retrypool.New(context.Background(), workers)

	// Create dependency pool
	dp, _ := retrypool.NewDependencyPool[RevTask, string, int](
		pool,
		func() retrypool.Worker[RevTask] {
			return &RevWorker{len(workers) + 1, executionLog}
		},
		retrypool.DependencyConfig[RevTask, string, int]{
			ExecutionOrder: retrypool.ExecutionOrderForward,
			TaskMode:       retrypool.TaskModeBlocking,
		},
	)

	// Define tasks with dependencies in reverse order
	groupB := []RevTask{
		{3, "groupB", []int{}},
		{2, "groupB", []int{3}},
		{1, "groupB", []int{2}},
	}

	// Submit all tasks
	fmt.Println("Submitting Reversed Tasks for Group B")
	for _, t := range groupB {
		_ = dp.Submit(t)
	}

	// Wait for completion
	fmt.Println("Waiting for Reversed Tasks to Complete")
	dp.WaitWithCallback(context.Background(), func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, time.Second)

	// Verify execution
	dp.Close()
	<-time.After(1 * time.Second)
	executionLog.Verify()
}
