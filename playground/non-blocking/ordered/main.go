// non-blocking/ordered.go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/davidroman0O/retrypool"
)

type OrdTask struct {
	id    int
	group string
	deps  []int
}

func (t OrdTask) GetDependencies() []int { return t.deps }
func (t OrdTask) GetGroupID() string     { return t.group }
func (t OrdTask) GetTaskID() int         { return t.id }

type ExecutionLog struct {
	mu     sync.Mutex
	events []ExecutionEvent
}

type ExecutionEvent struct {
	group     string
	id        int
	eventType string
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

type OrdWorker struct {
	id  int
	log *ExecutionLog
}

func (w *OrdWorker) Run(ctx context.Context, data OrdTask) error {
	w.log.AddStart(data.group, data.id)

	time.Sleep(500 * time.Millisecond)

	w.log.AddComplete(data.group, data.id)
	return nil
}

func main() {
	executionLog := NewExecutionLog()

	workers := []retrypool.Worker[OrdTask]{
		&OrdWorker{1, executionLog},
		&OrdWorker{2, executionLog},
	}

	pool := retrypool.New(context.Background(), workers)

	dp, _ := retrypool.NewDependencyPool[OrdTask, string, int](
		pool,
		func() retrypool.Worker[OrdTask] {
			return &OrdWorker{len(workers) + 1, executionLog}
		},
		retrypool.DependencyConfig[OrdTask, string, int]{
			ExecutionOrder: retrypool.ExecutionOrderForward,
			TaskMode:       retrypool.TaskModeIndependent,
			MaxWorkers:     10,
		},
	)

	groupA := []OrdTask{
		{2, "groupA", []int{1}},
		{1, "groupA", []int{}},
		{3, "groupA", []int{2}},
	}

	fmt.Println("Submitting Ordered Tasks for Group A")
	for _, t := range groupA {
		_ = dp.Submit(t)
	}

	fmt.Println("Waiting for Ordered Tasks to Complete")
	dp.WaitWithCallback(context.Background(), func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, time.Second)

	executionLog.Verify()
	dp.Close()
}
