// // blocking/reversed.go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/davidroman0O/retrypool"
)

type BlockingRevTask struct {
	id        int
	group     string
	deps      []int
	completed chan struct{}
}

func (t BlockingRevTask) GetDependencies() []int { return t.deps }
func (t BlockingRevTask) GetGroupID() string     { return t.group }
func (t BlockingRevTask) GetTaskID() int         { return t.id }

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

type BlockingRevWorker struct {
	id         int
	log        *ExecutionLog
	taskStates *sync.Map
}

func (w *BlockingRevWorker) Run(ctx context.Context, data BlockingRevTask) error {
	w.log.AddStart(data.group, data.id)

	// Wait for dependencies to complete
	for _, depID := range data.GetDependencies() {
		if depChan, ok := w.taskStates.Load(fmt.Sprintf("%s-%d", data.group, depID)); ok {
			<-depChan.(chan struct{})
		}
	}

	// Simulate work
	time.Sleep(500 * time.Millisecond)

	w.log.AddComplete(data.group, data.id)

	// Signal completion
	close(data.completed)
	w.taskStates.Store(fmt.Sprintf("%s-%d", data.group, data.id), data.completed)

	return nil
}

func main() {
	executionLog := NewExecutionLog()
	taskStates := &sync.Map{}

	workers := []retrypool.Worker[BlockingRevTask]{
		&BlockingRevWorker{1, executionLog, taskStates},
		&BlockingRevWorker{2, executionLog, taskStates},
	}

	pool := retrypool.New(context.Background(), workers)

	dp, _ := retrypool.NewDependencyPool[BlockingRevTask, string, int](
		pool,
		func() retrypool.Worker[BlockingRevTask] {
			return &BlockingRevWorker{len(workers) + 1, executionLog, taskStates}
		},
		retrypool.DependencyConfig[BlockingRevTask, string, int]{
			TaskOrderMode:  retrypool.TaskOrderOrdered,
			DependencyMode: retrypool.DependencyReversed,
		},
	)

	// Create tasks with completion channels
	groupB := []BlockingRevTask{
		{3, "groupB", []int{}, make(chan struct{})},
		{2, "groupB", []int{3}, make(chan struct{})},
		{1, "groupB", []int{2}, make(chan struct{})},
	}

	fmt.Println("Submitting Blocking Reversed Tasks for Group B")
	for _, t := range groupB {
		_ = dp.Submit(t)
	}

	fmt.Println("Waiting for Blocking Reversed Tasks to Complete")
	dp.WaitWithCallback(context.Background(), func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, time.Second)

	executionLog.Verify()
	dp.Close()
}
