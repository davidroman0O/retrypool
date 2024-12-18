// blocking/ordered.go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/davidroman0O/retrypool"
	"github.com/sasha-s/go-deadlock"
)

type OrdTask struct {
	id    int
	group string
	deps  []int
	dp    *retrypool.DependencyPool[OrdTask, string, int]
	log   *ExecutionLog
	done  chan struct{}
}

func (t OrdTask) GetDependencies() []int { return t.deps }
func (t OrdTask) GetGroupID() string     { return t.group }
func (t OrdTask) GetTaskID() int         { return t.id }

type ExecutionLog struct {
	mu          deadlock.Mutex
	events      []ExecutionEvent
	completeChs map[int]chan struct{} // Track completion channels by task ID
}

type ExecutionEvent struct {
	group     string
	id        int
	eventType string
	timestamp time.Time
}

func NewExecutionLog() *ExecutionLog {
	return &ExecutionLog{
		events:      make([]ExecutionEvent, 0),
		completeChs: make(map[int]chan struct{}),
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

	// Signal completion
	if ch, exists := l.completeChs[id]; exists {
		close(ch)
	}
}

func (l *ExecutionLog) GetCompletionCh(id int) chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ch, exists := l.completeChs[id]; exists {
		return ch
	}
	ch := make(chan struct{})
	l.completeChs[id] = ch
	return ch
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
	ID  int
	log *ExecutionLog
}

func (w *OrdWorker) Run(ctx context.Context, data OrdTask) error {
	w.log.AddStart(data.group, data.id)

	fmt.Println("\t Processing Task", w.ID, data.group, data.id)

	// For tasks 1 and 2, submit next task FIRST before doing anything else
	if data.id < 3 {
		nextID := data.id + 1
		nextDone := make(chan struct{})

		nextTask := OrdTask{
			id:    nextID,
			group: data.group,
			deps:  []int{data.id},
			dp:    data.dp,
			log:   data.log,
			done:  nextDone,
		}

		fmt.Println("\t\t Submitting Next Task", w.ID, data.group, nextID, "with deps", data.id, "...", nextTask.deps)

		// Submit next task immediately
		if err := data.dp.Submit(nextTask); err != nil {
			return err
		}

		<-nextDone
	}

	// Complete our task
	w.log.AddComplete(data.group, data.id)
	if data.done != nil {
		close(data.done)
	}

	return nil
}

func main() {
	executionLog := NewExecutionLog()

	workers := []retrypool.Worker[OrdTask]{
		&OrdWorker{log: executionLog},
		&OrdWorker{log: executionLog},
		&OrdWorker{log: executionLog},
	}

	pool := retrypool.New(context.Background(), workers)

	dp, _ := retrypool.NewDependencyPool[OrdTask, string, int](
		pool,
		func() retrypool.Worker[OrdTask] {
			return &OrdWorker{len(workers) + 1, executionLog}
		},
		retrypool.DependencyConfig[OrdTask, string, int]{
			ExecutionOrder: retrypool.ExecutionOrderReverse,
			TaskMode:       retrypool.TaskModeBlocking,
		},
	)

	// Create channels for synchronization
	doneChan := make(chan struct{})

	task1 := OrdTask{
		id:    1,
		group: "groupA",
		deps:  []int{},
		dp:    dp,
		log:   executionLog,
		done:  doneChan,
	}

	fmt.Println("Submitting Initial Task")
	if err := dp.Submit(task1); err != nil {
		panic(err)
	}

	// Wait for all tasks to complete
	<-doneChan

	fmt.Println("All tasks completed")
	dp.Close()
	executionLog.Verify()
}
