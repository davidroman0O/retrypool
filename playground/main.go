package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/davidroman0O/retrypool"
)

type task struct {
	id    int
	group string
	deps  []int
}

func (t task) GetDependencies() []int {
	return t.deps
}

func (t task) GetGroupID() string {
	return t.group
}

func (t task) GetTaskID() int {
	return t.id
}

// ExecutionEvent tracks both start and completion of tasks
type ExecutionEvent struct {
	group     string
	id        int
	eventType string // "start" or "complete"
	timestamp time.Time
}
type ExecutionLog struct {
	mu           sync.Mutex
	events       []ExecutionEvent
	activeGroups map[string]int // tracks number of active tasks per group
	order        []string       // stores "group-id" strings in execution order
}

func NewExecutionLog() *ExecutionLog {
	return &ExecutionLog{
		events:       make([]ExecutionEvent, 0),
		activeGroups: make(map[string]int),
		order:        make([]string, 0),
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
	l.activeGroups[group]++
	fmt.Printf("Task %s-%d started (active in group: %d)\n", group, id, l.activeGroups[group])
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
	l.activeGroups[group]--
	l.order = append(l.order, fmt.Sprintf("%s-%d", group, id))
	fmt.Printf("Task %s-%d completed (active in group: %d)\n", group, id, l.activeGroups[group])
}

func (l *ExecutionLog) VerifyExecution() {
	l.mu.Lock()
	defer l.mu.Unlock()

	// First, print execution order
	fmt.Println("\nExecution Order:")
	for i, entry := range l.order {
		fmt.Printf("%d. %s\n", i+1, entry)
	}

	fmt.Println("\nVerifying execution order:")

	// Track execution state for order verification
	aState := make(map[string]bool)
	bState := make(map[string]bool)

	// Verify execution order
	for _, entry := range l.order {
		switch entry {
		case "a-1":
			if len(aState) != 0 {
				fmt.Printf("✗ A1 should be first in group A: %s\n", entry)
			} else {
				fmt.Printf("✓ A1 correct\n")
			}
			aState["a-1"] = true
		case "a-2":
			if !aState["a-1"] {
				fmt.Printf("✗ A2 executed before A1: %s\n", entry)
			} else {
				fmt.Printf("✓ A2 correct\n")
			}
			aState["a-2"] = true
		case "a-3":
			if !aState["a-2"] {
				fmt.Printf("✗ A3 executed before A2: %s\n", entry)
			} else {
				fmt.Printf("✓ A3 correct\n")
			}
			aState["a-3"] = true
		case "a-4":
			if !aState["a-3"] {
				fmt.Printf("✗ A4 executed before A3: %s\n", entry)
			} else {
				fmt.Printf("✓ A4 correct\n")
			}
			aState["a-4"] = true
		case "b-1":
			fmt.Printf("✓ B1 correct\n")
			bState["b-1"] = true
		case "b-2", "b-3":
			if !bState["b-1"] {
				fmt.Printf("✗ %s executed before B1\n", entry)
			} else {
				fmt.Printf("✓ %s correct (after B1)\n", entry)
			}
			bState[entry] = true
		case "b-4":
			if !bState["b-3"] {
				fmt.Printf("✗ B4 executed before B3: %s\n", entry)
			} else {
				fmt.Printf("✓ B4 correct\n")
			}
			bState["b-4"] = true
		}
	}

	fmt.Println("\nVerifying concurrent execution:")

	// Track completed tasks and active tasks per group for concurrency verification
	completed := make(map[string]map[int]bool)
	active := make(map[string]map[int]bool)
	completed["a"] = make(map[int]bool)
	completed["b"] = make(map[int]bool)
	active["a"] = make(map[int]bool)
	active["b"] = make(map[int]bool)

	// For Group A (waitForCompletion), track if we ever had multiple concurrent tasks
	groupAConcurrentViolations := 0

	for _, event := range l.events {
		key := fmt.Sprintf("%s-%d", event.group, event.id)

		if event.eventType == "start" {
			if event.group == "a" {
				// Check waitForCompletion for Group A
				if len(active[event.group]) > 0 {
					fmt.Printf("✗ Task %s started while other Group A tasks were active\n", key)
					groupAConcurrentViolations++
				}
			}
			active[event.group][event.id] = true
		} else {
			completed[event.group][event.id] = true
			delete(active[event.group], event.id)
		}
	}

	fmt.Println("\nFinal verification:")

	// Verify Group A (strict sequence)
	if aState["a-1"] && aState["a-2"] && aState["a-3"] && aState["a-4"] {
		fmt.Println("✓ Group A completed in correct sequence")
	} else {
		fmt.Println("✗ Group A incomplete or incorrect sequence")
	}

	// Verify Group B (parallel allowed)
	if bState["b-1"] && bState["b-2"] && bState["b-3"] && bState["b-4"] {
		fmt.Println("✓ Group B completed with valid dependency order")
	} else {
		fmt.Println("✗ Group B incomplete or invalid dependency order")
	}

	// Verify concurrent execution
	if groupAConcurrentViolations == 0 {
		fmt.Println("✓ Group A maintained sequential execution (waitForCompletion respected)")
	} else {
		fmt.Printf("✗ Group A had %d concurrent execution violations\n", groupAConcurrentViolations)
	}
}

type worker struct {
	log *ExecutionLog
}

func (w *worker) Run(ctx context.Context, data task) error {
	w.log.AddStart(data.group, data.id)
	fmt.Printf("Running task ID %d in group %s (deps: %v)\n",
		data.id, data.group, data.deps)

	time.Sleep(100 * time.Millisecond) // Simulate work

	w.log.AddComplete(data.group, data.id)
	return nil
}

func main() {
	executionLog := NewExecutionLog()
	w := &worker{log: executionLog}

	pool := retrypool.New(context.Background(),
		[]retrypool.Worker[task]{w},
		retrypool.WithRoundRobinDistribution[task]())

	dp, _ := retrypool.NewDependencyPool[task, string, int](
		pool,
		func() retrypool.Worker[task] {
			return &worker{log: executionLog}
		},
		retrypool.DependencyConfig[task, string, int]{},
	)

	// Submit tasks in mixed order
	tasks := []struct {
		task     task
		withWait bool
	}{
		{task{id: 4, group: "a", deps: []int{3}}, true}, // Sequential
		{task{id: 2, group: "a", deps: []int{1}}, true},
		{task{id: 3, group: "a", deps: []int{2}}, true},
		{task{id: 1, group: "a", deps: []int{}}, true},

		{task{id: 3, group: "b", deps: []int{1}}, false}, // Parallel allowed
		{task{id: 4, group: "b", deps: []int{3}}, false},
		{task{id: 2, group: "b", deps: []int{1}}, false},
		{task{id: 1, group: "b", deps: []int{}}, false},
	}

	for _, t := range tasks {
		var err error
		if t.withWait {
			err = dp.Submit(t.task, retrypool.WithWaitForCompletion[task, string, int]())
		} else {
			err = dp.Submit(t.task)
		}
		if err != nil {
			fmt.Printf("Failed to submit task %d in group %s: %v\n", t.task.id, t.task.group, err)
		}
	}

	dp.WaitWithCallback(context.Background(), func(queueSize, processingCount, deadTaskCount int) bool {
		return queueSize > 0 || processingCount > 0
	}, time.Second)

	executionLog.VerifyExecution()
	dp.Close()
}
