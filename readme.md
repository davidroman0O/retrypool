# retrypool

[![GoDoc](https://godoc.org/github.com/davidroman0O/retrypool?status.svg)](https://godoc.org/github.com/davidroman0O/retrypool)  
[![Go Report Card](https://goreportcard.com/badge/github.com/davidroman0O/retrypool)](https://goreportcard.com/report/github.com/davidroman0O/retrypool)  
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**retrypool** is a Go library designed for scenarios where you have multiple workers that need to process the same type of tasks but each worker requires exclusive access to a resource. The pool handles task distribution, retries on failures, and lets you dynamically manage workers as your resource availability changes.

The key focus is on managing tasks that must be processed sequentially by workers with unique resources - rather than just parallelizing work across identical workers, retrypool helps you orchestrate task processing when your worker count is determined by how many unique resource instances (credentials, connections, etc.) you have available.

In many workloads, workers aren’t identical: they come with their own credentials, rate limits, or access restrictions. A simple example might be several API keys, each with different usage quotas. Rather than treating all workers as the same, retrypool recognizes these differences. It’s built so that each worker can carry its own conditions, while you just submit tasks and let the pool handle the rest.

If one worker frequently hits a rate limit, retrypool can reroute subsequent tasks to other workers that still have capacity. If another worker uses a premium credential, it can receive specialized tasks without manual effort. Over time, you can add, remove, pause, or resume workers as your environment changes, and the pool will smoothly adjust. Instead of juggling these details yourself, you define the basic rules, and retrypool ensures that tasks reach the most suitable worker—even as each worker’s unique constraints and availability evolve.

## Features

- **Generic Task Support**: Type-safe task processing with Go generics
- **Flexible Worker Management**:
  - Dynamic worker addition/removal
  - Worker pause/resume capabilities
  - Synchronous or asynchronous operation modes
  - Built-in worker lifecycle hooks (OnStart, OnStop, OnPause, OnResume, OnRemove)

- **Task Processing**:
  - Configurable retry attempts and delays
  - Multiple retry policies (fixed, exponential backoff)
  - Custom retry conditions
  - Task timeouts (per-attempt and total)
  - Immediate retry and bounce retry options
  - Dead task management with history tracking

- **Additional Features**:
  - Rate limiting
  - Maximum queue size limits
  - Deadtask limits
  - Request-response pattern support
  - Task state transitions tracking
  - Panic recovery and handling

## Installation

```bash
go get github.com/davidroman0O/retrypool
```

## Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "time"
    "github.com/davidroman0O/retrypool"
)

// Define your worker
type MyWorker struct {
    ID int
}

// Implement the Worker interface
func (w *MyWorker) Run(ctx context.Context, data int) error {
    fmt.Printf("Worker %d processing: %d\n", w.ID, data)
    return nil
}

func main() {
    ctx := context.Background()
    
    // Create workers
    workers := []retrypool.Worker[int]{
        &MyWorker{},
        &MyWorker{},
    }
    
    // Initialize pool with options
    pool := retrypool.New(ctx, workers,
        retrypool.WithAttempts[int](3),
        retrypool.WithDelay[int](time.Second),
        retrypool.WithRateLimit[int](10.0), // 10 tasks per second
    )

    // Submit tasks
    for i := 0; i < 5; i++ {
        if err := pool.Submit(i); err != nil {
            fmt.Printf("Failed to submit task: %v\n", err)
        }
    }

    // Wait for completion
    pool.WaitWithCallback(ctx, func(queueSize, processingCount, deadTaskCount int) bool {
        return queueSize > 0 || processingCount > 0
    }, time.Second)

    pool.Close()
}
```

## Advanced Configuration

### Custom Retry Policy

```go
pool := retrypool.New(ctx, workers,
    retrypool.WithRetryPolicy[int](&retrypool.ExponentialBackoffRetryPolicy[int]{
        BaseDelay: time.Second,
        MaxDelay:  time.Minute,
        MaxJitter: time.Second,
    }),
)
```

### Worker Lifecycle Management

```go
// Add a new worker
newWorker := &MyWorker{}
pool.Add(newWorker, nil)

// Pause a worker
pool.Pause(workerID)

// Resume a worker
pool.Resume(workerID)

// Remove a worker
pool.Remove(workerID)
```

### Task Options

```go
// Submit with immediate retry
pool.Submit(data, retrypool.WithImmediateRetry[int]())

// Submit with timeout
pool.Submit(data, retrypool.WithTimeout[int](time.Minute))

// Submit with bounce retry (retry on different workers)
pool.Submit(data, retrypool.WithBounceRetry[int]())
```

### Handling Tasks with Unique Credentials and Bounce Retry

Consider a scenario where you have multiple API tokens, each worker holding its own. Some tasks might fail with one token due to rate limiting, transient errors, or resource constraints, but could succeed under a different token’s conditions. By using bounce retry, **retrypool** automatically routes failed tasks to other workers with different tokens on subsequent attempts. You don’t need to manually shuffle tasks around—the pool takes care of it.

In the example below, each worker is associated with its own token (`tokenA`, `tokenB`, `tokenC`). Tasks may randomly fail in this simulation. When that happens, **retrypool** tries the task again—potentially handing it to a different worker with a different token. Over time, this improves the chance that tasks find a suitable environment to succeed, without any extra logic from you.

```go
type APIWorker struct {
	ID int
}

func (w *APIWorker) OnStart(ctx context.Context) {
	fmt.Printf("Worker with id %d started\n", w.ID)
}

func (w *APIWorker) Run(ctx context.Context, task *retrypool.RequestResponse[string, error]) error {
	fmt.Printf("Task '%s' started with id %d\n", task.Request, w.ID)

	if w.ID == 1 {
		fmt.Printf("\tTask '%s' failed with id %d\n", task.Request, w.ID)
		return fmt.Errorf("failed with id %d", w.ID)
	}

	task.Complete(nil)
	fmt.Printf("Task '%s' succeeded with id %d\n", task.Request, w.ID)
	return nil
}

func main() {
	ctx := context.Background()

	workers := []retrypool.Worker[*retrypool.RequestResponse[string, error]]{
		&APIWorker{},
		&APIWorker{},
	}

	pool := retrypool.New(ctx, workers,
		retrypool.WithAttempts[*retrypool.RequestResponse[string, error]](5),
	)

	for i := 0; i < 20; i++ {
		req := retrypool.NewRequestResponse[string, error](fmt.Sprintf("Task #%d", i))

		if err := pool.Submit(req, retrypool.WithBounceRetry[*retrypool.RequestResponse[string, error]]()); err != nil {
			fmt.Printf("Failed to submit task: %v\n", err)
		}
	}

	pool.WaitWithCallback(ctx, func(q, p, d int) bool {
		return q > 0 && p > 0 && d > 0
	}, 500*time.Millisecond)

	pool.Close()

	pool.RangeWorkerQueues(func(workerID int, queueSize int64) bool {
		fmt.Println("WorkerID:", workerID, "QueueSize:", queueSize)
		return true
	})

	pool.RangeTaskQueues(func(workerID int, queue retrypool.TaskQueue[*retrypool.RequestResponse[string, error]]) bool {
		fmt.Println("WorkerID:", workerID, "Size:", queue.Length())
		return true
	})
}
```


## Examples

The following examples demonstrate various use cases and features:

- Basic Tasks
  - [Basic task submission and processing](./examples/basic/)
  - [Custom worker implementation](./examples/custom-worker/)
  - [Task Notifications](./examples/task-notifications/)

- Retry Mechanisms
  - [Retry mechanism with fixed delay](./examples/retry-fixed-delay/)
  - [Exponential backoff retry policy](./examples/backoff-retrypolicy/)
  - [Custom Retry Policy Function](./examples/custom-retrypolicy-func/)
  - [Custom Error Handling with retryIf Function](./examples/custom-retryif/)
  - [Dynamic Adjustment of Retry Policies](./examples/retry-policies/)

- Worker Management
  - [Worker Lifecycle Management](./examples/worker-lifecycle/)
  - [Rate limiting task submission](./examples/ratelimit-tasks/)
  - [Task Options: Immediate Retry and Bounce Retry](./examples/immediate-bounce/)

- Error Handling
  - [Handling Dead Tasks with Callback](./examples/handling-deadtasks/)
  - [Panic Handling](./examples/panic-handling/)
  - [Error Handling and Specific Error Cases](./examples/error-handling/)

- Advanced Features
  - [Task Timeouts and deadlines](./examples/timeout-deadlines/)
  - [Request Response](./examples/request-response/)
  - [Testing Concurrency and Data Race Prevention](./examples/concurrency/)
  - [Graceful Shutdown and Cleanup](./examples/shutdown/)

- Specialized Pools
  - [Independent dependency pool](./examples/dependency-independent/)
  - [Blocking dependency pool](./examples/dependency-blocking/)

## Documentation

For detailed API documentation and more examples, visit:
- [GoDoc Documentation](https://godoc.org/github.com/davidroman0O/retrypool)
- [Examples Directory](./examples/)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.