# Documentation

## Table of Contents

- [Introduction](#introduction)
- [Getting Started](#getting-started)
- [Pool Configuration](#pool-configuration)
- [Worker Interface](#worker-interface)
- [Dispatching Tasks](#dispatching-tasks)
- [Retry Logic](#retry-logic)
- [Delay Types](#delay-types)
- [Error Handling](#error-handling)
- [Context Management](#context-management)
- [Advanced Usage](#advanced-usage)
  - [Custom Delay Functions](#custom-delay-functions)
  - [Custom Retry Conditions](#custom-retry-conditions)
  - [Task Time Limits](#task-time-limits)
  - [Dead Task Retrieval](#dead-task-retrieval)
- [Examples](#examples)
- [FAQ](#faq)

## Introduction

**retrypool** is designed to handle concurrent task processing with robust retry mechanisms. It abstracts the complexity of managing worker pools, retries, and backoff strategies, allowing developers to build resilient systems efficiently.

## Getting Started

To use **retrypool**, install it via:

```bash
go get github.com/davidroman0O/retrypool
```

Import the package in your Go code:

```go
import "github.com/davidroman0O/retrypool"
```

## Pool Configuration

Create a new pool using `retrypool.NewPool`:

```go
pool := retrypool.NewPool(ctx, workers, options...)
```

- **ctx**: The base context for the pool.
- **workers**: A slice of `Worker` instances.
- **options**: Functional options to configure the pool.

### Available Options

- `WithAttempts[T](attempts int)`: Set the maximum number of retry attempts.
- `WithDelay[T](delay time.Duration)`: Set the base delay between retries.
- `WithMaxDelay[T](maxDelay time.Duration)`: Set the maximum delay between retries.
- `WithMaxJitter[T](maxJitter time.Duration)`: Set the maximum random jitter added to delays.
- `WithDelayType[T](delayType DelayTypeFunc[T])`: Set a custom delay calculation function.
- `WithOnRetry[T](onRetry OnRetryFunc[T])`: Set a callback function invoked on retries.
- `WithRetryIf[T](retryIf RetryIfFunc)`: Set a custom condition for retrying errors.
- `WithContext[T](ctx context.Context)`: Override the pool's context.
- `WithTimer[T](timer Timer)`: Provide a custom timer implementation.
- `WithOnTaskSuccess[T](onTaskSuccess OnTaskSuccessFunc[T])`: Define a callback for successful task completion.
- `WithOnTaskFailure[T](onTaskFailure OnTaskFailureFunc[T])`: Define a callback for task failure.

## Worker Interface

Implement the `Worker` interface for your task type:

```go
type Worker[T any] interface {
    Run(ctx context.Context, data T) error
}
```

- **Run**: Executes the task logic. Should return `nil` on success or an `error` on failure.

## Dispatching Tasks

Add tasks to the pool using `Dispatch`:

```go
err := pool.Dispatch(data, taskOptions...)
```

- **data**: The task data to process.
- **taskOptions**: Optional configurations for the individual task.

### Task Options

- `WithMaxDuration[T](maxDuration time.Duration)`: Set a maximum duration for each attempt of the task.
- `WithTimeLimit[T](limit time.Duration)`: Set a time limit for the task's total processing time.
- `WithImmediateRetry[T]`: Enable immediate retry for a task.
- `WithPanicOnTimeout[T]`: Trigger a panic if the task exceeds its timeout.

## Retry Logic

The pool handles retries based on the configuration provided.

- **Attempts**: The maximum number of times a task will be attempted.
- **Delay**: The delay between retry attempts.
- **Backoff Strategies**: Adjust delays using strategies like fixed, exponential, or custom functions.

### Unlimited Attempts

Use `retrypool.UnlimitedAttempts` to retry indefinitely until success or context cancellation.

```go
retrypool.WithAttempts[int](retrypool.UnlimitedAttempts)
```

## Delay Types

Customize delay calculations between retries using delay functions.

- **FixedDelay**: Constant delay between retries.
- **BackOffDelay**: Exponential backoff delay.
- **RandomDelay**: Random delay up to `MaxJitter`.
- **CombineDelay**: Combine multiple delay functions.

### Example

```go
retrypool.WithDelayType[int](retrypool.CombineDelay(
    retrypool.BackOffDelay[int],
    retrypool.RandomDelay[int],
)),
```

## Error Handling

Distinguish between recoverable and unrecoverable errors.

- **Recoverable Errors**: Errors that can be retried.
- **Unrecoverable Errors**: Errors that should not be retried.

### Marking Unrecoverable Errors

Wrap errors with `retrypool.Unrecoverable` to prevent retries.

```go
return retrypool.Unrecoverable(fmt.Errorf("critical failure"))
```

### Custom Retry Conditions

Use `WithRetryIf` to provide a custom function determining whether to retry.

```go
retrypool.WithRetryIf[int](func(err error) bool {
    return errors.Is(err, ErrTemporary)
})
```

## Context Management

The pool and workers respect context cancellations and timeouts.

- **Pool Context**: Cancelling the pool's context stops all workers and tasks.
- **Task Context**: Each task receives the pool's context, which can be used to handle cancellations.

## Advanced Usage

### Custom Delay Functions

Define your own delay logic by implementing `DelayTypeFunc`.

```go
func customDelay[T any](n int, err error, config *retrypool.Config[T]) time.Duration {
    // Your custom delay calculation
}
```

Set it using:

```go
retrypool.WithDelayType[int](customDelay[int])
```

### Custom Retry Conditions

Implement complex retry conditions based on error types or contents.

```go
retrypool.WithRetryIf[int](func(err error) bool {
    // Analyze the error and decide whether to retry
})
```

### Task Time Limits

Limit the total processing time for individual tasks.

```go
pool.Dispatch(data, retrypool.WithTimeLimit[int](time.Second*5))
```

### Dead Task Retrieval

Access tasks that failed after all retry attempts using `DeadTasks`.

```go
deadTasks := pool.DeadTasks()
for _, task := range deadTasks {
    // Handle dead tasks
}
```

## Examples

### Custom Timer Implementation

You can provide a custom timer for testing or specialized timing needs.

```go
type CustomTimer struct{}

func (t *CustomTimer) After(d time.Duration) <-chan time.Time {
    // Your custom timing logic
}

retrypool.WithTimer[int](&CustomTimer{})
```

### Waiting with Callbacks

Wait for the pool to complete tasks while performing periodic checks or updates.

```go
err := pool.WaitWithCallback(ctx, func(queueSize, processingCount int) bool {
    // Return true to continue waiting, false to stop
    return queueSize > 0 || processingCount > 0
}, time.Second)
```

## FAQ

**Q:** Can I add more workers after the pool has been created?

**A:** Yes, you can dynamically add workers using `AddWorker` and remove them using `RemoveWorker`.

---

**Q:** How does the pool handle panics within worker functions?

**A:** Panics within `Worker.Run` are recovered by the pool, and the task is retried according to the configured retry logic.

---

**Q:** Can I prioritize certain tasks over others?

**A:** You can use the `WithImmediateRetry` task option to prioritize tasks for immediate retry, placing them at the front of the queue.

---

**Q:** How are tasks distributed among workers?

**A:** Tasks are assigned to workers that haven't tried them before. This ensures efficient utilization and avoids repeatedly assigning a failing task to the same worker.

---

**Q:** What happens if all workers are busy?

**A:** Tasks are queued until a worker becomes available. The pool manages the queues internally.

---

**Q:** Is **retrypool** safe for concurrent use?

**A:** Yes, the pool is designed for concurrent environments and handles synchronization internally.

---

**Q:** Can I retrieve the results of tasks?

**A:** The `Worker.Run` method does not return results directly. You can modify your task data to include channels or callbacks for result delivery.

---

Feel free to explore and experiment with **retrypool** to suit your specific use case. The library is designed to be flexible and adaptable to a wide range of scenarios.

