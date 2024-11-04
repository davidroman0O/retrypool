This previous README offers a thorough foundation for the `retrypool` documentation. Here’s how I'll integrate the key elements from the old README into a more detailed, real-world usage guide, with a structured layout for ease of understanding and quick reference:

---

# retrypool

[![GoDoc](https://godoc.org/github.com/davidroman0O/retrypool?status.svg)](https://godoc.org/github.com/davidroman0O/retrypool)  
[![Go Report Card](https://goreportcard.com/badge/github.com/davidroman0O/retrypool)](https://goreportcard.com/report/github.com/davidroman0O/retrypool)  
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**retrypool** is a versatile Go library that provides a high-level API for building resilient task processing systems with retry management, worker pooling, and custom retry strategies. 

## Features

- **Dynamic Worker Management**: Easily add or remove workers.
- **Advanced Retry Mechanisms**: Customize attempts, delays, backoff strategies, and jitter.
- **Flexible Task Distribution**: Adaptive to different worker capabilities.
- **Task Lifecycle Control**: Time limits, callbacks, and failure tracking.
- **Comprehensive Error Handling**: Differentiate between recoverable/unrecoverable errors.
- **Customizable Task Options**: Apply specific configurations to individual tasks.
- **Real-World Scenarios**: Suitable for API request pools, database operations, and long-running tasks.

## Table of Contents

- [Installation](#installation)
- [Getting Started](#getting-started)
- [Task Lifecycle and Retries](#task-lifecycle-and-retries)
- [Real-World Examples](#real-world-examples)
  - [Basic Example](#basic-example)
  - [HTTP Request Pool](#http-request-pool)
  - [Custom Retry Conditions](#custom-retry-conditions)
  - [Error Handling and Dead Task Tracking](#error-handling-and-dead-task-tracking)
- [API Reference](#api-reference)
- [Motivation and Design](#motivation-and-design)

## Installation

Install via `go get`:

```bash
go get github.com/davidroman0O/retrypool
```

## Getting Started

To start using `retrypool`, implement the `Worker` interface for your task and create a new pool:

```go
type Worker[T any] interface {
    Run(ctx context.Context, data T) error
}
```

### Basic Example

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    "github.com/davidroman0O/retrypool"
)

type SimpleWorker struct{}

func (w *SimpleWorker) Run(ctx context.Context, data int) error {
    time.Sleep(time.Duration(data) * time.Millisecond) // Simulate work
    fmt.Printf("Processed: %d\n", data)
    return nil
}

func main() {
    ctx := context.Background()
    workers := []retrypool.Worker[int]{&SimpleWorker{}, &SimpleWorker{}}
    pool := retrypool.New(ctx, workers)

    for i := 1; i <= 10; i++ {
        pool.Submit(i * 100)
    }

    pool.Wait() // Blocks until all tasks complete
    fmt.Println("All tasks completed")
}
```

## Task Lifecycle and Retries

### Overview of the Task Processing Algorithm

1. **Task Submission and Dispatch**: Tasks are submitted to the pool, checked against pool status, and configured with specific options.
2. **Worker Assignment and Task Execution**: Workers are assigned based on availability; tasks can enforce time limits or panic on timeout if configured.
3. **Retry and Error Handling**: Failed tasks are retried or added to dead tasks based on customizable conditions and backoff strategies.
4. **Dynamic Worker Management**: Workers can be added or removed during runtime.
5. **Custom Retry Strategies**: Configure fixed, exponential, random, or combined backoff delays with jitter support.

## Real-World Examples

### HTTP Request Pool

This example uses `retrypool` to manage API requests with multiple worker tokens and retries for error handling. We use `RequestResponse` to handle responses and errors.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
	"github.com/davidroman0O/retrypool"
)

type APIWorker struct {
	ID          int
	BearerToken string
}

type Data struct {
	URL     string
	Payload interface{}
}

func (w *APIWorker) Run(ctx context.Context, data *retrypool.RequestResponse[Data, error]) error {
	client := &http.Client{}
	payload, err := json.Marshal(data.Request.Payload)
	if err != nil {
		data.CompleteWithError(fmt.Errorf("error marshaling payload: %w", err))
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", data.Request.URL, bytes.NewBuffer(payload))
	req.Header.Set("Authorization", w.BearerToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		err := fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
		data.CompleteWithError(err)
		return err
	}

	data.Complete(nil)
	return nil
}

func main() {
	ctx := context.Background()
	workers := []retrypool.Worker[*retrypool.RequestResponse[Data, error]]{
		&APIWorker{ID: 1, BearerToken: "token1"},
		&APIWorker{ID: 2, BearerToken: "token2"},
	}

	pool := retrypool.New(ctx, workers,
		retrypool.WithAttempts[*retrypool.RequestResponse[Data, error]](3),
		retrypool.WithDelay[*retrypool.RequestResponse[Data, error]](time.Second),
		retrypool.WithOnTaskFailure[*retrypool.RequestResponse[Data, error]](func(pool retrypool.WorkerController[*retrypool.RequestResponse[Data, error]], workerID int, worker retrypool.Worker[*retrypool.RequestResponse[Data, error]], task *retrypool.TaskWrapper[*retrypool.RequestResponse[Data, error]], err error) retrypool.DeadTaskAction {
			if err.Error() == "server error" {
				return retrypool.DeadTaskActionForceRetry
			}
			return retrypool.DeadTaskActionAddToDeadTasks
		}),
	)

	for i := 0; i < 10; i++ {
		task := retrypool.NewRequestResponse[Data, error](Data{URL: "http://example.com", Payload: map[string]interface{}{"key": i}})
		pool.Submit(task)
	}
	pool.Wait()
}
```

### Custom Retry Conditions

Configure retries for specific error types or server statuses:

```go
pool := retrypool.New(ctx, workers,
	retrypool.WithAttempts ,
	retrypool.WithRetryIf[Data](func(err error) bool {
		return strings.Contains(err.Error(), "rate limited") || strings.Contains(err.Error(), "server error")
	}),
	retrypool.WithDelayType[Data](retrypool.BackOffDelay[Data]),
)
```

### Error Handling and Dead Task Tracking

Retrieve dead tasks for logging or inspection:

```go
deadTasks := pool.DeadTasks()
for _, task := range deadTasks {
	log.Printf("Dead task data: %+v", task.Data().Request)
}
```

## API Reference

For further configuration details and task-specific options, refer to the full [API Documentation](https://godoc.org/github.com/davidroman0O/retrypool).
