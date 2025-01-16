package retrypool

import (
	"context"
	"time"
)

// Pooler is an interface that exposes all of the public methods of the Pool[T] struct.
type Pooler[T any] interface {
	// SetOnTaskSuccess sets the handler that will be called when a task succeeds.
	SetOnTaskSuccess(handler func(data T, metadata Metadata))

	// SetOnTaskFailure sets the handler that will be called when a task fails.
	// The handler should return a TaskAction indicating how the pool should proceed.
	SetOnTaskFailure(handler func(data T, metadata Metadata, err error) TaskAction)

	SetOnTaskAttempt(handler func(task *Task[T], workerID int))

	// GetMetricsSnapshot returns a snapshot of the current pool metrics.
	// GetMetricsSnapshot() MetricsSnapshot[T]

	// NewWorkerID returns a new unique worker ID.
	// NewWorkerID() int

	// Add adds a new worker to the pool. If a queue is not provided, a new one will be created.
	Add(worker Worker[T], queue TaskQueue[T]) error

	// Remove removes a worker by its ID, redistributing its tasks to other workers.
	Remove(id int) error

	// Pause pauses a worker by its ID, redistributing its tasks and preventing it from processing further tasks.
	Pause(id int) error

	// Resume resumes a previously paused worker, allowing it to process tasks again.
	Resume(id int) error

	// Workers returns the list of worker IDs currently managed by the pool.
	Workers() ([]int, error)

	// GetFreeWorkers returns a list of worker IDs that have no tasks in their queue
	GetFreeWorkers() []int

	// SubmitToFreeWorker attempts to submit a task to a free worker
	SubmitToFreeWorker(taskData T, options ...TaskOption[T]) error

	// Submit allows submitting data directly as a task without pre-allocation. Optional TaskOptions can modify the task's behavior.
	Submit(data T, options ...TaskOption[T]) error

	// WaitWithCallback waits until the provided callback returns false, periodically invoking it at the given interval.
	// The callback receives the current queue size, processing count, and dead task count, and should return false to stop waiting.
	WaitWithCallback(ctx context.Context, callback func(queueSize, processingCount, deadTaskCount int) bool, interval time.Duration) error

	// Close gracefully shuts down the pool, stopping all workers and redistributing or discarding tasks as needed.
	Close() error

	// QueueSize returns the total number of tasks currently queued.
	QueueSize() int64

	// ProcessingCount returns the number of tasks currently being processed by workers.
	ProcessingCount() int64

	// DeadTaskCount returns the number of dead tasks that have permanently failed.
	DeadTaskCount() int64

	// RangeDeadTasks iterates over all dead tasks. If the callback returns false, iteration stops.
	RangeDeadTasks(fn func(*DeadTask[T]) bool)

	// PullDeadTask removes and returns a dead task at the specified index.
	PullDeadTask(idx int) (*DeadTask[T], error)

	// PullRangeDeadTasks removes and returns a range of dead tasks [from, to).
	PullRangeDeadTasks(from int, to int) ([]*DeadTask[T], error)

	// RangeWorkerQueues iterates over each worker's queue size. If the callback returns false, iteration stops.
	RangeWorkerQueues(f func(workerID int, queueSize int64) bool)

	// RangeTaskQueues iterates over each worker's TaskQueue. If the callback returns false, iteration stops.
	RangeTaskQueues(f func(workerID int, queue TaskQueue[T]) bool)

	// RangeWorkers iterates over each worker. If the callback returns false, iteration stops.
	RangeWorkers(f func(workerID int, state WorkerSnapshot[T]) bool)
}
