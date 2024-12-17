package retrypool

// import (
// 	"context"
// 	"errors"
// 	"fmt"
// 	"time"
// )

// /// T == the type of the data task given to a worker for processing
// /// GID == the type of the group ID
// /// TID == the type of the task ID

// type DependencyPool[T any, GID comparable, TID comparable] struct {
// 	pooler        Pooler[T]
// 	workerFactory WorkerFactory[T]
// 	config        DependencyConfig[T, GID, TID]
// }

// type DependencyConfig[T any, GID comparable, TID comparable] struct{}

// type DependentTask[GID comparable, TID comparable] interface {
// 	GetDependencies() []TID
// 	GetGroupID() GID
// 	GetTaskID() TID
// }

// // `pooler` is the Pool[T] that manage workers for you
// func NewDependencyPool[T any, GID comparable, TID comparable](
// 	pooler Pooler[T],
// 	workerFactory WorkerFactory[T],
// 	config DependencyConfig[T, GID, TID],
// ) (*DependencyPool[T, GID, TID], error) {
// 	dp := &DependencyPool[T, GID, TID]{
// 		pooler:        pooler,
// 		workerFactory: workerFactory,
// 		config:        config,
// 	}

// 	// ... add whatever you want there

// 	return dp, nil
// }

// type dependencyTaskConfig[T any, GID comparable, TID comparable] struct {
// 	waitForCompletion bool // will have to wait for dependent task to complete before processing (non-blocking processing) ELSE will be processed in parallel (blocking processing)
// }

// type dependencyTaskOption[T any, GID comparable, TID comparable] func(*dependencyTaskConfig[T, GID, TID])

// func WithWaitForCompletion[T any, GID comparable, TID comparable]() dependencyTaskOption[T, GID, TID] {
// 	return func(cfg *dependencyTaskConfig[T, GID, TID]) {
// 		cfg.waitForCompletion = true
// 	}
// }

// func (dp *DependencyPool[T, GID, TID]) Submit(data T, opt ...dependencyTaskOption[T, GID, TID]) error {
// 	// ... add whatever you want there
// 	dtask, ok := any(data).(DependentTask[GID, TID])
// 	if !ok {
// 		return errors.New("data does not implement DependentTask interface")
// 	}
// 	cfg := dependencyTaskConfig[T, GID, TID]{}
// 	for _, o := range opt {
// 		o(&cfg)
// 	}
// 	fmt.Println(dtask.GetGroupID())
// 	fmt.Println(dtask.GetTaskID())
// 	fmt.Println(dtask.GetDependencies())
// 	return nil
// }

// func (dp *DependencyPool[T, GID, TID]) WaitWithCallback(ctx context.Context, callback func(queueSize, processingCount, deadTaskCount int) bool, interval time.Duration) error {
// 	//... other things you want to do before waiting
// 	return dp.pooler.WaitWithCallback(ctx, callback, interval)
// }

// func (dp *DependencyPool[T, GID, TID]) Close() error {
// 	//... other things you want to do before closing the pooler
// 	return dp.pooler.Close()
// }
