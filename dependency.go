package retrypool

// import (
// 	"context"
// 	"fmt"
// 	"time"

// 	"github.com/sasha-s/go-deadlock"
// )

// type Dependency[T any] struct {
// 	pool    *Pool[T]
// 	factory WorkerFactory[T]
// 	config  DConfig[T]
// 	group   *groupsTree
// }

// type DConfig[T any] struct {
// 	MaxGroupResources int // Maximum of root group resources (default: 1)
// }

// type ownershipMap[T any] struct {
// 	mu     deadlock.Mutex
// 	owners map[int]groupID
// }

// type groupID interface{}
// type taskID interface{}

// // dependentTask represents a task that depends on other tasks
// type dependentTask interface {
// 	GetDependencies() []taskID
// 	GetGroupID() groupID
// 	GetTaskID() taskID
// 	HashID() uint64
// }

// // TaskInfo holds metadata about a task
// type taskInfo struct {
// 	ID           taskID
// 	State        TaskState
// 	Dependencies []taskID
// 	Completed    bool
// 	Failed       bool
// 	Processing   bool
// 	WorkerID     int // Tracks which worker is processing the task
// 	RetryCount   int
// 	QueuedAt     time.Time
// 	StartedAt    time.Time
// 	WaitingOn    []taskID // List of task IDs this task is waiting on
// }

// // groupInfo holds metadata about a group
// type groupInfo struct {
// 	Tasks          map[taskID]*taskInfo
// 	Started        bool
// 	Priority       int
// 	TaskOrder      []taskID // Maintains order of task submission
// 	ActiveTasks    int32    // Number of currently processing tasks
// 	CompletedTasks int32
// 	FailedTasks    int32
// }

// func NewDependency[T any](
// 	ctx context.Context,
// 	factory WorkerFactory[T],
// ) (*Dependency[T], error) {
// 	d := &Dependency[T]{
// 		pool: New[T](
// 			ctx,
// 			[]Worker[T]{factory()},
// 			WithRoundRobinDistribution[T](),
// 		),
// 		factory: factory,
// 		group:   &groupsTree{groups: make(map[groupID]*groupInfo)},
// 		config: DConfig[T]{ // TODO: make this configurable
// 			MaxGroupResources: 1,
// 		},
// 	}

// 	return d, nil
// }

// func (d *Dependency[T]) Submit(data T) error {
// 	task, ok := any(data).(dependentTask)
// 	if !ok {
// 		d.pool.logger.Error(d.pool.ctx, "Invalid task type", "data_type", fmt.Sprintf("%T", data))
// 		return fmt.Errorf("submitted data must implement DependentTask interface")
// 	}

// 	groupID := groupID(task.GetGroupID())

// 	// Simple check to see if the group exists
// 	if !d.group.Has(groupID) {
// 		info := &groupInfo{
// 			Tasks:     map[taskID]*taskInfo{},
// 			TaskOrder: []taskID{},
// 		}
// 		d.group.Set(groupID, info)
// 	} else {
// 		if _, ok := d.group.Get(groupID).Tasks[task.GetTaskID()]; !ok {
// 			d.group.Get(groupID).Tasks[task.GetTaskID()] = &taskInfo{
// 				ID:           taskID(task.GetTaskID()),
// 				State:        TaskStateQueued,
// 				Dependencies: task.GetDependencies(),
// 				WaitingOn:    []taskID{},
// 			}
// 		} else {
// 			d.pool.logger.Error(d.pool.ctx, "Task already exists", "group_id", groupID, "task_id", task.GetTaskID())
// 			return fmt.Errorf("task already exists")
// 		}
// 	}

// 	return nil
// }

// type groupsTree struct {
// 	mu     deadlock.RWMutex
// 	groups map[groupID]*groupInfo
// }

// func (g *groupsTree) Get(groupID groupID) *groupInfo {
// 	g.mu.RLock()
// 	defer g.mu.RUnlock()
// 	return g.groups[groupID]
// }

// func (g *groupsTree) GetSafe(groupID groupID) (*groupInfo, bool) {
// 	g.mu.RLock()
// 	defer g.mu.RUnlock()
// 	data, ok := g.groups[groupID]
// 	return data, ok
// }

// func (g *groupsTree) Has(groupID groupID) bool {
// 	g.mu.RLock()
// 	defer g.mu.RUnlock()
// 	_, ok := g.groups[groupID]
// 	return ok
// }

// func (g *groupsTree) Delete(groupID groupID) {
// 	g.mu.Lock()
// 	defer g.mu.Unlock()
// 	delete(g.groups, groupID)
// }

// func (g *groupsTree) Set(groupID groupID, group *groupInfo) {
// 	g.mu.Lock()
// 	defer g.mu.Unlock()
// 	g.groups[groupID] = group
// }

// func (g *groupsTree) Keys() []groupID {
// 	g.mu.RLock()
// 	defer g.mu.RUnlock()
// 	keys := make([]groupID, 0, len(g.groups))
// 	for key := range g.groups {
// 		keys = append(keys, key)
// 	}
// 	return keys
// }

// func (g *groupsTree) Values() []*groupInfo {
// 	g.mu.RLock()
// 	defer g.mu.RUnlock()
// 	values := make([]*groupInfo, 0, len(g.groups))
// 	for _, value := range g.groups {
// 		values = append(values, value)
// 	}
// 	return values
// }

// func (g *groupsTree) Range(f func(groupID groupID, group *groupInfo) bool) {
// 	g.mu.RLock()
// 	defer g.mu.RUnlock()
// 	for groupID, group := range g.groups {
// 		if !f(groupID, group) {
// 			break
// 		}
// 	}
// }
