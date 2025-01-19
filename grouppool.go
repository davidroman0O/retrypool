package retrypool

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/sasha-s/go-deadlock"
)

// GroupTask is the interface for tasks that have a group ID.
type GroupTask[GID comparable] interface {
	GetGroupID() GID
}

// GroupPoolConfig holds configuration options for the GroupPool.
type GroupPoolConfig[T any, GID comparable] struct {
	Logger            Logger
	WorkerFactory     WorkerFactory[T]
	MaxActivePools    int // -1 means unlimited
	MaxWorkersPerPool int // -1 means unlimited
	UseFreeWorkerOnly bool

	// Called when a task succeeds. We automatically pass the group ID and the pool ID.
	OnTaskSuccess func(gid GID, poolID uint, data T, metadata map[string]any)
	// Called when a task fails. We automatically pass the group ID and the pool ID.
	OnTaskFailure func(gid GID, poolID uint, data T, metadata map[string]any, err error) TaskAction
	// Called when a task is about to run. We pass the group ID, the pool ID, the *Task, and the worker ID.
	OnTaskAttempt func(gid GID, poolID uint, task *Task[T], workerID int)
	// Called whenever we get a snapshot from an underlying Pool. We pass that snapshot.
	OnSnapshot func(snapshot GroupMetricsSnapshot[T, GID])

	metadata *Metadata

	// onSnapshot func()
}

// GroupPoolOption is a functional option for configuring a GroupPool.
type GroupPoolOption[T any, GID comparable] func(cfg *GroupPoolConfig[T, GID])

// func WithGroupSnapshotHandler[T any, GID comparable](cb func()) GroupPoolOption[T, GID] {
// 	return func(c *GroupPoolConfig[T, GID]) {
// 		c.onSnapshot = cb
// 	}
// }

func WithGroupPoolLogger[T any, GID comparable](logger Logger) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.Logger = logger
	}
}

func WithGroupPoolWorkerFactory[T any, GID comparable](wf WorkerFactory[T]) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.WorkerFactory = wf
	}
}

func WithGroupPoolMaxActivePools[T any, GID comparable](max int) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.MaxActivePools = max
	}
}

func WithGroupPoolMaxWorkersPerPool[T any, GID comparable](max int) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.MaxWorkersPerPool = max
	}
}

func WithGroupPoolUseFreeWorkerOnly[T any, GID comparable]() GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.UseFreeWorkerOnly = true
	}
}

func WithGroupPoolOnTaskSuccess[T any, GID comparable](cb func(gid GID, poolID uint, data T, metadata map[string]any)) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.OnTaskSuccess = cb
	}
}

func WithGroupPoolOnTaskFailure[T any, GID comparable](cb func(gid GID, poolID uint, data T, metadata map[string]any, err error) TaskAction) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.OnTaskFailure = cb
	}
}

func WithGroupPoolOnTaskAttempt[T any, GID comparable](cb func(gid GID, poolID uint, task *Task[T], workerID int)) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.OnTaskAttempt = cb
	}
}

func WithGroupPoolOnSnapshot[T any, GID comparable](cb func(snapshot GroupMetricsSnapshot[T, GID])) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.OnSnapshot = cb
	}
}

func WithGroupPoolMetadata[T any, GID comparable](m *Metadata) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.metadata = m
	}
}

// poolItem tracks one active pool, identified by an internal incremental ID.
// assignedGroup indicates which group is currently using it, or a zero value if free.
type poolItem[T any, GID comparable] struct {
	mu            deadlock.RWMutex
	PoolID        uint
	Pool          Pooler[T]
	assignedGroup GID
	busy          bool
}

// isZeroValueOfGroup uses reflection to detect if GID is a zero value
func isZeroValueOfGroup[GID comparable](gid GID) bool {
	// Get zero value for the type
	var zero GID
	return reflect.DeepEqual(gid, zero)
}

// validateGroupID verifies that the GID is not a zero value
func (gp *GroupPool[T, GID]) validateGroupID(task T) error {
	gid := task.GetGroupID()
	if isZeroValueOfGroup(gid) {
		return fmt.Errorf("invalid group ID: zero value detected for type %T", gid)
	}
	return nil
}

// GroupPool manages multiple pools, each identified by an internal incremental ID. When a group
// needs processing, if a free pool is available it is reserved for that group. If no pool is
// free and the maximum active pools limit has been reached, tasks for that group are queued in
// pendingTasks until a pool becomes available.
//
// By default, once a pool is assigned to a group, it stays assigned until the user calls
// CloseGroup(...) or FreePool(...). If you want the group to automatically free up the pool
// when all tasks complete, you can implement that logic in your OnTaskSuccess or OnTaskFailure
// callbacks (e.g., by checking whether the pool is fully idle and then freeing it). After freeing
// a pool, you can try to assign it to any pending tasks again.
type GroupPool[T GroupTask[GID], GID comparable] struct {
	mu           deadlock.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	config       GroupPoolConfig[T, GID]
	nextID       uint
	pools        map[uint]*poolItem[T, GID] // all known pools
	pendingTasks map[GID][]T                // tasks for groups without an assigned pool

	snapshotMu     deadlock.RWMutex // Protects access to the snapshot
	snapshotGroups map[GID]MetricsSnapshot[T]
	snapshot       GroupMetricsSnapshot[T, GID]
}

// NewGroupPool constructs a new GroupPool with the supplied options.
func NewGroupPool[T GroupTask[GID], GID comparable](
	ctx context.Context,
	opts ...GroupPoolOption[T, GID],
) (*GroupPool[T, GID], error) {
	cfg := GroupPoolConfig[T, GID]{
		Logger:            NewLogger(slog.LevelDebug),
		MaxActivePools:    -1,
		MaxWorkersPerPool: -1,
		UseFreeWorkerOnly: false,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.WorkerFactory == nil {
		return nil, fmt.Errorf("worker factory is required")
	}

	var gp *GroupPool[T, GID]

	// Wrap OnTaskSuccess to detect pool idle state
	originalSuccess := cfg.OnTaskSuccess
	cfg.OnTaskSuccess = func(gid GID, poolID uint, data T, meta map[string]any) {
		// Forward to user callback if set
		if originalSuccess != nil {
			originalSuccess(gid, poolID, data, meta)
		}
		go func() {
			// Check if the pool is idle now that a task completed
			gp.handleTaskCompletion(poolID)
		}()
	}

	// Wrap OnTaskFailure to detect pool idle state
	originalFailure := cfg.OnTaskFailure
	cfg.OnTaskFailure = func(gid GID, poolID uint, data T, meta map[string]any, err error) TaskAction {
		var action TaskAction
		if originalFailure != nil {
			action = originalFailure(gid, poolID, data, meta, err)
		} else {
			action = TaskActionRemove
		}
		go func() {
			gp.handleTaskCompletion(poolID)
		}()
		return action
	}

	c, cancel := context.WithCancel(ctx)
	gp = &GroupPool[T, GID]{
		ctx:            c,
		cancel:         cancel,
		config:         cfg,
		nextID:         0,
		pools:          make(map[uint]*poolItem[T, GID]),
		pendingTasks:   make(map[GID][]T),
		snapshotGroups: make(map[GID]MetricsSnapshot[T]),
	}
	return gp, nil
}

// PoolMetricsSnapshot represents metrics for a single pool
type PoolMetricsSnapshot[T any] struct {
	PoolID        uint
	AssignedGroup any // The GID will be type-asserted when used
	IsActive      bool
	QueueSize     int64
	Workers       map[int]WorkerSnapshot[T]
}

// PendingTasksSnapshot represents metrics about pending tasks for each group
type PendingTasksSnapshot[GID comparable] struct {
	GroupID      GID
	TasksPending int
}

// GroupMetricSnapshot now includes pool and task information
type GroupMetricSnapshot[T any, GID comparable] struct {
	GroupID         GID
	PoolID          uint // The pool ID assigned to this group, if any
	HasPool         bool // Whether this group has an assigned pool
	IsPending       bool // Whether this group has pending tasks
	TasksPending    int  // Number of pending tasks for this group
	MetricsSnapshot MetricsSnapshot[T]
}

// GroupMetricsSnapshot now includes pool and pending tasks information
type GroupMetricsSnapshot[T any, GID comparable] struct {
	// Existing fields
	TotalTasksSubmitted int64
	TotalTasksProcessed int64
	TotalTasksSucceeded int64
	TotalTasksFailed    int64
	TotalDeadTasks      int64

	// New fields
	TotalPools        int
	ActivePools       int
	TotalPendingTasks int
	GroupsWithPending int

	// Detailed metrics
	pools        []PoolMetricsSnapshot[T]
	PendingTasks []PendingTasksSnapshot[GID]
	Metrics      []GroupMetricSnapshot[T, GID]
}

func (m GroupMetricsSnapshot[T, GID]) Clone() GroupMetricsSnapshot[T, GID] {
	clone := GroupMetricsSnapshot[T, GID]{
		TotalTasksSubmitted: m.TotalTasksSubmitted,
		TotalTasksProcessed: m.TotalTasksProcessed,
		TotalTasksSucceeded: m.TotalTasksSucceeded,
		TotalTasksFailed:    m.TotalTasksFailed,
		TotalDeadTasks:      m.TotalDeadTasks,
		TotalPools:          m.TotalPools,
		ActivePools:         m.ActivePools,
		TotalPendingTasks:   m.TotalPendingTasks,
		GroupsWithPending:   m.GroupsWithPending,
		pools:               make([]PoolMetricsSnapshot[T], len(m.pools)),
		PendingTasks:        make([]PendingTasksSnapshot[GID], len(m.PendingTasks)),
		Metrics:             make([]GroupMetricSnapshot[T, GID], len(m.Metrics)),
	}

	// Deep copy pools
	for i, pool := range m.pools {
		poolClone := pool
		poolClone.Workers = make(map[int]WorkerSnapshot[T], len(pool.Workers))
		for k, v := range pool.Workers {
			workerClone := v
			if v.Metadata != nil {
				workerClone.Metadata = make(map[string]any, len(v.Metadata))
				for mk, mv := range v.Metadata {
					workerClone.Metadata[mk] = mv
				}
			}
			poolClone.Workers[k] = workerClone
		}
		clone.pools[i] = poolClone
	}

	// Copy pending tasks
	copy(clone.PendingTasks, m.PendingTasks)

	// Copy metrics
	copy(clone.Metrics, m.Metrics)

	return clone
}

func (p *GroupPool[T, GID]) GetSnapshot() GroupMetricsSnapshot[T, GID] {
	p.snapshotMu.RLock()
	defer p.snapshotMu.RUnlock()
	return p.snapshot
}

func (p *GroupPool[T, GID]) calculateMetricsSnapshot() {

	snapshot := GroupMetricsSnapshot[T, GID]{}

	// Collect pool metrics
	p.mu.RLock()
	snapshot.TotalPools = len(p.pools)
	snapshot.pools = make([]PoolMetricsSnapshot[T], 0, len(p.pools))

	// Track active pools and collect pool metrics
	for id, pool := range p.pools {
		pool.mu.RLock()
		isActive := pool.busy
		assignedGroup := pool.assignedGroup
		pool.mu.RUnlock()

		if isActive {
			snapshot.ActivePools++
		}

		// Collect worker metrics for this pool
		workers := make(map[int]WorkerSnapshot[T])
		if pooler := pool.Pool; pooler != nil {
			pooler.RangeWorkers(func(workerID int, state WorkerSnapshot[T]) bool {
				workers[workerID] = state
				return true
			})
		}

		// Get queue size for this pool
		var queueSize int64
		if pooler := pool.Pool; pooler != nil {
			queueSize = pooler.QueueSize()
		}

		snapshot.pools = append(snapshot.pools, PoolMetricsSnapshot[T]{
			PoolID:        id,
			AssignedGroup: assignedGroup,
			IsActive:      isActive,
			QueueSize:     queueSize,
			Workers:       workers,
		})
	}

	// Collect pending tasks metrics
	snapshot.PendingTasks = make([]PendingTasksSnapshot[GID], 0, len(p.pendingTasks))
	snapshot.TotalPendingTasks = 0
	snapshot.GroupsWithPending = len(p.pendingTasks)

	for gid, tasks := range p.pendingTasks {
		pendingCount := len(tasks)
		snapshot.TotalPendingTasks += pendingCount
		snapshot.PendingTasks = append(snapshot.PendingTasks, PendingTasksSnapshot[GID]{
			GroupID:      gid,
			TasksPending: pendingCount,
		})
	}
	p.mu.RUnlock()

	// Aggregate group metrics
	for id, metric := range p.snapshotGroups {
		snapshot.TotalTasksSubmitted += metric.TasksSubmitted
		snapshot.TotalTasksProcessed += metric.TasksProcessed
		snapshot.TotalTasksSucceeded += metric.TasksSucceeded
		snapshot.TotalTasksFailed += metric.TasksFailed
		snapshot.TotalDeadTasks += metric.DeadTasks

		// Find pool assigned to this group
		var poolID uint
		var hasPool bool
		var isPending bool
		var tasksPending int

		// Look for assigned pool
		for _, pool := range snapshot.pools {
			if pool.IsActive && pool.AssignedGroup == id {
				poolID = pool.PoolID
				hasPool = true
				break
			}
		}

		// Check if group has pending tasks
		for _, pending := range snapshot.PendingTasks {
			if pending.GroupID == id {
				isPending = true
				tasksPending = pending.TasksPending
				break
			}
		}

		snapshot.Metrics = append(snapshot.Metrics, GroupMetricSnapshot[T, GID]{
			GroupID:         id,
			PoolID:          poolID,
			HasPool:         hasPool,
			IsPending:       isPending,
			TasksPending:    tasksPending,
			MetricsSnapshot: metric,
		})
	}

	p.snapshot = snapshot
}

// buildPool creates a new poolItem with the next available internal ID, wiring up user callbacks.
func (gp *GroupPool[T, GID]) buildPool() (*poolItem[T, GID], error) {
	w := gp.config.WorkerFactory()
	if w == nil {
		return nil, fmt.Errorf("failed to create worker")
	}
	id := gp.nextID
	gp.nextID++

	metadata := NewMetadata()
	metadata.Set("pool_id", id)

	opts := []Option[T]{
		WithLogger[T](gp.config.Logger),
		WithMetadata[T](metadata),
		WithSnapshots[T](),                              // TODO: we should have options for that
		WithSnapshotInterval[T](time.Millisecond * 100), // TODO: we should have options for that
		WithSnapshotCallback[T](
			func(ms MetricsSnapshot[T]) {

				gp.snapshotMu.Lock()

				if pool, ok := gp.pools[id]; ok {
					pool.mu.Lock()
					gp.snapshotGroups[pool.assignedGroup] = ms
					pool.mu.Unlock()
					gp.calculateMetricsSnapshot()
					if gp.config.OnSnapshot != nil {
						gp.config.OnSnapshot(gp.snapshot)
					}
				}

				gp.snapshotMu.Unlock()

			}),
	}

	// We already wrapped OnTaskSuccess/OnTaskFailure in the constructor,
	// so we just pass them along here.
	if gp.config.OnTaskSuccess != nil {
		userFn := gp.config.OnTaskSuccess
		opts = append(opts, WithOnTaskSuccess[T](func(data T, meta map[string]any) {
			gid := data.GetGroupID()
			userFn(gid, id, data, meta)
		}))
	}

	if gp.config.OnTaskFailure != nil {
		userFn := gp.config.OnTaskFailure
		opts = append(opts, WithOnTaskFailure[T](func(data T, meta map[string]any, err error) TaskAction {
			gid := data.GetGroupID()
			return userFn(gid, id, data, meta, err)
		}))
	}

	if gp.config.OnTaskAttempt != nil {
		userFn := gp.config.OnTaskAttempt
		opts = append(opts, WithOnTaskAttempt[T](func(tsk *Task[T], wid int) {
			userFn(tsk.data.GetGroupID(), id, tsk, wid)
		}))
	}

	p := New[T](gp.ctx, []Worker[T]{w}, opts...)

	return &poolItem[T, GID]{
		PoolID:        id,
		Pool:          p,
		assignedGroup: *new(GID), // zero value
		busy:          false,
	}, nil
}

// assignPoolIfPossible tries to find a free (unassigned) pool or create a new one to handle this group.
// Returns the pool item or nil if we couldn't assign one (due to max limit).
func (gp *GroupPool[T, GID]) assignPoolIfPossible(gid GID) (*poolItem[T, GID], error) {
	// Search for a free pool
	var freePool *poolItem[T, GID]
	for _, it := range gp.pools {
		it.mu.RLock()
		busy := it.busy
		it.mu.RUnlock()
		if !busy {
			freePool = it
			break
		}
	}
	if freePool == nil {
		// No free pool - see if we can create one
		activeCount := 0
		for _, it := range gp.pools {
			it.mu.RLock()
			if it.busy {
				activeCount++
			}
			it.mu.RUnlock()
		}
		if gp.config.MaxActivePools >= 0 && activeCount >= gp.config.MaxActivePools {
			// reached pool limit
			return nil, nil
		}
		newp, err := gp.buildPool()
		if err != nil {
			return nil, err
		}
		gp.pools[newp.PoolID] = newp
		freePool = newp
	}

	// Mark pool as assigned
	freePool.mu.Lock()
	defer freePool.mu.Unlock()
	freePool.assignedGroup = gid
	freePool.busy = true
	return freePool, nil
}

// releasePool frees a pool item so it can be reassigned to another group. We do not close the pool.
func (gp *GroupPool[T, GID]) releasePool(poolID uint) error {
	it, ok := gp.pools[poolID]
	if !ok {
		return fmt.Errorf("pool %d not found", poolID)
	}
	it.mu.Lock()
	it.assignedGroup = *new(GID)
	it.busy = false
	it.mu.Unlock()
	return nil
}

// handleTaskCompletion is invoked (via goroutine) whenever a task completes or fails, to see if the pool is idle.
// If idle, we free up the pool and try to assign it to any pending group. This helps process more than 2 groups.
func (gp *GroupPool[T, GID]) handleTaskCompletion(poolID uint) {
	// Acquire group pool lock first
	gp.mu.Lock()
	defer gp.mu.Unlock()

	it, exists := gp.pools[poolID]
	if !exists {
		return
	}

	// Then acquire pool item lock
	it.mu.Lock()
	pool := it.Pool
	it.mu.Unlock()

	// Check if pool is idle
	q := pool.QueueSize()
	p := pool.ProcessingCount()
	if q == 0 && p == 0 {
		// Release pool before acquiring new locks
		errRelease := gp.releasePool(poolID)
		if errRelease != nil {
			gp.config.Logger.Warn(gp.ctx, "Error releasing pool", "pool_id", poolID, "error", errRelease)
		}
		// Try to consume pending groups
		gp.consumePendingGroupsLocked()
	}
}

// consumePendingGroupsLocked tries to assign free pools to any pending groups in pendingTasks.
func (gp *GroupPool[T, GID]) consumePendingGroupsLocked() {
	for groupID, tasks := range gp.pendingTasks {
		// Attempt to assign a free pool
		poolItem, err := gp.assignPoolIfPossible(groupID)
		if err != nil {
			// If there's an error building a pool, we just skip
			continue
		}
		if poolItem == nil {
			// No free pool available (limit reached), skip
			continue
		}
		// We got a pool, let's submit all tasks
		delete(gp.pendingTasks, groupID)
		for _, t := range tasks {
			subErr := gp.submitToPool(poolItem, t)
			if subErr != nil {
				gp.config.Logger.Warn(gp.ctx, "Error while submitting pending tasks to pool", "error", subErr)
			}
		}
	}
}

// scaleWorkersIfNeeded tries to ensure there's a free worker if UseFreeWorkerOnly is set. Respects MaxWorkersPerPool.
func (gp *GroupPool[T, GID]) scaleWorkersIfNeeded(it *poolItem[T, GID]) error {
	free := it.Pool.GetFreeWorkers()
	if len(free) > 0 {
		return nil
	}
	if gp.config.MaxWorkersPerPool == 0 {
		return ErrNoWorkersAvailable
	}
	if gp.config.MaxWorkersPerPool < 0 {
		w := gp.config.WorkerFactory()
		if w == nil {
			return fmt.Errorf("failed to create worker for scaling")
		}
		return it.Pool.Add(w, nil)
	}
	ws, err := it.Pool.Workers()
	if err != nil {
		return err
	}
	if len(ws) < gp.config.MaxWorkersPerPool {
		w := gp.config.WorkerFactory()
		if w == nil {
			return fmt.Errorf("failed to create worker for scaling")
		}
		return it.Pool.Add(w, nil)
	}
	return ErrNoWorkersAvailable
}

type groupSubmitConfig struct {
	queueNotification     *QueuedNotification
	processedNotification *ProcessedNotification
	metadata              map[string]any
}

type GroupTaskOption[T any, GID comparable] func(*groupSubmitConfig)

func WithTaskGroupQueueNotification[T any, GID comparable](notification *QueuedNotification) GroupTaskOption[T, GID] {
	return func(c *groupSubmitConfig) {
		c.queueNotification = notification
	}
}

func WithTaskGroupProcessedNotification[T any, GID comparable](notification *ProcessedNotification) GroupTaskOption[T, GID] {
	return func(c *groupSubmitConfig) {
		c.processedNotification = notification
	}
}

func WithTaskGroupMetadata[T any, GID comparable](metadata map[string]any) GroupTaskOption[T, GID] {
	return func(c *groupSubmitConfig) {
		c.metadata = metadata
	}
}

// Submit enqueues a task for its group. If the group already has a pool, we submit. Otherwise,
// we try to assign one. If we can't, tasks go to pending for that group. We'll attempt to
// consume pending whenever a pool is freed.
func (gp *GroupPool[T, GID]) Submit(task T, options ...GroupTaskOption[T, GID]) error {
	if err := gp.validateGroupID(task); err != nil {
		return err
	}

	gid := task.GetGroupID()

	gp.mu.Lock()
	defer gp.mu.Unlock()

	// Check if we already have a pool assigned to this group
	var assigned *poolItem[T, GID]
	for _, it := range gp.pools {
		it.mu.RLock()
		g := it.assignedGroup
		it.mu.RUnlock()
		if g == gid {
			assigned = it
			break
		}
	}

	if assigned == nil {
		p, err := gp.assignPoolIfPossible(gid)
		if p == nil && err == nil {
			// Means we couldn't assign a pool, so we queue the task
			gp.pendingTasks[gid] = append(gp.pendingTasks[gid], task)
			return nil
		} else if err != nil {
			return err
		}
		assigned = p
		// drain any previously pending tasks
		pend := gp.pendingTasks[gid]
		delete(gp.pendingTasks, gid)
		for _, queuedTask := range pend {
			if subErr := gp.submitToPool(assigned, queuedTask, options...); subErr != nil {
				return subErr
			}
		}
	}
	return gp.submitToPool(assigned, task, options...)
}

// submitToPool tries to submit one task to the assigned pool. If we only want free workers, we attempt scaling.
// If ErrNoWorkersAvailable is encountered, we add the task to pendingTasks for later processing.
func (gp *GroupPool[T, GID]) submitToPool(it *poolItem[T, GID], t T, opts ...GroupTaskOption[T, GID]) error {
	options := []TaskOption[T]{}
	cfg := &groupSubmitConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.queueNotification != nil {
		options = append(options, WithTaskQueuedNotification[T](cfg.queueNotification))
	}
	if cfg.processedNotification != nil {
		options = append(options, WithTaskProcessedNotification[T](cfg.processedNotification))
	}
	if cfg.metadata != nil {
		m := NewMetadata()
		m.store = cfg.metadata
		options = append(options, WithTaskMetadata[T](m))
	}

	if gp.config.UseFreeWorkerOnly {
		if err := gp.scaleWorkersIfNeeded(it); err != nil && err != ErrNoWorkersAvailable {
			return err
		}
		subErr := it.Pool.SubmitToFreeWorker(t, options...)
		if subErr == ErrNoWorkersAvailable {
			if scErr := gp.scaleWorkersIfNeeded(it); scErr != nil && scErr != ErrNoWorkersAvailable {
				return scErr
			}
			subErr = it.Pool.SubmitToFreeWorker(t, options...)
			if subErr == ErrNoWorkersAvailable {
				gp.pendingTasks[t.GetGroupID()] = append(gp.pendingTasks[t.GetGroupID()], t)
				return nil
			}
		}
		return subErr
	} else {
		subErr := it.Pool.Submit(t, options...)
		if subErr == ErrNoWorkersAvailable {
			gp.pendingTasks[t.GetGroupID()] = append(gp.pendingTasks[t.GetGroupID()], t)
			return nil
		}
		return subErr
	}
}

// CloseGroup forcibly closes the pool that is assigned to the given group, removing that pool
// from our map entirely. If you only want to free (unassign) the group from the pool, call FreePool.
func (gp *GroupPool[T, GID]) CloseGroup(gid GID) error {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	var poolID uint
	var found bool
	for id, it := range gp.pools {
		it.mu.RLock()
		if it.assignedGroup == gid {
			poolID = id
			found = true
		}
		it.mu.RUnlock()
		if found {
			break
		}
	}
	if !found {
		return fmt.Errorf("group %v not found in any active pool", gid)
	}

	it := gp.pools[poolID]
	it.mu.Lock()
	pool := it.Pool
	it.mu.Unlock()

	if err := pool.Close(); err != nil {
		return err
	}
	delete(gp.pools, poolID)
	return nil
}

// FreePool forcibly releases a pool's group assignment but does not close it, making the pool free again.
// Then tries to consume pending groups. This allows reusing the same pool for a new group.
func (gp *GroupPool[T, GID]) FreePool(poolID uint) error {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	if err := gp.releasePool(poolID); err != nil {
		return err
	}
	// Try to assign a freed pool to pending tasks
	gp.consumePendingGroupsLocked()
	return nil
}

// WaitWithCallback waits until callback returns false, checking tasks across all active pools.
func (gp *GroupPool[T, GID]) WaitWithCallback(
	ctx context.Context,
	callback func(q, p, d int) bool,
	interval time.Duration,
) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			q := gp.QueueSize()
			pr := gp.ProcessingCount()
			de := gp.DeadTaskCount()
			if !callback(int(q), int(pr), int(de)) {
				return nil
			}
		}
	}
}

// Close closes all pools and cancels the GroupPool context. All tasks in progress or pending
// will be discarded, so typically you should call WaitWithCallback or ensure your tasks finish first.
func (gp *GroupPool[T, GID]) Close() error {
	gp.mu.Lock()
	defer gp.mu.Unlock()
	gp.cancel()

	var firstErr error
	for id, it := range gp.pools {
		it.mu.Lock()
		pool := it.Pool
		it.mu.Unlock()

		if err := pool.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(gp.pools, id)
	}
	gp.pendingTasks = make(map[GID][]T)
	return firstErr
}

// QueueSize returns the total queue size across all pools plus all pending tasks that haven't been assigned a pool yet.
func (gp *GroupPool[T, GID]) QueueSize() int64 {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

	var total int64
	for _, it := range gp.pools {
		it.mu.RLock()
		pool := it.Pool
		it.mu.RUnlock()
		total += pool.QueueSize()
	}
	for _, tasks := range gp.pendingTasks {
		total += int64(len(tasks))
	}
	return total
}

// ProcessingCount returns the total processing count across all pools.
func (gp *GroupPool[T, GID]) ProcessingCount() int64 {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

	var total int64
	for _, it := range gp.pools {
		it.mu.RLock()
		pool := it.Pool
		it.mu.RUnlock()
		total += pool.ProcessingCount()
	}
	return total
}

// DeadTaskCount returns the total number of dead tasks across all pools.
func (gp *GroupPool[T, GID]) DeadTaskCount() int64 {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

	var total int64
	for _, it := range gp.pools {
		it.mu.RLock()
		pool := it.Pool
		it.mu.RUnlock()
		total += pool.DeadTaskCount()
	}
	return total
}

// RangeDeadTasks iterates over all dead tasks in all pools.
func (gp *GroupPool[T, GID]) RangeDeadTasks(fn func(*DeadTask[T]) bool) {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

outer:
	for _, it := range gp.pools {
		it.mu.RLock()
		pool := it.Pool
		it.mu.RUnlock()

		cont := true
		pool.RangeDeadTasks(func(dt *DeadTask[T]) bool {
			if !fn(dt) {
				cont = false
				return false
			}
			return true
		})
		if !cont {
			break outer
		}
	}
}

// PullDeadTask removes and returns a dead task at the specified index, scanning each pool in turn.
func (gp *GroupPool[T, GID]) PullDeadTask(poolID uint, idx int) (*DeadTask[T], error) {
	gp.mu.Lock()
	defer gp.mu.Unlock()
	if pool, ok := gp.pools[poolID]; !ok {
		return nil, fmt.Errorf("pool %d not found", poolID)
	} else {
		pool.mu.RLock()
		defer pool.mu.RUnlock()
		return pool.Pool.PullDeadTask(idx)
	}
}

// PullRangeDeadTasks removes and returns a range of dead tasks [from, to) across all pools.
func (gp *GroupPool[T, GID]) PullRangeDeadTasks(from, to int) ([]*DeadTask[T], error) {
	if from >= to {
		return nil, fmt.Errorf("invalid range")
	}
	gp.mu.Lock()
	defer gp.mu.Unlock()

	var result []*DeadTask[T]
	offset := 0
	for _, it := range gp.pools {
		it.mu.RLock()
		pool := it.Pool
		count := int(pool.DeadTaskCount())
		it.mu.RUnlock()

		end := offset + count
		if end <= from {
			offset = end
			continue
		}
		if offset >= to {
			break
		}
		if from <= offset && end <= to {
			dts, err := pool.PullRangeDeadTasks(0, count)
			if err != nil {
				return nil, err
			}
			result = append(result, dts...)
			offset = end
			continue
		}
		localFrom := 0
		localTo := count
		if from > offset {
			localFrom = from - offset
		}
		if to < end {
			localTo = to - offset
		}
		dts, err := pool.PullRangeDeadTasks(localFrom, localTo)
		if err != nil {
			return nil, err
		}
		result = append(result, dts...)
		offset = end
		if offset >= to {
			break
		}
	}
	return result, nil
}

// RangeWorkerQueues iterates over each worker queue size in all pools.
func (gp *GroupPool[T, GID]) RangeWorkerQueues(f func(workerID int, queueSize int64) bool) {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

outer:
	for _, it := range gp.pools {
		it.mu.RLock()
		pool := it.Pool
		it.mu.RUnlock()

		cont := true
		pool.RangeWorkerQueues(func(wid int, qs int64) bool {
			if !f(wid, qs) {
				cont = false
				return false
			}
			return true
		})
		if !cont {
			break outer
		}
	}
}

// RangeTaskQueues iterates over each worker's queue in all pools.
func (gp *GroupPool[T, GID]) RangeTaskQueues(f func(workerID int, tq TaskQueue[T]) bool) {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

outer:
	for _, it := range gp.pools {
		it.mu.RLock()
		pool := it.Pool
		it.mu.RUnlock()

		cont := true
		pool.RangeTaskQueues(func(wid int, queue TaskQueue[T]) bool {
			if !f(wid, queue) {
				cont = false
				return false
			}
			return true
		})
		if !cont {
			break outer
		}
	}
}

// RangeWorkers iterates over each worker in all pools.
func (gp *GroupPool[T, GID]) RangeWorkers(f func(workerID int, state WorkerSnapshot[T]) bool) {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

outer:
	for _, it := range gp.pools {
		it.mu.RLock()
		pool := it.Pool
		it.mu.RUnlock()

		cont := true
		pool.RangeWorkers(func(wid int, ws WorkerSnapshot[T]) bool {
			if !f(wid, ws) {
				cont = false
				return false
			}
			return true
		})
		if !cont {
			break outer
		}
	}
}

// Scale the amount of parallel pools that can be active at the same time
func (p *GroupPool[T, GID]) SetConcurrentPools(max int) {
	if max < 1 {
		max = 1
	}
	p.mu.Lock()
	p.config.MaxActivePools = max
	p.mu.Unlock()
}

func (p *GroupPool[T, GID]) SetConcurrentWorkers(max int) {
	if max < 1 {
		max = 1
	}
	p.mu.Lock()
	p.config.MaxWorkersPerPool = max
	p.mu.Unlock()
}

// AddWorkerToPool forcibly adds a worker to a specific internal pool ID. Respects MaxWorkersPerPool.
func (gp *GroupPool[T, GID]) AddWorkerToPool(poolID uint, queue TaskQueue[T]) error {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	it, ok := gp.pools[poolID]
	if !ok {
		return fmt.Errorf("pool %d not found", poolID)
	}
	if gp.config.MaxWorkersPerPool == 0 {
		return fmt.Errorf("cannot add worker, MaxWorkersPerPool is 0")
	}
	ws, err := it.Pool.Workers()
	if err != nil {
		return err
	}
	if gp.config.MaxWorkersPerPool > 0 && len(ws) >= gp.config.MaxWorkersPerPool {
		return fmt.Errorf("cannot add worker, pool %d reached max workers limit", poolID)
	}
	w := gp.config.WorkerFactory()
	if w == nil {
		return fmt.Errorf("failed to create worker for pool %d", poolID)
	}
	return it.Pool.Add(w, queue)
}

// RemoveWorkerFromPool removes a worker by ID from the specified pool ID.
func (gp *GroupPool[T, GID]) RemoveWorkerFromPool(poolID uint, workerID int) error {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

	it, ok := gp.pools[poolID]
	if !ok {
		return fmt.Errorf("pool %d not found", poolID)
	}
	return it.Pool.Remove(workerID)
}

// PauseWorkerInPool pauses a worker by ID in the specified pool ID.
func (gp *GroupPool[T, GID]) PauseWorkerInPool(poolID uint, workerID int) error {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

	it, ok := gp.pools[poolID]
	if !ok {
		return fmt.Errorf("pool %d not found", poolID)
	}
	return it.Pool.Pause(workerID)
}

// ResumeWorkerInPool resumes a worker by ID in the specified pool ID.
func (gp *GroupPool[T, GID]) ResumeWorkerInPool(poolID uint, workerID int) error {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

	it, ok := gp.pools[poolID]
	if !ok {
		return fmt.Errorf("pool %d not found", poolID)
	}
	return it.Pool.Resume(workerID)
}

// WorkersInPool returns the worker IDs for the given pool ID.
func (gp *GroupPool[T, GID]) WorkersInPool(poolID uint) ([]int, error) {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

	it, ok := gp.pools[poolID]
	if !ok {
		return nil, fmt.Errorf("pool %d not found", poolID)
	}
	return it.Pool.Workers()
}

// GetFreeWorkersInPool returns the free worker IDs in the given pool ID.
func (gp *GroupPool[T, GID]) GetFreeWorkersInPool(poolID uint) []int {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

	it, ok := gp.pools[poolID]
	if !ok {
		return nil
	}
	return it.Pool.GetFreeWorkers()
}

// GroupRequestResponse manages the lifecycle of a task request and its response
type GroupRequestResponse[T any, R any, GID comparable] struct {
	groupID     GID              // The group ID
	request     T                // The request data
	done        chan struct{}    // Channel to signal completion
	response    R                // Stores the successful response
	err         error            // Stores any error that occurred
	mu          deadlock.RWMutex // Protects response and err
	isCompleted bool             // Indicates if request is completed
}

// NewGroupRequestResponse creates a new GroupRequestResponse instance
func NewGroupRequestResponse[T any, R any, GID comparable](request T, gid GID) *GroupRequestResponse[T, R, GID] {
	return &GroupRequestResponse[T, R, GID]{
		request: request,
		done:    make(chan struct{}),
		groupID: gid,
	}
}

func (rr GroupRequestResponse[T, R, GID]) GetGroupID() GID {
	return rr.groupID
}

// Safely consults the request data
func (rr *GroupRequestResponse[T, R, GID]) ConsultRequest(fn func(T) error) error {
	rr.mu.Lock()
	err := fn(rr.request)
	rr.mu.Unlock()
	return err
}

// Complete safely marks the request as complete with a response
func (rr *GroupRequestResponse[T, R, GID]) Complete(response R) {
	var completed bool
	rr.mu.RLock()
	completed = rr.isCompleted
	rr.mu.RUnlock()

	if !completed {
		rr.mu.Lock()
		rr.response = response
		rr.isCompleted = true
		close(rr.done)
		rr.mu.Unlock()
	}
}

// CompleteWithError safely marks the request as complete with an error
func (rr *GroupRequestResponse[T, R, GID]) CompleteWithError(err error) {
	var completed bool
	rr.mu.RLock()
	completed = rr.isCompleted
	rr.mu.RUnlock()

	if !completed {
		rr.mu.Lock()
		rr.err = err
		rr.isCompleted = true
		close(rr.done)
		rr.mu.Unlock()
	}
}

// Done returns a channel that's closed when the request is complete
func (rr *GroupRequestResponse[T, R, GID]) Done() <-chan struct{} {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	return rr.done
}

// Err returns any error that occurred during the request
func (rr *GroupRequestResponse[T, R, GID]) Err() error {
	var e error
	rr.mu.RLock()
	e = rr.err
	rr.mu.RUnlock()
	return e
}

// Wait waits for the request to complete and returns the response and any error
func (rr *GroupRequestResponse[T, R, GID]) Wait(ctx context.Context) (R, error) {
	select {
	case <-rr.done:
		var err error
		var response R
		rr.mu.RLock()
		response = rr.response
		err = rr.err
		rr.mu.RUnlock()
		return response, err
	case <-ctx.Done():
		var completed bool
		rr.mu.RLock()
		completed = rr.isCompleted
		rr.mu.RUnlock()
		if !completed {
			var zero R
			rr.mu.Lock()
			rr.err = ctx.Err()
			rr.isCompleted = true
			close(rr.done)
			rr.mu.Unlock()
			return zero, ctx.Err()
		} else {
			var err error
			var response R
			rr.mu.RLock()
			response = rr.response
			err = rr.err
			rr.mu.RUnlock()
			return response, err
		}
	}
}
