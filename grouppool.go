package retrypool

import (
	"context"
	"fmt"
	"log/slog"
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
	OnTaskSuccess func(gid GID, poolID uint, data T, metadata Metadata)
	// Called when a task fails. We automatically pass the group ID and the pool ID.
	OnTaskFailure func(gid GID, poolID uint, data T, metadata Metadata, err error) TaskAction
	// Called when a task is about to run. We pass the group ID, the pool ID, the *Task, and the worker ID.
	OnTaskAttempt func(gid GID, poolID uint, task *Task[T], workerID int)
	// Called whenever we get a snapshot from an underlying Pool. We pass that snapshot.
	OnSnapshot func(snapshot MetricsSnapshot[T])

	metadata Metadata
}

// GroupPoolOption is a functional option for configuring a GroupPool.
type GroupPoolOption[T any, GID comparable] func(cfg *GroupPoolConfig[T, GID])

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

func WithGroupPoolOnTaskSuccess[T any, GID comparable](cb func(gid GID, poolID uint, data T, metadata Metadata)) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.OnTaskSuccess = cb
	}
}

func WithGroupPoolOnTaskFailure[T any, GID comparable](cb func(gid GID, poolID uint, data T, metadata Metadata, err error) TaskAction) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.OnTaskFailure = cb
	}
}

func WithGroupPoolOnTaskAttempt[T any, GID comparable](cb func(gid GID, poolID uint, task *Task[T], workerID int)) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.OnTaskAttempt = cb
	}
}

func WithGroupPoolOnSnapshot[T any, GID comparable](cb func(snapshot MetricsSnapshot[T])) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.OnSnapshot = cb
	}
}

func WithGroupPoolMetadata[T any, GID comparable](m Metadata) GroupPoolOption[T, GID] {
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
	cfg.OnTaskSuccess = func(gid GID, poolID uint, data T, meta Metadata) {
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
	cfg.OnTaskFailure = func(gid GID, poolID uint, data T, meta Metadata, err error) TaskAction {
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
		ctx:          c,
		cancel:       cancel,
		config:       cfg,
		nextID:       0,
		pools:        make(map[uint]*poolItem[T, GID]),
		pendingTasks: make(map[GID][]T),
	}
	return gp, nil
}

// buildPool creates a new poolItem with the next available internal ID, wiring up user callbacks.
func (gp *GroupPool[T, GID]) buildPool() (*poolItem[T, GID], error) {
	w := gp.config.WorkerFactory()
	if w == nil {
		return nil, fmt.Errorf("failed to create worker")
	}
	id := gp.nextID
	gp.nextID++

	metadata := make(Metadata)
	metadata.Set("pool_id", id)

	// TODO: we should have options for that
	opts := []Option[T]{
		WithLogger[T](gp.config.Logger),
		WithSnapshotInterval[T](time.Second / 4),
		WithSnapshots[T](),
		WithMetadata[T](metadata),
	}

	// We already wrapped OnTaskSuccess/OnTaskFailure in the constructor,
	// so we just pass them along here.
	if gp.config.OnTaskSuccess != nil {
		userFn := gp.config.OnTaskSuccess
		opts = append(opts, WithOnTaskSuccess[T](func(data T, meta Metadata) {
			gid := data.GetGroupID()
			userFn(gid, id, data, meta)
		}))
	}
	if gp.config.OnTaskFailure != nil {
		userFn := gp.config.OnTaskFailure
		opts = append(opts, WithOnTaskFailure[T](func(data T, meta Metadata, err error) TaskAction {
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
	if gp.config.OnSnapshot != nil {
		userFn := gp.config.OnSnapshot
		opts = append(opts, WithSnapshotCallback[T](func(s MetricsSnapshot[T]) {
			userFn(s)
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
	gp.mu.Lock()
	defer gp.mu.Unlock()

	it, exists := gp.pools[poolID]
	if !exists {
		return
	}
	// Check if the pool is truly idle: no tasks in the queue, none processing
	q := it.Pool.QueueSize()
	p := it.Pool.ProcessingCount()
	if q == 0 && p == 0 {
		// This pool is done with its group. Let's free it.
		_ = gp.releasePool(poolID)
		// Now that we have a free pool, let's see if we can consume pending tasks from any group
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
			_ = gp.submitToPool(poolItem, t) // ignoring error for brevity
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
		options = append(options, WithTaskMetadata[T](cfg.metadata))
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
		}
		return subErr
	}
	return it.Pool.Submit(t, options...)
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
