package retrypool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/sasha-s/go-deadlock"
)

var (
	ErrGroupNotFound     = errors.New("group not found")
	ErrGroupEnded        = errors.New("group already ended")
	ErrGroupFailed       = errors.New("group has failed")
	ErrInvalidGroupID    = errors.New("invalid group ID")
	ErrTaskGroupMismatch = errors.New("task group ID does not match submit group ID")
)

// GroupTask is the interface for tasks that have a group ID
type GroupTask[GID comparable] interface {
	GetGroupID() GID
}

// GroupPoolConfig holds configuration options for the GroupPool
type GroupPoolConfig[T GroupTask[GID], GID comparable] struct {
	Logger            Logger
	WorkerFactory     WorkerFactory[T]
	MaxActivePools    int
	MaxWorkersPerPool int
	UseFreeWorkerOnly bool
	GroupMustSucceed  bool // By default a group can have deadtask/failure, if true the pending tasks will be discarded

	OnTaskSuccess func(gid GID, poolID uint, data T, metadata map[string]any)
	OnTaskFailure func(gid GID, poolID uint, data T, metadata map[string]any, err error) TaskAction

	// GroupPool specific callback handled either OnTaskSuccess or OnTaskFailure
	OnTaskExecuted func(gid GID, poolID uint, data T, metadata map[string]any, err error)

	OnSnapshot func(snapshot GroupMetricsSnapshot[T, GID])

	OnGroupStarts func(gid GID, poolID uint)
	OnGroupEnds   func(gid GID, poolID uint)
	OnGroupFails  func(gid GID, poolID uint, pendingTasks []T) // Only if GroupMustSucceed is enabled

	metadata *Metadata
}

// GroupOption is a functional option for configuring a GroupPool
type GroupPoolOption[T GroupTask[GID], GID comparable] func(*GroupPoolConfig[T, GID])

func WithGroupPoolLogger[T GroupTask[GID], GID comparable](logger Logger) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.Logger = logger
	}
}

func WithGroupPoolWorkerFactory[T GroupTask[GID], GID comparable](wf WorkerFactory[T]) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.WorkerFactory = wf
	}
}

func WithGroupPoolMaxActivePools[T GroupTask[GID], GID comparable](max int) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.MaxActivePools = max
	}
}

func WithGroupPoolMaxWorkersPerPool[T GroupTask[GID], GID comparable](max int) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.MaxWorkersPerPool = max
	}
}

func WithGroupPoolUseFreeWorkerOnly[T GroupTask[GID], GID comparable]() GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.UseFreeWorkerOnly = true
	}
}

func WithGroupPoolOnTaskSuccess[T GroupTask[GID], GID comparable](cb func(gid GID, poolID uint, data T, metadata map[string]any)) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.OnTaskSuccess = cb
	}
}

func WithGroupPoolOnTaskFailure[T GroupTask[GID], GID comparable](cb func(gid GID, poolID uint, data T, metadata map[string]any, err error) TaskAction) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.OnTaskFailure = cb
	}
}

func WithGroupPoolOnTaskExecuted[T GroupTask[GID], GID comparable](cb func(gid GID, poolID uint, data T, metadata map[string]any, err error)) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.OnTaskExecuted = cb
	}
}

func WithGroupPoolOnSnapshot[T GroupTask[GID], GID comparable](cb func(snapshot GroupMetricsSnapshot[T, GID])) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.OnSnapshot = cb
	}
}

func WithGroupPoolOnGroupStarts[T GroupTask[GID], GID comparable](cb func(gid GID, poolID uint)) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.OnGroupStarts = cb
	}
}

func WithGroupPoolOnGroupEnds[T GroupTask[GID], GID comparable](cb func(gid GID, poolID uint)) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.OnGroupEnds = cb
	}
}

func WithGroupPoolOnGroupFails[T GroupTask[GID], GID comparable](cb func(gid GID, poolID uint, pendingTasks []T)) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.OnGroupFails = cb
	}
}

func WithGroupPoolGroupMustSucceed[T GroupTask[GID], GID comparable](must bool) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.GroupMustSucceed = must
	}
}

func WithGroupPoolMetadata[T GroupTask[GID], GID comparable](m *Metadata) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.metadata = m
	}
}

func isZeroValueOfGroup[GID comparable](gid GID) bool {
	var zero GID
	return reflect.DeepEqual(gid, zero)
}

// Managed pool by group pool
type poolGroupPoolItem[T GroupTask[GID], GID comparable] struct {
	mu            deadlock.RWMutex
	PoolID        uint
	Pool          Pooler[T]
	assignedGroup GID
	busy          bool
}

// HasFreeWorkers checks if pool has any available workers
func (pi *poolGroupPoolItem[T, GID]) HasFreeWorkers() bool {
	if pi.Pool == nil {
		return false
	}

	freeWorkers := pi.Pool.GetFreeWorkers()
	return len(freeWorkers) > 0
}

// Not yet ready to be sent
// - no pool own that group yet
type poolGroupPending[T GroupTask[GID], GID comparable] struct {
	groupID GID
	tasks   []T
	isEnded bool
	endTime time.Time
}

// Own by a pool and active
type poolGroupActive[T GroupTask[GID], GID comparable] struct {
	poolID   uint
	groupID  GID
	isEnded  bool // Will refuse new tasks
	isFailed bool // Track group failure state
	tasks    []T  // Tasks not yet sent
	fails    []T  // Failed tasks
}

type GroupPool[T GroupTask[GID], GID comparable] struct {
	mu     deadlock.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	config GroupPoolConfig[T, GID]

	pools map[uint]*poolGroupPoolItem[T, GID]

	activeGroups  map[GID]*poolGroupActive[T, GID]
	pendingGroups map[GID]*poolGroupPending[T, GID]

	completedPendingGroups []GID

	// Keep accumulative group metrics
	snapshotMu     deadlock.RWMutex
	poolMetrics    map[uint]MetricsSnapshot[T]
	snapshotGroups map[GID]MetricsSnapshot[T]

	// The final snapshot
	snapshot GroupMetricsSnapshot[T, GID]

	nextPoolID uint
}

// NewGroupPool creates a new GroupPool instance with provided options
func NewGroupPool[T GroupTask[GID], GID comparable](
	ctx context.Context,
	opts ...GroupPoolOption[T, GID],
) (*GroupPool[T, GID], error) {
	if ctx == nil {
		return nil, errors.New("context cannot be nil")
	}

	ctx, cancel := context.WithCancel(ctx)

	cfg := GroupPoolConfig[T, GID]{
		Logger:            NewLogger(slog.LevelDebug),
		MaxActivePools:    -1,
		MaxWorkersPerPool: -1,
		UseFreeWorkerOnly: false,
		GroupMustSucceed:  false,
	}
	cfg.Logger.Disable()

	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.WorkerFactory == nil {
		defer cancel()
		return nil, fmt.Errorf("worker factory is required")
	}

	gp := &GroupPool[T, GID]{
		ctx:                    ctx,
		cancel:                 cancel,
		config:                 cfg,
		pools:                  make(map[uint]*poolGroupPoolItem[T, GID]),
		activeGroups:           make(map[GID]*poolGroupActive[T, GID]),
		pendingGroups:          make(map[GID]*poolGroupPending[T, GID]),
		completedPendingGroups: make([]GID, 0),
		poolMetrics:            make(map[uint]MetricsSnapshot[T]),
		snapshotGroups:         make(map[GID]MetricsSnapshot[T]),
	}

	return gp, nil
}

// buildPool creates a new internal pool with proper configuration and event handling
func (gp *GroupPool[T, GID]) buildPool() (*poolGroupPoolItem[T, GID], error) {
	worker := gp.config.WorkerFactory()
	if worker == nil {
		return nil, fmt.Errorf("worker factory returned nil")
	}

	id := gp.nextPoolID
	gp.nextPoolID++

	metadata := NewMetadata()
	metadata.Set("pool_id", id)

	opts := []Option[T]{
		WithLogger[T](gp.config.Logger),
		WithMetadata[T](metadata),
		WithSnapshots[T](),
		WithSnapshotInterval[T](time.Millisecond * 100),
		WithSnapshotCallback[T](func(ms MetricsSnapshot[T]) {
			gp.snapshotMu.Lock()
			if _, havePool := gp.pools[id]; havePool {
				gp.poolMetrics[id] = ms
				gp.calculateMetricsSnapshot()
			}
			snapshot := gp.snapshot.Clone()
			gp.snapshotMu.Unlock()

			if gp.config.OnSnapshot != nil {
				gp.config.OnSnapshot(snapshot)
			}
		}),
	}

	// Set success/failure handlers that call OnTaskExecuted
	opts = append(opts, WithOnTaskSuccess[T](func(data T, meta map[string]any) {
		if gp.config.OnTaskExecuted != nil {
			retrypoolMeta, ok := meta[RetrypoolMetadataKey].(map[string]any)
			if !ok {
				retrypoolMeta = map[string]any{}
			}
			gp.config.OnTaskExecuted(data.GetGroupID(), id, data, retrypoolMeta, nil)
		}
		if gp.config.OnTaskSuccess != nil {
			gp.config.OnTaskSuccess(data.GetGroupID(), id, data, meta)
		}
		gp.onTaskExecuted(data.GetGroupID(), id, data, meta, nil)
	}))

	opts = append(opts, WithOnTaskFailure[T](func(data T, meta map[string]any, err error) TaskAction {
		if gp.config.OnTaskExecuted != nil {
			retrypoolMeta, ok := meta[RetrypoolMetadataKey].(map[string]any)
			if !ok {
				retrypoolMeta = map[string]any{}
			}
			gp.config.OnTaskExecuted(data.GetGroupID(), id, data, retrypoolMeta, err)
		}

		var action TaskAction
		if gp.config.OnTaskFailure != nil {
			action = gp.config.OnTaskFailure(data.GetGroupID(), id, data, meta, err)
		} else {
			action = TaskActionRemove
		}

		gp.onTaskExecuted(data.GetGroupID(), id, data, meta, err)
		return action
	}))

	pool := New[T](gp.ctx, []Worker[T]{worker}, opts...)

	// Add more workers if configured for more than 1
	if gp.config.MaxWorkersPerPool != 0 && gp.config.MaxWorkersPerPool != 1 {
		// Add workers up to max or just one more for unlimited
		targetWorkers := gp.config.MaxWorkersPerPool
		if targetWorkers < 0 {
			targetWorkers = 2 // For unlimited, start with 2 workers
		}

		for i := 1; i < targetWorkers; i++ {
			w := gp.config.WorkerFactory()
			if w == nil {
				return nil, fmt.Errorf("worker factory returned nil for additional worker")
			}
			if err := pool.Add(w, nil); err != nil {
				return nil, fmt.Errorf("failed to add additional worker: %w", err)
			}
		}
	}

	return &poolGroupPoolItem[T, GID]{
		PoolID:        id,
		Pool:          pool,
		assignedGroup: *new(GID),
		busy:          false,
	}, nil
}

func (gp *GroupPool[T, GID]) SetMaxActivePools(max int) {
	gp.mu.Lock()
	defer gp.mu.Unlock()
	gp.config.MaxActivePools = max
}

func (gp *GroupPool[T, GID]) SetMaxWorkersPerPool(max int) {
	gp.mu.Lock()
	defer gp.mu.Unlock()
	gp.config.MaxWorkersPerPool = max
}

// Submit task to a group
func (gp *GroupPool[T, GID]) Submit(gid GID, task T, opts ...GroupTaskOption[T, GID]) error {
	if isZeroValueOfGroup(gid) {
		return ErrInvalidGroupID
	}

	if gid != task.GetGroupID() {
		return ErrTaskGroupMismatch
	}

	gp.mu.Lock()
	defer gp.mu.Unlock()

	// Check if group is active
	if active, ok := gp.activeGroups[gid]; ok {
		// Group exists and is active

		if active.isEnded {
			return ErrGroupEnded
		}

		if active.isFailed && gp.config.GroupMustSucceed {
			return ErrGroupFailed
		}

		// Get assigned pool
		pi, ok := gp.pools[active.poolID]
		if !ok {
			return fmt.Errorf("pool %d not found for active group %v", active.poolID, gid)
		}

		// If we have pending tasks, append this one
		if len(active.tasks) > 0 {
			active.tasks = append(active.tasks, task)
			return nil
		}

		// If we need free workers, check availability
		if gp.config.UseFreeWorkerOnly && !pi.HasFreeWorkers() {
			active.tasks = append(active.tasks, task)
			return nil
		}

		// Submit directly
		return gp.submitToPool(pi, task, opts...)

	} else {
		// Group is not active yet

		// Check if group failed before
		if pg, exists := gp.pendingGroups[gid]; exists && pg.isEnded && gp.config.GroupMustSucceed {
			return ErrGroupFailed
		}

		// Get or create pending group
		pg, ok := gp.pendingGroups[gid]
		if !ok {
			pg = &poolGroupPending[T, GID]{
				groupID: gid,
				tasks:   make([]T, 0),
			}
			gp.pendingGroups[gid] = pg
		}

		// Add task to pending
		pg.tasks = append(pg.tasks, task)

		// Try to activate the group
		return gp.tryActivatePendingGroup()
	}
}

// scaleWorkersForPool handles worker scaling for a pool based on constraints
func (gp *GroupPool[T, GID]) scaleWorkersForPool(pi *poolGroupPoolItem[T, GID]) error {
	// No scaling needed if max is 0
	if gp.config.MaxWorkersPerPool == 0 {
		return nil
	}

	// Get current worker count
	workers, err := pi.Pool.Workers()
	if err != nil {
		return err
	}

	currentCount := len(workers)

	// Handle unlimited case (-1)
	if gp.config.MaxWorkersPerPool < 0 {
		// Add one more worker
		w := gp.config.WorkerFactory()
		if w == nil {
			return fmt.Errorf("worker factory returned nil")
		}
		return pi.Pool.Add(w, nil)
	}

	// Handle limited case
	if currentCount < gp.config.MaxWorkersPerPool {
		// Add one more worker up to limit
		w := gp.config.WorkerFactory()
		if w == nil {
			return fmt.Errorf("worker factory returned nil")
		}
		return pi.Pool.Add(w, nil)
	}

	return nil
}

// submitToPool handles submitting a task to a specific pool with proper configuration
func (gp *GroupPool[T, GID]) submitToPool(pi *poolGroupPoolItem[T, GID], t T, opts ...GroupTaskOption[T, GID]) error {
	cfg := &groupSubmitConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	var taskOpts []TaskOption[T]
	if cfg.queueNotification != nil {
		taskOpts = append(taskOpts, WithTaskQueuedNotification[T](cfg.queueNotification))
	}
	if cfg.processedNotification != nil {
		taskOpts = append(taskOpts, WithTaskProcessedNotification[T](cfg.processedNotification))
	}

	// Build metadata merging existing and group info
	meta := NewMetadata()
	if cfg.metadata != nil {
		for k, v := range cfg.metadata {
			meta.Set(k, v)
		}
	}
	meta.Set("$group", map[string]interface{}{
		"$groupID": t.GetGroupID(),
		"$poolID":  pi.PoolID,
	})
	taskOpts = append(taskOpts, WithTaskMetadata[T](meta))

	if gp.config.UseFreeWorkerOnly {
		err := pi.Pool.SubmitToFreeWorker(t, taskOpts...)
		if err == ErrNoWorkersAvailable {
			// Try scaling up workers
			if scaleErr := gp.scaleWorkersForPool(pi); scaleErr != nil && scaleErr != ErrNoWorkersAvailable {
				return scaleErr
			}
			// Try submit again
			err = pi.Pool.SubmitToFreeWorker(t, taskOpts...)
		}
		return err
	}

	// Normal submit path
	err := pi.Pool.Submit(t, taskOpts...)
	if err == ErrNoWorkersAvailable {
		// Try scaling up workers
		if scaleErr := gp.scaleWorkersForPool(pi); scaleErr != nil && scaleErr != ErrNoWorkersAvailable {
			return scaleErr
		}
		// Try submit again
		err = pi.Pool.Submit(t, taskOpts...)
	}
	return err
}

// EndGroup signals no more tasks for the group, but allows existing tasks to complete
func (gp *GroupPool[T, GID]) EndGroup(gid GID) error {
	if isZeroValueOfGroup(gid) {
		return ErrInvalidGroupID
	}

	gp.mu.Lock()
	defer gp.mu.Unlock()

	// Check if group is active - just mark as ended and continue processing
	if active, ok := gp.activeGroups[gid]; ok {
		// Just mark as ended - tasks will continue processing
		active.isEnded = true
		return nil
	}

	// Check if group is pending
	pg, ok := gp.pendingGroups[gid]
	if !ok {
		return ErrGroupNotFound
	}

	if pg.isEnded {
		return ErrGroupEnded
	}

	// Mark pending group as ended, but keep tasks
	pg.isEnded = true
	pg.endTime = time.Now()

	// Add to completed pending groups if not already there
	found := false
	for _, cgid := range gp.completedPendingGroups {
		if cgid == gid {
			found = true
			break
		}
	}
	if !found {
		gp.completedPendingGroups = append(gp.completedPendingGroups, gid)
	}

	// Try to activate group - tasks will be processed normally
	return gp.tryActivatePendingGroup()
}

// onTaskExecuted becomes the key driver of task flow
func (gp *GroupPool[T, GID]) onTaskExecuted(gid GID, poolID uint, data T, metadata map[string]any, err error) {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	active, ok := gp.activeGroups[gid]
	if !ok {
		return
	}

	// If group must succeed and there was an error
	if err != nil && gp.config.GroupMustSucceed {
		active.isFailed = true

		// Get remaining tasks
		remaining := active.tasks

		// Clean up pool
		pi := gp.pools[poolID]
		if pi != nil {
			pi.mu.Lock()
			pi.assignedGroup = *new(GID)
			pi.busy = false
			pi.mu.Unlock()
		}

		// Clear metrics
		delete(gp.snapshotGroups, gid)
		delete(gp.activeGroups, gid)

		// Notify failure
		if gp.config.OnGroupFails != nil {
			gp.config.OnGroupFails(gid, poolID, remaining)
		}

		// Try to activate next group
		gp.tryActivatePendingGroup()
		return
	}

	// Check for more pending tasks
	if len(active.tasks) > 0 {

		// Submit next task
		next := active.tasks[0]
		active.tasks = active.tasks[1:]

		pi := gp.pools[poolID]
		if err := gp.submitToPool(pi, next); err != nil {
			active.fails = append(active.fails, next)
			if gp.config.GroupMustSucceed {
				active.isFailed = true

				if gp.config.OnGroupFails != nil {
					gp.config.OnGroupFails(gid, poolID, active.tasks)
				}

				delete(gp.activeGroups, gid)
				pi.mu.Lock()
				pi.assignedGroup = *new(GID)
				pi.busy = false
				pi.mu.Unlock()

				gp.tryActivatePendingGroup()
			}
		}
		return
	}

	// Only clean up when no more tasks AND ended
	if len(active.tasks) == 0 && active.isEnded {
		// Notify group end
		if gp.config.OnGroupEnds != nil {
			gp.config.OnGroupEnds(gid, poolID)
		}

		// Clean up group
		delete(gp.activeGroups, gid)
		delete(gp.snapshotGroups, gid)

		// Clean up pool
		pi := gp.pools[poolID]
		if pi != nil {
			pi.mu.Lock()
			pi.assignedGroup = *new(GID)
			pi.busy = false
			pi.mu.Unlock()
		}

		// Try to activate next group
		gp.tryActivatePendingGroup()
	}
}

// tryActivatePendingGroup attempts to activate a pending group if resources available
func (gp *GroupPool[T, GID]) tryActivatePendingGroup() error {
	// Find or create available pool
	pi, err := gp.findOrCreateFreePoolLocked()
	if err != nil {
		return err
	}
	if pi == nil {
		// No pool available
		return nil
	}

	// Handle completed pending groups first
	if len(gp.completedPendingGroups) > 0 {
		gid := gp.completedPendingGroups[0]
		gp.completedPendingGroups = gp.completedPendingGroups[1:]

		pg, ok := gp.pendingGroups[gid]
		if !ok {
			return gp.tryActivatePendingGroup()
		}

		// Create active group - even if ended we activate to process remaining tasks
		active := &poolGroupActive[T, GID]{
			poolID:   pi.PoolID,
			groupID:  gid,
			isEnded:  pg.isEnded,
			isFailed: false,
			tasks:    make([]T, len(pg.tasks)),
			fails:    make([]T, 0),
		}
		copy(active.tasks, pg.tasks)

		// Update mappings
		delete(gp.pendingGroups, gid)
		gp.activeGroups[gid] = active

		// Update pool
		pi.mu.Lock()
		pi.assignedGroup = gid
		pi.busy = true
		pi.mu.Unlock()

		// Initialize metrics if needed
		if _, exists := gp.snapshotGroups[gid]; !exists {
			gp.snapshotGroups[gid] = MetricsSnapshot[T]{}
		}

		// Notify group start
		if gp.config.OnGroupStarts != nil {
			gp.config.OnGroupStarts(gid, pi.PoolID)
		}

		// Submit first task if any
		if len(active.tasks) > 0 {
			task := active.tasks[0]
			active.tasks = active.tasks[1:]
			if err := gp.submitToPool(pi, task); err != nil {
				active.fails = append(active.fails, task)
				if gp.config.GroupMustSucceed {
					active.isFailed = true
					if gp.config.OnGroupFails != nil {
						gp.config.OnGroupFails(gid, pi.PoolID, active.tasks)
					}
					delete(gp.activeGroups, gid)
					pi.mu.Lock()
					pi.assignedGroup = *new(GID)
					pi.busy = false
					pi.mu.Unlock()
					return gp.tryActivatePendingGroup()
				}
			}
		}

		return nil
	}

	// Try other pending groups
	for gid, pg := range gp.pendingGroups {
		// Create active group
		active := &poolGroupActive[T, GID]{
			poolID:   pi.PoolID,
			groupID:  gid,
			isEnded:  pg.isEnded,
			isFailed: false,
			tasks:    make([]T, len(pg.tasks)),
			fails:    make([]T, 0),
		}
		copy(active.tasks, pg.tasks)

		// Update mappings
		delete(gp.pendingGroups, gid)
		gp.activeGroups[gid] = active

		// Update pool
		pi.mu.Lock()
		pi.assignedGroup = gid
		pi.busy = true
		pi.mu.Unlock()

		// Initialize metrics if needed
		if _, exists := gp.snapshotGroups[gid]; !exists {
			gp.snapshotGroups[gid] = MetricsSnapshot[T]{}
		}

		// Notify group start
		if gp.config.OnGroupStarts != nil {
			gp.config.OnGroupStarts(gid, pi.PoolID)
		}

		// Submit first task if any
		if len(active.tasks) > 0 {
			task := active.tasks[0]
			active.tasks = active.tasks[1:]
			if err := gp.submitToPool(pi, task); err != nil {
				active.fails = append(active.fails, task)
				if gp.config.GroupMustSucceed {
					active.isFailed = true
					if gp.config.OnGroupFails != nil {
						gp.config.OnGroupFails(gid, pi.PoolID, active.tasks)
					}
					delete(gp.activeGroups, gid)
					pi.mu.Lock()
					pi.assignedGroup = *new(GID)
					pi.busy = false
					pi.mu.Unlock()
					return gp.tryActivatePendingGroup()
				}
			}
		}

		break // Only activate one group at a time
	}

	return nil
}

// findOrCreateFreePoolLocked finds an available pool or creates a new one if allowed
func (gp *GroupPool[T, GID]) findOrCreateFreePoolLocked() (*poolGroupPoolItem[T, GID], error) {
	// Look for existing free pool
	var free *poolGroupPoolItem[T, GID]
	for _, pi := range gp.pools {
		pi.mu.RLock()
		busy := pi.busy
		pi.mu.RUnlock()
		if !busy {
			free = pi
			break
		}
	}

	if free != nil {
		return free, nil
	}

	// Count active pools
	activeCount := 0
	for _, pi := range gp.pools {
		pi.mu.RLock()
		if pi.busy {
			activeCount++
		}
		pi.mu.RUnlock()
	}

	// Check if we can create new pool
	if gp.config.MaxActivePools >= 0 && activeCount >= gp.config.MaxActivePools {
		return nil, nil
	}

	// Create new pool
	newPool, err := gp.buildPool()
	if err != nil {
		return nil, err
	}
	gp.pools[newPool.PoolID] = newPool
	return newPool, nil
}

// WaitGroup waits for all tasks in a group to complete
func (gp *GroupPool[T, GID]) WaitGroup(ctx context.Context, gid GID) error {
	if isZeroValueOfGroup(gid) {
		return ErrInvalidGroupID
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			gp.mu.Lock()

			// Check if group is active
			if active, ok := gp.activeGroups[gid]; ok {
				pi, exist := gp.pools[active.poolID]
				if !exist {
					gp.mu.Unlock()
					continue
				}

				// We only complete if no more tasks and ended
				qSize := pi.Pool.QueueSize()
				pCount := pi.Pool.ProcessingCount()
				if qSize == 0 && pCount == 0 && len(active.tasks) == 0 && active.isEnded {
					gp.mu.Unlock()
					continue
				}

				gp.mu.Unlock()
				continue
			}

			// Check if group is pending
			if pg, exists := gp.pendingGroups[gid]; exists {
				if !pg.isEnded {
					gp.mu.Unlock()
					continue
				}
				gp.mu.Unlock()
				continue
			}

			// Group not found -> completed
			gp.mu.Unlock()
			return nil
		}
	}
}

// Close gracefully shuts down all pools and cleans up resources
func (gp *GroupPool[T, GID]) Close() error {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	// Cancel context
	gp.cancel()

	// Close all pools
	var firstErr error
	for id, pi := range gp.pools {
		pi.mu.Lock()
		p := pi.Pool
		pi.mu.Unlock()

		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(gp.pools, id)
	}

	// Clear maps
	gp.activeGroups = make(map[GID]*poolGroupActive[T, GID])
	gp.pendingGroups = make(map[GID]*poolGroupPending[T, GID])
	gp.completedPendingGroups = nil
	gp.poolMetrics = make(map[uint]MetricsSnapshot[T])
	gp.snapshotGroups = make(map[GID]MetricsSnapshot[T])

	return firstErr
}

// GetSnapshot returns current metrics snapshot
func (gp *GroupPool[T, GID]) GetSnapshot() GroupMetricsSnapshot[T, GID] {
	gp.snapshotMu.RLock()
	defer gp.snapshotMu.RUnlock()
	return gp.snapshot.Clone()
}

// calculateMetricsSnapshot aggregates metrics from internal pools and groups
func (gp *GroupPool[T, GID]) calculateMetricsSnapshot() {
	snap := GroupMetricsSnapshot[T, GID]{
		Pools:        make([]PoolMetricsSnapshot[T], 0),
		PendingTasks: make([]PendingTasksSnapshot[GID], 0),
		Metrics:      make([]GroupMetricSnapshot[T, GID], 0),
	}

	// Sum up totals from pool metrics
	for _, metrics := range gp.poolMetrics {
		snap.TotalTasksSubmitted += metrics.TasksSubmitted
		snap.TotalTasksProcessed += metrics.TasksProcessed
		snap.TotalTasksSucceeded += metrics.TasksSucceeded
		snap.TotalTasksFailed += metrics.TasksFailed
		snap.TotalDeadTasks += metrics.DeadTasks
	}

	// Collect pools info
	gp.mu.RLock()
	snap.TotalPools = len(gp.pools)

	activeCount := 0
	for id, pi := range gp.pools {
		pi.mu.RLock()
		assigned := pi.assignedGroup
		busy := pi.busy
		pi.mu.RUnlock()

		if busy {
			activeCount++
		}

		workers := make(map[int]WorkerSnapshot[T])
		qSize := int64(0)
		if pi.Pool != nil {
			pi.Pool.RangeWorkers(func(wid int, s WorkerSnapshot[T]) bool {
				workers[wid] = s
				return true
			})
			qSize = pi.Pool.QueueSize()
		}

		snap.Pools = append(snap.Pools, PoolMetricsSnapshot[T]{
			PoolID:        id,
			AssignedGroup: assigned,
			IsActive:      busy,
			QueueSize:     qSize,
			Workers:       workers,
		})
	}
	snap.ActivePools = activeCount

	// Collect pending groups info
	snap.GroupsWithPending = len(gp.pendingGroups)
	for gid, pg := range gp.pendingGroups {
		snap.TotalPendingTasks += len(pg.tasks)
		snap.PendingTasks = append(snap.PendingTasks, PendingTasksSnapshot[GID]{
			GroupID:      gid,
			TasksPending: len(pg.tasks),
		})
	}

	// Track groups to collect metrics for
	activeOrPending := make(map[GID]bool)
	for g := range gp.activeGroups {
		activeOrPending[g] = true
	}
	for g := range gp.pendingGroups {
		activeOrPending[g] = true
	}

	// Collect metrics for each group
	for gID := range gp.snapshotGroups {
		if !activeOrPending[gID] {
			continue
		}

		active, inActive := gp.activeGroups[gID]
		pending, inPending := gp.pendingGroups[gID]

		var pid uint
		var hasPool bool
		var isPending bool
		var pendCount int

		if inActive {
			pid = active.poolID
			hasPool = true
		}

		if inPending {
			isPending = true
			pendCount = len(pending.tasks)
		}

		snap.Metrics = append(snap.Metrics, GroupMetricSnapshot[T, GID]{
			GroupID:         gID,
			PoolID:          pid,
			HasPool:         hasPool,
			IsPending:       isPending,
			TasksPending:    pendCount,
			MetricsSnapshot: gp.snapshotGroups[gID],
		})
	}
	gp.mu.RUnlock()

	gp.snapshot = snap
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

// PoolMetricsSnapshot represents metrics for a single pool
type PoolMetricsSnapshot[T any] struct {
	PoolID        uint
	AssignedGroup any
	IsActive      bool
	QueueSize     int64
	Workers       map[int]WorkerSnapshot[T]
}

type PendingTasksSnapshot[GID comparable] struct {
	GroupID      GID
	TasksPending int
}

type GroupMetricSnapshot[T any, GID comparable] struct {
	GroupID         GID
	PoolID          uint
	HasPool         bool
	IsPending       bool
	TasksPending    int
	MetricsSnapshot MetricsSnapshot[T]
}

type GroupMetricsSnapshot[T any, GID comparable] struct {
	// These totals are GLOBAL and never reset
	TotalTasksSubmitted int64
	TotalTasksProcessed int64
	TotalTasksSucceeded int64
	TotalTasksFailed    int64
	TotalDeadTasks      int64

	TotalPools        int
	ActivePools       int
	TotalPendingTasks int
	GroupsWithPending int

	Pools        []PoolMetricsSnapshot[T]
	PendingTasks []PendingTasksSnapshot[GID]
	Metrics      []GroupMetricSnapshot[T, GID]
}

// Clone creates a deep copy of the snapshot
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
		Pools:               make([]PoolMetricsSnapshot[T], len(m.Pools)),
		PendingTasks:        make([]PendingTasksSnapshot[GID], len(m.PendingTasks)),
		Metrics:             make([]GroupMetricSnapshot[T, GID], len(m.Metrics)),
	}

	// Deep copy pools
	for i, pool := range m.Pools {
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
		clone.Pools[i] = poolClone
	}

	// Copy simple slices
	copy(clone.PendingTasks, m.PendingTasks)
	copy(clone.Metrics, m.Metrics)

	return clone
}

// GroupRequestResponse manages a request/response pair for tasks in a group.
type GroupRequestResponse[T any, R any, GID comparable] struct {
	groupID     GID
	request     T
	done        chan struct{}
	response    R
	err         error
	mu          deadlock.RWMutex
	isCompleted bool
}

func NewGroupRequestResponse[T any, R any, GID comparable](req T, gid GID) *GroupRequestResponse[T, R, GID] {
	return &GroupRequestResponse[T, R, GID]{
		request: req,
		done:    make(chan struct{}),
		groupID: gid,
	}
}

func (rr GroupRequestResponse[T, R, GID]) GetGroupID() GID {
	return rr.groupID
}

func (rr *GroupRequestResponse[T, R, GID]) ConsultRequest(fn func(T) error) error {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	return fn(rr.request)
}

func (rr *GroupRequestResponse[T, R, GID]) Complete(resp R) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	if !rr.isCompleted {
		rr.response = resp
		rr.isCompleted = true
		close(rr.done)
	}
}

func (rr *GroupRequestResponse[T, R, GID]) CompleteWithError(err error) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	if !rr.isCompleted {
		rr.err = err
		rr.isCompleted = true
		close(rr.done)
	}
}

func (rr *GroupRequestResponse[T, R, GID]) Done() <-chan struct{} {
	rr.mu.RLock()
	defer rr.mu.RUnlock()
	return rr.done
}

func (rr *GroupRequestResponse[T, R, GID]) Err() error {
	rr.mu.RLock()
	defer rr.mu.RUnlock()
	return rr.err
}

func (rr *GroupRequestResponse[T, R, GID]) Wait(ctx context.Context) (R, error) {
	select {
	case <-rr.Done():
		rr.mu.RLock()
		defer rr.mu.RUnlock()
		return rr.response, rr.err
	case <-ctx.Done():
		rr.mu.Lock()
		if !rr.isCompleted {
			rr.err = ctx.Err()
			rr.isCompleted = true
			close(rr.done)
		}
		r := rr.response
		e := rr.err
		rr.mu.Unlock()
		return r, e
	}
}
