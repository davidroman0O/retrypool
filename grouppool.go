package retrypool

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"sync/atomic"
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
	MaxActivePools    int
	MaxWorkersPerPool int
	UseFreeWorkerOnly bool

	OnTaskSuccess func(gid GID, poolID uint, data T, metadata map[string]any)
	OnTaskFailure func(gid GID, poolID uint, data T, metadata map[string]any, err error) TaskAction
	OnTaskAttempt func(gid GID, poolID uint, task *Task[T], workerID int)
	OnSnapshot    func(snapshot GroupMetricsSnapshot[T, GID])

	OnGroupStart func(gid GID, poolID uint)
	OnGroupEnd   func(gid GID, poolID uint)

	metadata *Metadata
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

func WithGroupPoolOnGroupStart[T any, GID comparable](cb func(gid GID, poolID uint)) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.OnGroupStart = cb
	}
}

func WithGroupPoolOnGroupEnd[T any, GID comparable](cb func(gid GID, poolID uint)) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.OnGroupEnd = cb
	}
}

func WithGroupPoolMetadata[T any, GID comparable](m *Metadata) GroupPoolOption[T, GID] {
	return func(cfg *GroupPoolConfig[T, GID]) {
		cfg.metadata = m
	}
}

// GroupConfig can hold per-group config if needed.
type GroupConfig[T GroupTask[GID], GID comparable] struct{}

type GroupOption[T GroupTask[GID], GID comparable] func(*GroupConfig[T, GID])

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

type instructionType int

const (
	instructStart instructionType = iota
	instructSubmit
	instructEnd
)

type instruction[T GroupTask[GID], GID comparable] struct {
	instruct  instructionType
	task      T
	groupOpts []GroupOption[T, GID]
	taskOpts  []GroupTaskOption[T, GID]
	timestamp time.Time
}

type pendingGroup[T GroupTask[GID], GID comparable] struct {
	instructions []instruction[T, GID]
	config       GroupConfig[T, GID]
	isEnded      bool
	endTime      time.Time
}

type groupState struct {
	poolID         uint
	isActive       bool
	isEnded        bool
	taskCount      atomic.Int64
	completedCount atomic.Int64
}

type poolItem[T any, GID comparable] struct {
	mu            deadlock.RWMutex
	PoolID        uint
	Pool          Pooler[T]
	assignedGroup GID
	busy          bool
}

func isZeroValueOfGroup[GID comparable](gid GID) bool {
	var zero GID
	return reflect.DeepEqual(gid, zero)
}

// PoolMetricsSnapshot represents metrics for a single pool.
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
	// These totals are GLOBAL and never reset.
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

	copy(clone.PendingTasks, m.PendingTasks)
	copy(clone.Metrics, m.Metrics)

	return clone
}

type GroupPool[T GroupTask[GID], GID comparable] struct {
	mu                     deadlock.RWMutex
	ctx                    context.Context
	cancel                 context.CancelFunc
	config                 GroupPoolConfig[T, GID]
	pools                  map[uint]*poolItem[T, GID]
	activeGroups           map[GID]*groupState
	pendingGroups          map[GID]*pendingGroup[T, GID]
	completedPendingGroups []GID

	// Keep accumulative group metrics for each active/pending group:
	snapshotMu     deadlock.RWMutex
	poolMetrics    map[uint]MetricsSnapshot[T] // Track metrics per pool
	snapshotGroups map[GID]MetricsSnapshot[T]

	// The final snapshot
	snapshot GroupMetricsSnapshot[T, GID]

	nextPoolID uint
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

	c, cancel := context.WithCancel(ctx)
	gp := &GroupPool[T, GID]{
		ctx:                    c,
		cancel:                 cancel,
		config:                 cfg,
		pools:                  make(map[uint]*poolItem[T, GID]),
		activeGroups:           make(map[GID]*groupState),
		pendingGroups:          make(map[GID]*pendingGroup[T, GID]),
		completedPendingGroups: make([]GID, 0),
		snapshotGroups:         make(map[GID]MetricsSnapshot[T]),
		poolMetrics:            make(map[uint]MetricsSnapshot[T]),
	}
	return gp, nil
}

// buildPool creates a new internal Pool. We wrap user callbacks to detect completion.
func (gp *GroupPool[T, GID]) buildPool() (*poolItem[T, GID], error) {
	w := gp.config.WorkerFactory()
	if w == nil {
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
				// Store the latest metrics for this pool
				gp.poolMetrics[id] = ms

				// Calculate and update snapshot under the same lock
				gp.calculateMetricsSnapshotLocked()
			}
			// Get a copy of the snapshot while still holding the lock
			snapshot := gp.snapshot.Clone()
			gp.snapshotMu.Unlock()

			// Call user callback outside the lock with the copy
			if gp.config.OnSnapshot != nil {
				gp.config.OnSnapshot(snapshot)
			}
		}),
	}

	// Wrap success
	if gp.config.OnTaskSuccess != nil {
		userFn := gp.config.OnTaskSuccess
		opts = append(opts, WithOnTaskSuccess[T](func(data T, meta map[string]any) {
			gidVal := data.GetGroupID()
			if !isZeroValueOfGroup(gidVal) {
				userFn(gidVal, id, data, meta)
			}
			go gp.handleTaskCompletion(id)
		}))
	} else {
		opts = append(opts, WithOnTaskSuccess[T](func(data T, meta map[string]any) {
			go gp.handleTaskCompletion(id)
		}))
	}

	// Wrap failure
	if gp.config.OnTaskFailure != nil {
		userFn := gp.config.OnTaskFailure
		opts = append(opts, WithOnTaskFailure[T](func(data T, meta map[string]any, err error) TaskAction {
			gidVal := data.GetGroupID()
			var action TaskAction
			if !isZeroValueOfGroup(gidVal) {
				action = userFn(gidVal, id, data, meta, err)
			} else {
				action = TaskActionRemove
			}
			go gp.handleTaskCompletion(id)
			return action
		}))
	} else {
		opts = append(opts, WithOnTaskFailure[T](func(data T, meta map[string]any, err error) TaskAction {
			go gp.handleTaskCompletion(id)
			return TaskActionRemove
		}))
	}

	// Wrap attempt
	if gp.config.OnTaskAttempt != nil {
		userFn := gp.config.OnTaskAttempt
		opts = append(opts, WithOnTaskAttempt[T](func(tsk *Task[T], wid int) {
			gidVal := tsk.data.GetGroupID()
			if !isZeroValueOfGroup(gidVal) {
				userFn(gidVal, id, tsk, wid)
			}
		}))
	}

	p := New[T](gp.ctx, []Worker[T]{w}, opts...)

	return &poolItem[T, GID]{
		PoolID:        id,
		Pool:          p,
		assignedGroup: *new(GID),
		busy:          false,
	}, nil
}

// StartGroup tries to start a group.
func (gp *GroupPool[T, GID]) StartGroup(gid GID, opts ...GroupOption[T, GID]) error {
	if isZeroValueOfGroup(gid) {
		return fmt.Errorf("invalid group ID: zero value")
	}
	gp.mu.Lock()
	defer gp.mu.Unlock()

	if _, found := gp.activeGroups[gid]; found {
		gp.config.Logger.Debug(gp.ctx, "StartGroup called again, ignoring", "groupID", gid)
		return nil
	}

	pg, pending := gp.pendingGroups[gid]
	if !pending {
		pg = &pendingGroup[T, GID]{}
		gp.pendingGroups[gid] = pg
	}
	alreadyStart := false
	for _, ins := range pg.instructions {
		if ins.instruct == instructStart {
			alreadyStart = true
			break
		}
	}
	if !alreadyStart {
		pg.instructions = append(pg.instructions, instruction[T, GID]{
			instruct:  instructStart,
			timestamp: time.Now(),
			groupOpts: opts,
		})
	}

	return gp.tryActivatePendingGroupLocked(gid)
}

// SubmitToGroup enqueues a task for the group.
func (gp *GroupPool[T, GID]) SubmitToGroup(gid GID, task T, opts ...GroupTaskOption[T, GID]) error {
	if isZeroValueOfGroup(gid) {
		return fmt.Errorf("invalid group ID: zero value")
	}
	if gid != task.GetGroupID() {
		return fmt.Errorf("task's group ID %v does not match SubmitToGroup ID %v", task.GetGroupID(), gid)
	}

	gp.mu.Lock()
	defer gp.mu.Unlock()

	st, active := gp.activeGroups[gid]
	if active {
		pi, found := gp.pools[st.poolID]
		if !found {
			return fmt.Errorf("pool %d not found for active group %v", st.poolID, gid)
		}
		return gp.submitToPool(pi, task, opts...)
	}

	pg, pending := gp.pendingGroups[gid]
	if !pending {
		pg = &pendingGroup[T, GID]{}
		gp.pendingGroups[gid] = pg
	}
	pg.instructions = append(pg.instructions, instruction[T, GID]{
		instruct:  instructSubmit,
		task:      task,
		taskOpts:  opts,
		timestamp: time.Now(),
	})
	return gp.tryActivatePendingGroupLocked(gid)
}

// EndGroup signals no more tasks for the group.
func (gp *GroupPool[T, GID]) EndGroup(gid GID) error {
	if isZeroValueOfGroup(gid) {
		return fmt.Errorf("invalid group ID: zero value")
	}

	gp.mu.Lock()
	defer gp.mu.Unlock()

	st, active := gp.activeGroups[gid]
	if active {
		st.isEnded = true
		return nil
	}

	pg, pending := gp.pendingGroups[gid]
	if !pending {
		return nil
	}
	if pg.isEnded {
		return nil
	}
	pg.instructions = append(pg.instructions, instruction[T, GID]{
		instruct:  instructEnd,
		timestamp: time.Now(),
	})
	pg.isEnded = true
	pg.endTime = time.Now()

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

	return gp.tryActivatePendingGroupLocked(gid)
}

// WaitGroup waits until the group completes (pool is idle).
func (gp *GroupPool[T, GID]) WaitGroup(ctx context.Context, gid GID) error {
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			gp.mu.Lock()
			st, active := gp.activeGroups[gid]
			if !active {
				pg, exist := gp.pendingGroups[gid]
				if !exist {
					gp.mu.Unlock()
					return nil
				}
				if pg.isEnded {
					gp.mu.Unlock()
					return nil
				}
				gp.mu.Unlock()
				continue
			}
			pi, ok := gp.pools[st.poolID]
			if !ok {
				gp.mu.Unlock()
				return nil
			}
			p := pi.Pool
			gp.mu.Unlock()

			if p.QueueSize() == 0 && p.ProcessingCount() == 0 {
				return nil
			}
		}
	}
}

// Close tears down all pools, discarding any pending instructions.
func (gp *GroupPool[T, GID]) Close() error {
	gp.mu.Lock()
	defer gp.mu.Unlock()
	gp.cancel()

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
	gp.activeGroups = make(map[GID]*groupState)
	gp.pendingGroups = make(map[GID]*pendingGroup[T, GID])
	gp.completedPendingGroups = nil
	gp.snapshotGroups = make(map[GID]MetricsSnapshot[T])
	return firstErr
}

func (gp *GroupPool[T, GID]) findOrCreateFreePoolLocked() (*poolItem[T, GID], error) {
	var free *poolItem[T, GID]
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
	activeCount := 0
	for _, pi := range gp.pools {
		pi.mu.RLock()
		if pi.busy {
			activeCount++
		}
		pi.mu.RUnlock()
	}
	if gp.config.MaxActivePools >= 0 && activeCount >= gp.config.MaxActivePools {
		return nil, nil
	}
	newp, err := gp.buildPool()
	if err != nil {
		return nil, err
	}
	gp.pools[newp.PoolID] = newp
	return newp, nil
}

func (gp *GroupPool[T, GID]) tryActivatePendingGroupLocked(gid GID) error {
	pg, ok := gp.pendingGroups[gid]
	if !ok {
		return nil
	}
	if !gp.hasStart(pg) {
		return nil
	}
	pi, err := gp.findOrCreateFreePoolLocked()
	if err != nil {
		return err
	}
	if pi == nil {
		return nil
	}
	delete(gp.pendingGroups, gid)
	pi.mu.Lock()
	pi.assignedGroup = gid
	pi.busy = true
	pi.mu.Unlock()

	st := &groupState{
		poolID:   pi.PoolID,
		isActive: true,
		isEnded:  pg.isEnded,
	}
	gp.activeGroups[gid] = st

	// Initialize the metrics snapshot for this group if needed
	if _, exists := gp.snapshotGroups[gid]; !exists {
		gp.snapshotGroups[gid] = MetricsSnapshot[T]{}
	}

	if gp.config.OnGroupStart != nil {
		gp.config.OnGroupStart(gid, pi.PoolID)
	}
	return gp.replayInstructionsLocked(gid, pi, pg)
}

func (gp *GroupPool[T, GID]) hasStart(pg *pendingGroup[T, GID]) bool {
	for _, ins := range pg.instructions {
		if ins.instruct == instructStart {
			return true
		}
	}
	return false
}

func (gp *GroupPool[T, GID]) replayInstructionsLocked(gid GID, pi *poolItem[T, GID], pg *pendingGroup[T, GID]) error {
	for _, ins := range pg.instructions {
		switch ins.instruct {
		case instructStart:
		case instructSubmit:

			//// TODO: we should take in account if there are free workers or not, if we have UseFreeWorkerOnly that mean we should wait for a free worker before sending more tasks
			// pi.mu.Lock()
			// wrs := pi.Pool.GetFreeWorkers()
			// pi.mu.Unlock()

			if err := gp.submitToPool(pi, ins.task, ins.taskOpts...); err != nil {
				gp.config.Logger.Warn(gp.ctx, "Error replaying submit", "gid", gid, "err", err)
			}
		case instructEnd:
			st := gp.activeGroups[gid]
			st.isEnded = true
		}
	}
	return nil
}

func (gp *GroupPool[T, GID]) submitToPool(pi *poolItem[T, GID], t T, opts ...GroupTaskOption[T, GID]) error {
	cfg := &groupSubmitConfig{}
	for _, o := range opts {
		o(cfg)
	}

	var taskOpts []TaskOption[T]
	if cfg.queueNotification != nil {
		taskOpts = append(taskOpts, WithTaskQueuedNotification[T](cfg.queueNotification))
	}
	if cfg.processedNotification != nil {
		taskOpts = append(taskOpts, WithTaskProcessedNotification[T](cfg.processedNotification))
	}
	if cfg.metadata != nil {
		m := NewMetadata()
		for k, v := range cfg.metadata {
			m.Set(k, v)
		}
		taskOpts = append(taskOpts, WithTaskMetadata[T](m))
	}

	if gp.config.UseFreeWorkerOnly {
		if err := gp.scaleWorkersIfNeeded(pi); err != nil && err != ErrNoWorkersAvailable {
			return err
		}
		err := pi.Pool.SubmitToFreeWorker(t, taskOpts...)
		if err == ErrNoWorkersAvailable {
			if scErr := gp.scaleWorkersIfNeeded(pi); scErr != nil && scErr != ErrNoWorkersAvailable {
				return scErr
			}
			err = pi.Pool.SubmitToFreeWorker(t, taskOpts...)
			if err == ErrNoWorkersAvailable {
				return err
			}
		}
		return err
	} else {
		err := pi.Pool.Submit(t, taskOpts...)
		if err == ErrNoWorkersAvailable {
			return err
		}
		return err
	}
}

func (gp *GroupPool[T, GID]) scaleWorkersIfNeeded(pi *poolItem[T, GID]) error {
	free := pi.Pool.GetFreeWorkers()
	if len(free) > 0 {
		return nil
	}
	if gp.config.MaxWorkersPerPool == 0 {
		return ErrNoWorkersAvailable
	}
	if gp.config.MaxWorkersPerPool < 0 {
		w := gp.config.WorkerFactory()
		if w == nil {
			return fmt.Errorf("worker factory returned nil")
		}
		return pi.Pool.Add(w, nil)
	}
	ws, err := pi.Pool.Workers()
	if err != nil {
		return err
	}
	if len(ws) < gp.config.MaxWorkersPerPool {
		w := gp.config.WorkerFactory()
		if w == nil {
			return fmt.Errorf("worker factory returned nil")
		}
		return pi.Pool.Add(w, nil)
	}
	return ErrNoWorkersAvailable
}

func (gp *GroupPool[T, GID]) handleTaskCompletion(poolID uint) {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	pi, ok := gp.pools[poolID]
	if !ok {
		return
	}
	gid := pi.assignedGroup
	if isZeroValueOfGroup(gid) {
		return
	}
	st, active := gp.activeGroups[gid]
	if !active {
		return
	}
	p := pi.Pool

	if st.isEnded && p.QueueSize() == 0 && p.ProcessingCount() == 0 {
		delete(gp.activeGroups, gid)

		if gp.config.OnGroupEnd != nil {
			gp.config.OnGroupEnd(gid, poolID)
		}

		// Remove the group from snapshotGroups after we have merged metrics into global counters (which we do continuously).
		delete(gp.snapshotGroups, gid)

		pi.mu.Lock()
		pi.assignedGroup = *new(GID)
		pi.busy = false
		pi.mu.Unlock()

		gp.consumeFreedPoolLocked(pi)
	}
}

func (gp *GroupPool[T, GID]) consumeFreedPoolLocked(pi *poolItem[T, GID]) {
	if len(gp.completedPendingGroups) > 0 {
		gid := gp.completedPendingGroups[0]
		gp.completedPendingGroups = gp.completedPendingGroups[1:]
		pg, ok := gp.pendingGroups[gid]
		if !ok {
			gp.consumeFreedPoolLocked(pi)
			return
		}
		delete(gp.pendingGroups, gid)

		pi.mu.Lock()
		pi.assignedGroup = gid
		pi.busy = true
		pi.mu.Unlock()

		st := &groupState{
			poolID:   pi.PoolID,
			isActive: true,
			isEnded:  pg.isEnded,
		}
		gp.activeGroups[gid] = st

		if _, exists := gp.snapshotGroups[gid]; !exists {
			gp.snapshotGroups[gid] = MetricsSnapshot[T]{}
		}

		if gp.config.OnGroupStart != nil {
			gp.config.OnGroupStart(gid, pi.PoolID)
		}
		_ = gp.replayInstructionsLocked(gid, pi, pg)
		return
	}
	// any other pending group
	for gid, pg := range gp.pendingGroups {
		if gp.hasStart(pg) {
			delete(gp.pendingGroups, gid)

			pi.mu.Lock()
			pi.assignedGroup = gid
			pi.busy = true
			pi.mu.Unlock()

			st := &groupState{
				poolID:   pi.PoolID,
				isActive: true,
				isEnded:  pg.isEnded,
			}
			gp.activeGroups[gid] = st
			if _, exists := gp.snapshotGroups[gid]; !exists {
				gp.snapshotGroups[gid] = MetricsSnapshot[T]{}
			}
			if gp.config.OnGroupStart != nil {
				gp.config.OnGroupStart(gid, pi.PoolID)
			}
			_ = gp.replayInstructionsLocked(gid, pi, pg)
			return
		}
	}
}

// // Public version acquires lock
// func (gp *GroupPool[T, GID]) calculateMetricsSnapshot() {
// 	gp.snapshotMu.Lock()
// 	gp.calculateMetricsSnapshotLocked()
// 	gp.snapshotMu.Unlock()
// }

// calculateMetricsSnapshot aggregates metrics from internal pools, pending groups, etc.
// We do NOT reset global counters (TotalTasksSubmitted, etc.). We read them from the global atomic fields.
// Private locked version - caller must hold snapshotMu
func (gp *GroupPool[T, GID]) calculateMetricsSnapshotLocked() {
	var snap GroupMetricsSnapshot[T, GID]

	// Calculate totals from pool metrics
	for _, metrics := range gp.poolMetrics {
		snap.TotalTasksSubmitted += metrics.TasksSubmitted
		snap.TotalTasksProcessed += metrics.TasksProcessed
		snap.TotalTasksSucceeded += metrics.TasksSucceeded
		snap.TotalTasksFailed += metrics.TasksFailed
		snap.TotalDeadTasks += metrics.DeadTasks
	}

	gp.mu.RLock()
	snap.TotalPools = len(gp.pools)
	snap.Pools = make([]PoolMetricsSnapshot[T], 0, len(gp.pools))

	activeCount := 0
	for id, pi := range gp.pools {
		pi.mu.RLock()
		assigned := pi.assignedGroup
		busy := pi.busy
		pi.mu.RUnlock()

		if busy {
			activeCount++
		}

		ws := make(map[int]WorkerSnapshot[T])
		qSize := int64(0)
		if pi.Pool != nil {
			pi.Pool.RangeWorkers(func(wid int, s WorkerSnapshot[T]) bool {
				ws[wid] = s
				return true
			})
			qSize = pi.Pool.QueueSize()
		}

		snap.Pools = append(snap.Pools, PoolMetricsSnapshot[T]{
			PoolID:        id,
			AssignedGroup: assigned,
			IsActive:      busy,
			QueueSize:     qSize,
			Workers:       ws,
		})
	}
	snap.ActivePools = activeCount

	snap.PendingTasks = make([]PendingTasksSnapshot[GID], 0, len(gp.pendingGroups))
	snap.TotalPendingTasks = 0
	snap.GroupsWithPending = len(gp.pendingGroups)
	for gid, pg := range gp.pendingGroups {
		c := len(pg.instructions)
		snap.TotalPendingTasks += c
		snap.PendingTasks = append(snap.PendingTasks, PendingTasksSnapshot[GID]{
			GroupID:      gid,
			TasksPending: c,
		})
	}

	// Collect group metrics
	activeOrPending := make(map[GID]bool)
	for g := range gp.activeGroups {
		activeOrPending[g] = true
	}
	for g := range gp.pendingGroups {
		activeOrPending[g] = true
	}

	for gID := range gp.snapshotGroups {
		if !activeOrPending[gID] {
			continue
		}
		st, inActive := gp.activeGroups[gID]
		pg, inPend := gp.pendingGroups[gID]

		var pid uint
		var hasPool bool
		var isPending bool
		var pendCount int

		if inActive {
			pid = st.poolID
			hasPool = true
		}
		if inPend {
			isPending = true
			pendCount = len(pg.instructions)
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

func (gp *GroupPool[T, GID]) GetSnapshot() GroupMetricsSnapshot[T, GID] {
	gp.snapshotMu.RLock()
	defer gp.snapshotMu.RUnlock()
	return gp.snapshot
}

func (gp *GroupPool[T, GID]) AddWorkerToPool(poolID uint, queue TaskQueue[T]) error {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	pi, ok := gp.pools[poolID]
	if !ok {
		return fmt.Errorf("pool %d not found", poolID)
	}
	if gp.config.MaxWorkersPerPool == 0 {
		return fmt.Errorf("maxWorkersPerPool=0, cannot add worker")
	}
	ws, err := pi.Pool.Workers()
	if err != nil {
		return err
	}
	if gp.config.MaxWorkersPerPool > 0 && len(ws) >= gp.config.MaxWorkersPerPool {
		return fmt.Errorf("pool %d reached max worker limit", poolID)
	}
	w := gp.config.WorkerFactory()
	if w == nil {
		return fmt.Errorf("worker factory returned nil")
	}
	return pi.Pool.Add(w, queue)
}

func (gp *GroupPool[T, GID]) RemoveWorkerFromPool(poolID uint, workerID int) error {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	pi, ok := gp.pools[poolID]
	if !ok {
		return fmt.Errorf("pool %d not found", poolID)
	}
	return pi.Pool.Remove(workerID)
}

func (gp *GroupPool[T, GID]) PauseWorkerInPool(poolID uint, workerID int) error {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	pi, ok := gp.pools[poolID]
	if !ok {
		return fmt.Errorf("pool %d not found", poolID)
	}
	return pi.Pool.Pause(workerID)
}

func (gp *GroupPool[T, GID]) ResumeWorkerInPool(poolID uint, workerID int) error {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	pi, ok := gp.pools[poolID]
	if !ok {
		return fmt.Errorf("pool %d not found", poolID)
	}
	return pi.Pool.Resume(workerID)
}

func (gp *GroupPool[T, GID]) WorkersInPool(poolID uint) ([]int, error) {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	pi, ok := gp.pools[poolID]
	if !ok {
		return nil, fmt.Errorf("pool %d not found", poolID)
	}
	return pi.Pool.Workers()
}

func (gp *GroupPool[T, GID]) GetFreeWorkersInPool(poolID uint) []int {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

	pi, ok := gp.pools[poolID]
	if !ok {
		return nil
	}
	return pi.Pool.GetFreeWorkers()
}

func (gp *GroupPool[T, GID]) SetConcurrentPools(max int) {
	if max < 1 {
		max = 1
	}
	gp.mu.Lock()
	gp.config.MaxActivePools = max
	gp.mu.Unlock()
}

func (gp *GroupPool[T, GID]) SetConcurrentWorkers(max int) {
	if max < 1 {
		max = 1
	}
	gp.mu.Lock()
	gp.config.MaxWorkersPerPool = max
	gp.mu.Unlock()
}

func (gp *GroupPool[T, GID]) QueueSize() int64 {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

	var total int64
	for _, pi := range gp.pools {
		pi.mu.RLock()
		p := pi.Pool
		pi.mu.RUnlock()
		total += p.QueueSize()
	}
	return total
}

func (gp *GroupPool[T, GID]) ProcessingCount() int64 {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

	var total int64
	for _, pi := range gp.pools {
		pi.mu.RLock()
		p := pi.Pool
		pi.mu.RUnlock()
		total += p.ProcessingCount()
	}
	return total
}

func (gp *GroupPool[T, GID]) DeadTaskCount() int64 {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

	var total int64
	for _, pi := range gp.pools {
		pi.mu.RUnlock()
		pi.mu.RLock()
		p := pi.Pool
		pi.mu.RUnlock()
		total += p.DeadTaskCount()
	}
	return total
}

func (gp *GroupPool[T, GID]) RangeDeadTasks(fn func(*DeadTask[T]) bool) {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

outer:
	for _, pi := range gp.pools {
		pi.mu.RLock()
		p := pi.Pool
		pi.mu.RUnlock()
		cont := true
		p.RangeDeadTasks(func(dt *DeadTask[T]) bool {
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

func (gp *GroupPool[T, GID]) PullDeadTask(poolID uint, idx int) (*DeadTask[T], error) {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	pi, ok := gp.pools[poolID]
	if !ok {
		return nil, fmt.Errorf("pool %d not found", poolID)
	}
	pi.mu.RLock()
	defer pi.mu.RUnlock()
	return pi.Pool.PullDeadTask(idx)
}

func (gp *GroupPool[T, GID]) PullRangeDeadTasks(from, to int) ([]*DeadTask[T], error) {
	if from >= to {
		return nil, fmt.Errorf("invalid range")
	}
	gp.mu.Lock()
	defer gp.mu.Unlock()

	var result []*DeadTask[T]
	offset := 0
	for _, pi := range gp.pools {
		pi.mu.RLock()
		p := pi.Pool
		count := int(p.DeadTaskCount())
		pi.mu.RUnlock()

		end := offset + count
		if end <= from {
			offset = end
			continue
		}
		if offset >= to {
			break
		}
		if from <= offset && end <= to {
			dts, err := p.PullRangeDeadTasks(0, count)
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
		dts, err := p.PullRangeDeadTasks(localFrom, localTo)
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

func (gp *GroupPool[T, GID]) RangeWorkerQueues(f func(workerID int, queueSize int64) bool) {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

outer:
	for _, pi := range gp.pools {
		pi.mu.RLock()
		p := pi.Pool
		pi.mu.RUnlock()
		cont := true
		p.RangeWorkerQueues(func(wid int, qs int64) bool {
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

func (gp *GroupPool[T, GID]) RangeTaskQueues(f func(workerID int, tq TaskQueue[T]) bool) {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

outer:
	for _, pi := range gp.pools {
		pi.mu.RLock()
		p := pi.Pool
		pi.mu.RUnlock()
		cont := true
		p.RangeTaskQueues(func(wid int, queue TaskQueue[T]) bool {
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

func (gp *GroupPool[T, GID]) RangeWorkers(f func(workerID int, state WorkerSnapshot[T]) bool) {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

outer:
	for _, pi := range gp.pools {
		pi.mu.RLock()
		p := pi.Pool
		pi.mu.RUnlock()
		cont := true
		p.RangeWorkers(func(wid int, ws WorkerSnapshot[T]) bool {
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
