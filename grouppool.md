
I need to re-think and re-do my design

```go
var (
    ErrGroupNotFound   = errors.New("group not found")
    ErrGroupEnded      = errors.New("group already ended")
    ErrGroupFailed     = errors.New("group has failed")
    ErrInvalidGroupID  = errors.New("invalid group ID")
    ErrTaskGroupMismatch = errors.New("task group ID does not match submit group ID")
)

// GroupTask is the interface for tasks that have a group ID.
type GroupTask[GID comparable] interface {
	GetGroupID() GID
}

// GroupPoolConfig holds configuration options for the GroupPool.
type GroupPoolConfig[T GroupTask[GID], GID comparable] struct {
	Logger            Logger
	WorkerFactory     WorkerFactory[T]
	MaxActivePools    int
	MaxWorkersPerPool int
	UseFreeWorkerOnly bool
    GroupMustSucceed  bool // by default a group can have deadtask/failure, if true the pending tasks will be discarded

	OnTaskSuccess func(gid GID, poolID uint, data T, metadata map[string]any)
	OnTaskFailure func(gid GID, poolID uint, data T, metadata map[string]any, err error) TaskAction

    // GroupPool specific callback handled either OnTaskSuccess or OnTaskFailure
    OnTaskExecuted func(gid GID, poolID uint, data T, metadata map[string]any, err error)

	OnSnapshot    func(snapshot GroupMetricsSnapshot[T, GID])

	OnGroupStarts       func(gid GID, poolID uint)
	OnGroupEnds         func(gid GID, poolID uint)
    OnGroupFails        func(gid GID, poolID uint, pendingTasks []T) // only if `GroupMustSucceed` is enabled
}

// GroupPoolOption is a functional option for configuring a GroupPool.
type GroupPoolOption[T any, GID comparable] func(cfg *GroupPoolConfig[T, GID])

/*
func WithGroupPoolXXXX....
*/

func isZeroValueOfGroup[GID comparable](gid GID) bool {
	var zero GID
	return reflect.DeepEqual(gid, zero)
}

// Managed pool by group pool
type poolGroupPoolItem[T any, GID comparable] struct {
	mu            deadlock.RWMutex
	PoolID        uint
	Pool          Pooler[T]
	assignedGroup GID
	busy          bool
}

// HasFreeWorkers checks if pool has any available workers
func (pi *poolGroupPoolItem[T, GID]) HasFreeWorkers() bool {
    // - check if pool exists
    // - get free workers list
    // - return true if list not empty
}

// Not yet ready to be sent
// - no pool own that group yet
type poolGroupPending[T GroupTask[GID], GID comparable] struct {
    groupID      GID
    tasks        []T
}

// Own by a pool and active
type poolGroupActive[T GroupTask[GID], GID comparable] struct {
	poolID         uint
    groupID        GID
	isEnded        bool // will refuse new tasks
    isFailed       bool    // track group failure state  
    tasks          []T  // tasks not yet sent
    fails          []T  // failed tasks
}

type GroupPool[T GroupTask[GID], GID comparable] struct {
	mu                     deadlock.RWMutex
	ctx                    context.Context
	cancel                 context.CancelFunc
	config                 GroupPoolConfig[T, GID]

	pools                  map[uint]*poolGroupPoolItem[T, GID]

	activeGroups           map[GID]*poolGroupActive
	pendingGroups          map[GID]*poolGroupPending[T, GID]

	completedPendingGroups []GID

	// Keep accumulative group metrics for each active/pending group:
	snapshotMu     deadlock.RWMutex
	poolMetrics    map[uint]MetricsSnapshot[T] // Track metrics per pool
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
    // - validate context
    // - create cancel context
    // - create default config with:
    //   - logger set to debug level but disabled
    //   - MaxActivePools = -1 (unlimited)
    //   - MaxWorkersPerPool = -1 (unlimited)
    //   - UseFreeWorkerOnly = false
    //   - GroupMustSucceed = false
    // - apply options
    // - validate worker factory exists
    // - initialize maps:
    //   - pools
    //   - activeGroups
    //   - pendingGroups
    //   - poolMetrics
    //   - snapshotGroups
    // - initialize empty completedPendingGroups slice
    // - set nextPoolID to 0
    // - return pool instance
}

// WaitGroup waits for all tasks in a group to complete
func (gp *GroupPool[T, GID]) WaitGroup(ctx context.Context, gid GID) error {
    // - validate group ID not zero
    // - create ticker for polling (200ms default)
    // - loop:
    //   - check context cancellation
    //   - check if group is pending:
    //     - if pending and not ended, continue waiting
    //   - check if group is active:
    //     - get pool
    //     - check queue size and processing count
    //     - if both zero and group ended, return
    //   - if group not found and not pending, return (completed)
}

// Close shuts down all pools and cleans up resources
func (gp *GroupPool[T, GID]) Close() error {
    // - acquire lock
    // - cancel context
    // - for each pool:
    //   - close pool
    //   - collect first error
    // - clear maps:
    //   - pools
    //   - activeGroups
    //   - pendingGroups
    //   - poolMetrics
    //   - snapshotGroups
    // - clear completedPendingGroups
    // - return first error encountered if any
}

// GetSnapshot returns current metrics snapshot
func (gp *GroupPool[T, GID]) GetSnapshot() GroupMetricsSnapshot[T, GID] {
    // - acquire read lock
    // - return copy of snapshot
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


// buildPool creates a new internal pool with proper configuration and event handling.
// Returns the wrapped pool item or error if creation failed.
func (gp *GroupPool[T, GID]) buildPool() (*poolGroupPoolItem[T, GID], error) {
    // - create worker and validate
    // - generate pool ID 
    // - add pool metadata containing poolID
    // - create pool options:
    //   - logger from config
    //   - metadata 
    //   - snapshots enabled with interval
    //   - snapshot callback that:
    //     - updates internal metrics
    //     - triggers user snapshot callback if configured
    // - wrap and add success handler that:
    //   - extracts group metadata ($group containing $groupID and $poolID)
    //   - calls onTaskExecuted with success
    // - wrap and add failure handler that:
    //   - extracts group metadata ($group containing $groupID and $poolID)
    //   - calls onTaskExecuted with error
    //   - returns TaskActionRemove
    // - create pool with worker and options
    // - wrap in poolGroupPoolItem and return
}

// Submit task to a group
func (gp *GroupPool[T, GID]) Submit(gid GID, task T, opts ...GroupTaskOption[T, GID]) error {
    // - validate group ID not zero value
    // - validate task's group ID matches submitted gid
    // - acquire lock for state access
    
    // - if check if group is active or not
    //  - if active then:
    //      - CHECK IF GROUP IS ENDED -> return error if true
    //      - CHECK IF GROUP FAILED (GroupMustSucceed=true) -> return error
    //      - check if we have tasks pending before that task, we just append our task to that tasks array or our active group
    //      - else then, if we enabled `UseFreeWorkerOnly` we have to check if we have free workers
    //          - if not, then we append the task to the `tasks` of our active group
    //          - else, we can use the private submit function that will handle our task and opts
    //  - if not active then:
    //      - CHECK IF GROUP WAS FAILED -> return error
    //      - check if pending group exists (if not create it) and assign the task to that pending group 
    //      - ATTEMPT TO ACTIVATE GROUP (might succeed right away if resources available)
    // - will add extra metadata: $groupID $poolID which will be helpful when onTaskExecuted is executed
}

// submitToPool handles submitting a task to a specific pool with proper configuration
// Manages notifications, metadata and submission method based on pool config
func (gp *GroupPool[T, GID]) submitToPool(pi *poolGroupPoolItem[T, GID], t T, opts ...GroupTaskOption[T, GID]) error {
    // - process task options into config
    // - build task options array:
    //   - add queue notification if configured
    //   - add processed notification if configured
    //   - build metadata combining:
    //     - user provided metadata
    //     - group tracking metadata ($group with $groupID and $poolID)
    // - if UseFreeWorkerOnly enabled:
    //   - use SubmitToFreeWorker
    // - else:
    //   - use normal Submit
}

// Signal we ended the group
// Once a group is ended, it will no longer accept new tasks
// Group remains active until all pending tasks are processed
func (gp *GroupPool[T, GID]) EndGroup(gid GID) error {
    // - validate group ID not zero value
    // - acquire lock for state access
    
    // - check if group is active:
    //   - if active:
    //     - mark isEnded = true
    //     - return success
    
    // - check if group is pending:
    //   - if pending and not already ended:
    //     - add to completedPendingGroups if not already there
    //     - mark pending group as ended
    //     - store end timestamp
    //     - try to activate pending group (it might get activated right away if resources available)
    //     - return success
    
    // - if group not found in either active or pending:
    //   - return error (group not found or already completed)
}

// onTaskExecuted becomes the key driver of task flow
func (gp *GroupPool[T, GID]) onTaskExecuted(gid GID, poolID uint, data T, metadata map[string]any, err error) {
    // - get active group
    // - handle error if GroupMustSucceed:
    //   - mark group as failed
    //   - clean up pool assignment
    //   - mark pool as not busy
    //   - trigger OnGroupFails with remaining tasks
    //   - clear metrics
    //   - ATTEMPT TO ACTIVATE PENDING GROUP since we freed a pool
    //   - return early
    
    // - if group has pending tasks:
    //   - CHECK IF GROUP IS ENDED before submitting next task
    //   - submit next task immediately
    //   - if submit fails:
    //     - add to fails list
    //     - if GroupMustSucceed, handle failure as above
    
    // - if no more tasks and group marked ended:
    //   - trigger OnGroupEnds
    //   - cleanup group
    //   - clean up pool assignment
    //   - mark pool as not busy
    //   - clear metrics
    //   - attempt to activate another pending group
    
    // - trigger OnTaskExecuted callback
}

// tryActivatePendingGroup attempts to activate a pending group if resources available
// Only triggered during key events (submit, task execution completion, etc.)
func (gp *GroupPool[T, GID]) tryActivatePendingGroup() error {
    // - find free pool if any exists
    // - if no free pool and can create new pool (check MaxActivePools):
    //   - create new pool
    //   - add to pools map
    // - if no pool available, return (groups stay pending)
    
    // Important: handle completed pending groups first
    // - check completedPendingGroups list:
    //   - if any exist, take first
    //   - get its pending group
    //   - remove from pending groups
    //   - activate it with available pool
    //   - return
    
    // If no completed pending groups:
    // - check other pending groups:
    //   - find one that has start instruction
    //   - validate group can be activated
    //   - create active group with:
    //     - pool ID
    //     - group ID 
    //     - pending tasks list
    //     - empty fails list
    //   - update pool state:
    //     - assign group
    //     - mark busy
    //   - update mappings:
    //     - remove from pending groups
    //     - add to active groups
    //   - initialize metrics if needed
    //   - trigger OnGroupStarts
    //   - submit first task if any exist
    //   - break after first group activated
}

// Helper to properly cleanup group resources
func (gp *GroupPool[T, GID]) cleanupGroup(gid GID, poolID uint) {
    // - remove from active groups
    // - cleanup pool assignment
    // - clear metrics
    // - mark pool as not busy
}

// Helper to release pool for reuse
func (gp *GroupPool[T, GID]) releasePool(poolID uint) {
    // - clear pool assignment
    // - mark as not busy
    // - attempt to activate pending group
}

```


