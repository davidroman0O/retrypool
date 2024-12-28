package retrypool

type IndependentDependentTask[GID comparable, TID comparable] interface {
	GetDependencies() []TID
	GetGroupID() GID
	GetTaskID() TID
}

type BlockingDependentTask[GID comparable, TID comparable] interface {
	GetGroupID() GID
	GetTaskID() TID
}

// DependencyMode defines how dependencies are interpreted
type DependencyMode int

const (
	// ForwardMode means tasks depend on previous tasks (A depends on B means A waits for B)
	ForwardMode DependencyMode = iota
	// ReverseMode means tasks depend on subsequent tasks (A depends on B means B waits for A)
	ReverseMode
)
