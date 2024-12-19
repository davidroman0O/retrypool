package retrypool

type DependentTask[GID comparable, TID comparable] interface {
	GetDependencies() []TID
	GetGroupID() GID
	GetTaskID() TID
}
