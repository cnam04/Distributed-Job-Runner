package job

type TaskStatus string

const (
	TaskPending           TaskStatus = "pending"
	TaskCloning           TaskStatus = "pulling image"
	TaskCreatingContainer TaskStatus = "creating containers"
	TaskRunning           TaskStatus = "running"
	TaskCompleted         TaskStatus = "completed"
	TaskFailed            TaskStatus = "failed"
)

type Task struct {
	ID       string     `json:"id"`
	JobID    string     `json:"jobId"`
	ImageRef string     `json:"imageRef"`
	Status   TaskStatus `json:"status"`
	Logs     string     `json:"logs"`
}
