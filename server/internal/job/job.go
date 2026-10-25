package job

type Status string

const (
	StatusQueued    Status = "queued"
	StatusCloning   Status = "cloning"
	StatusBuilding  Status = "building"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

// Job logs are split by worker
type Job struct {
	ID             string      `json:"id"`
	RepoURL        string      `json:"repoUrl"`
	DockerfilePath string      `json:"dockerfilePath"`
	ImageRef       string      `json:"imageRef"`
	Status         Status      `json:"status"`
	WorkerLogs     []WorkerLog `json:"workerLogs"`
}

// Worker logs are split by task
type WorkerLog struct {
	WorkerID int       `json:"workerId"`
	Tasks    []TaskLog `json:"tasks"`
}

// Task log is just one continuous string
type TaskLog struct {
	TaskID string `json:"taskId"`
	Logs   string `json:"logs"`
}

type JobSpec struct {
	JobID    string `json:"jobId"`
	TaskID   string `json:"taskId"`
	ImageRef string `json:"imageRef"`
}

type LogEvent struct {
	Line  *string `json:"line,omitempty"`
	Error string  `json:"error,omitempty"`
	Done  bool    `json:"done,omitempty"`
}
