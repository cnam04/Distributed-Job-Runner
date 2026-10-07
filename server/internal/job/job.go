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

type Job struct {
	ID             string `json:"id"`
	RepoURL        string `json:"repoUrl"`
	DockerfilePath string `json:"dockerfilePath"`
	ImageRef       string `json:"imageRef"`
	Status         Status `json:"status"`
	Logs           string `json:"logs"`
}

type JobSpec struct {
	JobID    string `json:"jobId"`
	ImageRef string `json:"imageRef"`
}

type LogEvent struct {
	Line  *string `json:"line,omitempty"`
	Error string  `json:"error,omitempty"`
	Done  bool    `json:"done,omitempty"`
}
