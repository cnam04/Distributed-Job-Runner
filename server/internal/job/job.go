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
}

type JobSpec struct {
	JobID    string `json:"jobId"`
	ImageRef string `json:"imageRef"`
}
