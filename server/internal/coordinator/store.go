package coordinator

import (
	"context"
	"distributed-job-runner/internal/job"
	"errors"
	"sync"
)

var ErrJobNotFound = errors.New("job not found")
var ErrWorkerNotFound = errors.New("worker not found")
var ErrTaskNotFound = errors.New("task not found")

// ponytail: process-local jobs have no retention limit; add persistent storage and retention when durability or capacity requires it.
type MemoryJobStore struct {
	mu   sync.RWMutex
	jobs map[string]job.Job
}

func NewMemoryJobStore() *MemoryJobStore {
	return &MemoryJobStore{jobs: make(map[string]job.Job)}
}

func (s *MemoryJobStore) Create(ctx context.Context, j *job.Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.jobs[j.ID]; exists {
		return errors.New("job already exists")
	}
	s.jobs[j.ID] = cloneJob(*j)
	return nil
}

func (s *MemoryJobStore) UpdateStatus(ctx context.Context, jobID string, status job.Status) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	j, exists := s.jobs[jobID]
	if !exists {
		return ErrJobNotFound
	}
	j.Status = status
	s.jobs[jobID] = j
	return nil
}

func (s *MemoryJobStore) SetImageRef(ctx context.Context, jobID, imageRef string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	j, exists := s.jobs[jobID]
	if !exists {
		return ErrJobNotFound
	}
	j.ImageRef = imageRef
	s.jobs[jobID] = j
	return nil
}

// Assign a task to the logs for a specific worker
func (s *MemoryJobStore) AssignTask(ctx context.Context, jobID string, workerID int, taskID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	j, exists := s.jobs[jobID]
	if !exists {
		return ErrJobNotFound
	}
	for i := range j.WorkerLogs {
		if j.WorkerLogs[i].WorkerID == workerID {
			j.WorkerLogs[i].Tasks = append(j.WorkerLogs[i].Tasks, job.TaskLog{TaskID: taskID})
			s.jobs[jobID] = j
			return nil
		}
	}
	return ErrWorkerNotFound
}

func (s *MemoryJobStore) AppendLog(ctx context.Context, jobID string, workerID int, taskID, line string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	j, exists := s.jobs[jobID]
	if !exists {
		return ErrJobNotFound
	}
	// Add log line to the task logs for that worker's task
	for i := range j.WorkerLogs {
		if j.WorkerLogs[i].WorkerID != workerID {
			continue
		}
		for k := range j.WorkerLogs[i].Tasks {
			if j.WorkerLogs[i].Tasks[k].TaskID == taskID {
				// ponytail: task output remains one string; chunk it if log volume becomes a limit.
				j.WorkerLogs[i].Tasks[k].Logs += line + "\n"
				s.jobs[jobID] = j
				return nil
			}
		}
		return ErrTaskNotFound
	}
	return ErrWorkerNotFound
}

func cloneJob(j job.Job) job.Job {
	workerLogs := make([]job.WorkerLog, len(j.WorkerLogs))
	copy(workerLogs, j.WorkerLogs)
	for i := range workerLogs {
		workerLogs[i].Tasks = append([]job.TaskLog{}, j.WorkerLogs[i].Tasks...)
	}
	j.WorkerLogs = workerLogs
	return j
}

func (s *MemoryJobStore) Get(ctx context.Context, jobID string) (*job.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, exists := s.jobs[jobID]
	if !exists {
		return nil, ErrJobNotFound
	}
	j = cloneJob(j)
	return &j, nil
}
