package coordinator

import (
	"context"
	"distributed-job-runner/internal/job"
	"errors"
	"sync"
)

var ErrJobNotFound = errors.New("job not found")

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
	s.jobs[j.ID] = *j
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

func (s *MemoryJobStore) AppendLog(ctx context.Context, jobID, line string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	j, exists := s.jobs[jobID]
	if !exists {
		return ErrJobNotFound
	}
	// ponytail: one combined string is enough for demo logs; chunk storage if volume matters.
	j.Logs += line + "\n"
	s.jobs[jobID] = j
	return nil
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
	return &j, nil
}
