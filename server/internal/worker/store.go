package worker

import (
	"context"
	"distributed-job-runner/internal/job"
	"errors"
	"sync"
)

var ErrTaskNotFound = errors.New("task not found")

// ponytail: process-local tasks have no retention limit; add persistent storage and retention when durability or capacity requires it.
type WorkerStatusStore struct {
	mu           sync.RWMutex
	runningTasks map[string]job.Task
}

func NewWorkerStatusStore() *WorkerStatusStore {
	return &WorkerStatusStore{runningTasks: make(map[string]job.Task)}
}

func (s *WorkerStatusStore) Create(ctx context.Context, task *job.Task) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.runningTasks[task.ID]; exists {
		return errors.New("task already exists")
	}
	s.runningTasks[task.ID] = *task
	return nil
}

func (s *WorkerStatusStore) UpdateStatus(ctx context.Context, taskID string, status job.TaskStatus) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	task, exists := s.runningTasks[taskID]
	if !exists {
		return ErrTaskNotFound
	}
	task.Status = status
	s.runningTasks[taskID] = task
	return nil
}

func (s *WorkerStatusStore) AppendLog(ctx context.Context, taskID, line string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	task, exists := s.runningTasks[taskID]
	if !exists {
		return ErrTaskNotFound
	}
	// ponytail: one combined string is enough for demo logs; chunk storage if volume matters.
	task.Logs += line + "\n"
	s.runningTasks[taskID] = task
	return nil
}

func (s *WorkerStatusStore) Get(ctx context.Context, taskID string) (*job.Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	task, exists := s.runningTasks[taskID]
	if !exists {
		return nil, ErrTaskNotFound
	}
	return &task, nil
}
