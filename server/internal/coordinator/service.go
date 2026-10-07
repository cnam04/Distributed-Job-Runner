package coordinator

import (
	"context"
	"distributed-job-runner/internal/job"
	"errors"
	"log"
	"strings"
	"time"
)

type RepoFetcher interface {
	GetRepo(ctx context.Context, link string) (string, error)
	CleanupRepo() error
}

type ImageBuilder interface {
	Build(ctx context.Context, repoPath, dockerfilePath, jobID string) (string, error)
	Push(ctx context.Context, imageRef string) error
}

type WorkerClient interface {
	Run(ctx context.Context, spec job.JobSpec) error
}

type JobStore interface {
	Create(ctx context.Context, j *job.Job) error
	UpdateStatus(ctx context.Context, jobID string, status job.Status) error
	SetImageRef(ctx context.Context, jobID, imageRef string) error
	AppendLog(ctx context.Context, jobID, line string) error
	Get(ctx context.Context, jobID string) (*job.Job, error)
}

type Service struct {
	NewRepo    func() RepoFetcher
	Images     ImageBuilder
	Workers    WorkerClient
	Jobs       JobStore
	RunContext context.Context
}

func (s *Service) create(ctx context.Context, j *job.Job) error {
	if strings.TrimSpace(j.ID) == "" || strings.TrimSpace(j.RepoURL) == "" || strings.TrimSpace(j.DockerfilePath) == "" {
		return errors.New("id, repoUrl, and dockerfilePath are required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	j.Status = job.StatusQueued
	if err := s.Jobs.Create(ctx, j); err != nil {
		return err
	}
	log.Printf("Job queued: id=%s", j.ID)
	return nil
}

func (s *Service) Submit(ctx context.Context, j *job.Job) error {
	if err := s.create(ctx, j); err != nil {
		return err
	}
	runCtx := s.RunContext
	if runCtx == nil {
		runCtx = context.Background()
	}
	backgroundJob := *j
	go func() {
		if err := s.execute(runCtx, &backgroundJob); err != nil {
			log.Printf("Job failed: id=%s error=%v", backgroundJob.ID, err)
		}
	}()
	return nil
}

func (s *Service) Run(ctx context.Context, j *job.Job) error {
	if err := s.create(ctx, j); err != nil {
		return err
	}
	return s.execute(ctx, j)
}

func (s *Service) execute(ctx context.Context, j *job.Job) (err error) {
	// Each job owns its source directory, even when requests run concurrently.
	repo := s.NewRepo()
	defer func() {
		err = errors.Join(err, repo.CleanupRepo())
		if err == nil {
			err = s.setStatus(ctx, j, job.StatusCompleted)
		}
		if err != nil {
			stateCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			err = errors.Join(err, s.setStatus(stateCtx, j, job.StatusFailed))
		}
	}()
	if err = s.setStatus(ctx, j, job.StatusCloning); err != nil {
		return err
	}
	repoPath, err := repo.GetRepo(ctx, j.RepoURL)
	if err != nil {
		return err
	}
	if err = s.setStatus(ctx, j, job.StatusBuilding); err != nil {
		return err
	}
	// ponytail: images are retained on hosts and GHCR; add retention when storage becomes a limit.
	j.ImageRef, err = s.Images.Build(ctx, repoPath, j.DockerfilePath, j.ID)
	if err != nil {
		return err
	}
	if err = s.Jobs.SetImageRef(ctx, j.ID, j.ImageRef); err != nil {
		return err
	}
	if err = s.Images.Push(ctx, j.ImageRef); err != nil {
		return err
	}
	if err = s.setStatus(ctx, j, job.StatusRunning); err != nil {
		return err
	}
	return s.Workers.Run(ctx, job.JobSpec{JobID: j.ID, ImageRef: j.ImageRef})
}

func (s *Service) setStatus(ctx context.Context, j *job.Job, status job.Status) error {
	j.Status = status
	return s.Jobs.UpdateStatus(ctx, j.ID, status)
}
