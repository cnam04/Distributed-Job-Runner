package worker

import (
	"context"
	"distributed-job-runner/internal/job"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

type ContainerRunner interface {
	Pull(ctx context.Context, imageRef string) error
	Create(ctx context.Context, imageRef string) (string, error)
	Start(ctx context.Context, containerIDs []string) error
	Logs(ctx context.Context, containerID, jobID string, sink LogSink) error
	Wait(ctx context.Context, containerIDs []string) error
	Remove(ctx context.Context, containerIDs []string) error
}

type ExitError struct {
	ContainerID string
	StatusCode  int64
}

func (e *ExitError) Error() string {
	return fmt.Sprintf("container %s exited with status %d", e.ContainerID, e.StatusCode)
}

type LogSink interface {
	Write(ctx context.Context, jobID, line string) error
}

type ConsoleLogSink struct{}

func (ConsoleLogSink) Write(ctx context.Context, jobID, line string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	log.Printf("[%s] %s", jobID, line)
	return nil
}

type Service struct {
	Runner ContainerRunner
	Logs   LogSink
}

func (s *Service) Run(ctx context.Context, spec job.JobSpec) (err error) {
	if strings.TrimSpace(spec.JobID) == "" || strings.TrimSpace(spec.ImageRef) == "" {
		return errors.New("jobId and imageRef are required")
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = s.Runner.Pull(ctx, spec.ImageRef); err != nil {
		return err
	}
	containerID, err := s.Runner.Create(ctx, spec.ImageRef)
	if err != nil {
		return err
	}

	// ponytail: one container per job; add chunk assignments when dataset scheduling exists.
	containerIDs := []string{containerID}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		err = errors.Join(err, s.Runner.Remove(cleanupCtx, containerIDs))
	}()
	if err = s.Runner.Start(ctx, containerIDs); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	logErrors := make(chan error, 1)
	// async logging using logsink function
	go func() {
		logErr := s.Runner.Logs(runCtx, containerID, spec.JobID, s.Logs)
		if logErr != nil {
			cancel()
		}
		logErrors <- logErr
	}()
	waitErr := s.Runner.Wait(runCtx, containerIDs)
	var exitErr *ExitError
	// An observed nonzero exit still leaves final logs to drain.
	if waitErr != nil && !errors.As(waitErr, &exitErr) {
		cancel()
	}
	return errors.Join(waitErr, <-logErrors)
}
