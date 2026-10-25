package coordinator

import (
	"context"
	"distributed-job-runner/internal/job"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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
	NewRepo func() RepoFetcher
	Images  ImageBuilder
	// current worker pool refers to one singular worker rather than an array of workers
	Workers    []WorkerClient
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
	// Add the job to the memory store
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

	// immediately asynchronously execute the coordinator aspect of the job
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

	// set the status of the job in the store
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

	// Split job into tasks and run
	if err := s.ManageTasks(ctx, j); err != nil {
		return err
	}

	return nil
}

func (s *Service) setStatus(ctx context.Context, j *job.Job, status job.Status) error {
	j.Status = status
	return s.Jobs.UpdateStatus(ctx, j.ID, status)
}

// Split jobs into tasks, schedule workers, track execution, etc.
// main method for runtime task management per job.
func (s *Service) ManageTasks(ctx context.Context, j *job.Job) error {
	// TODO: Replace with real job->[]task logic
	// Create all of the tasks
	var tasks = []job.Task{}
	var ARBITRARY_NUMBER_OF_TASKS_REPLACE_LATER = 9
	for range ARBITRARY_NUMBER_OF_TASKS_REPLACE_LATER {
		tasks = append(tasks, s.CreateTask(ctx, j))
	}

	// Schedule tasks to Non-busy workers.
	// TODO: More efficient scheduling logic
	var errs []error
	currWorker := 0

	// Buffering it to the number of tasks ensures goroutines never get blocked writing to it
	errChan := make(chan error, len(tasks))

	// Create a buffered channel of workers
	availableWorkers := make(chan int, len(s.Workers))
	for i := range s.Workers {
		availableWorkers <- i
	}

	var wg sync.WaitGroup
	var workerIdx int
	for _, task := range tasks {
		// wait for a free worker if all workers are running a task (1 task per worker for now)
		select {
		case <-ctx.Done():
			// allow running tasks to finish
			wg.Wait()
			// Drain the errors from the channel back into your slice safely
			for err := range errChan {
				errs = append(errs, err)
			}
			// Add context error to errors
			errs = append(errs, ctx.Err())
			return errors.Join(errs...)
		case workerIdx = <-availableWorkers:
		}

		wg.Add(1)

		// Launch the goroutine
		// Pass task and currWorker as parameters to avoid loop variable capture bugs
		go func(t job.Task, workerIdx int) {
			defer wg.Done()
			// add the worker back to the pool when it's done with the current task
			defer func() {
				availableWorkers <- workerIdx
			}()

			// Run the task synchronously inside this specific goroutine
			err := s.Workers[workerIdx].Run(ctx, job.JobSpec{JobID: t.JobID, ImageRef: t.ImageRef})

			if err != nil {
				errChan <- err // Send the error to the channel
			} else {
				log.Printf("[Worker: %d] Job %s | Task %s Complete,", workerIdx, t.JobID, t.ID)
			}
		}(task, workerIdx) // <-- Variables are copied into the parameters safely here

		currWorker = (currWorker + 1) % len(s.Workers)
	}

	// Wait for all goroutines to finish in the background, then close the channel
	wg.Wait()
	close(errChan)
	close(availableWorkers)

	// Drain the errors from the channel back into your slice safely
	for err := range errChan {
		errs = append(errs, err)
	}

	return errors.Join(errs...)

}

// Create a task from job. For now, just creates a task that is a job. Will later divide the job into tasks
func (s *Service) CreateTask(ctx context.Context, j *job.Job) job.Task {
	return job.Task{
		ID:       uuid.New().String(),
		JobID:    j.ID,
		ImageRef: j.ImageRef,
		Status:   job.TaskPending,
		Logs:     "",
	}
}
