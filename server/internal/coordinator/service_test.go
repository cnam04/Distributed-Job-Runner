package coordinator

import (
	"context"
	"distributed-job-runner/internal/job"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func coordinatorJob(id string) job.Job {
	return job.Job{ID: id, RepoURL: "https://github.com/example/" + id, DockerfilePath: "docker/" + id + ".Dockerfile"}
}

func coordinatorImage(id string) string { return "ghcr.io/example/jobs:" + id }

type coordinatorFake struct {
	*MemoryJobStore
	t        *testing.T
	ctx      context.Context
	mu       sync.Mutex
	calls    []string
	repos    []*coordinatorRepoFake
	failures map[string]error
	before   func(context.Context, string)
}

func (f *coordinatorFake) call(ctx context.Context, stage string) error {
	f.mu.Lock()
	f.calls = append(f.calls, stage)
	f.mu.Unlock()
	if ctx != nil && stage != "failed" && ctx != f.ctx {
		f.t.Errorf("%s did not receive caller context", stage)
	}
	if f.before != nil {
		f.before(ctx, stage)
	}
	if err := f.failures[stage]; err != nil {
		return err
	}
	if ctx != nil {
		return ctx.Err()
	}
	return nil
}

func (f *coordinatorFake) service() *Service {
	return &Service{NewRepo: func() RepoFetcher {
		f.call(nil, "repo")
		r := &coordinatorRepoFake{fake: f}
		f.mu.Lock()
		f.repos = append(f.repos, r)
		f.mu.Unlock()
		return r
	}, Images: f, Workers: f, Jobs: f}
}

type coordinatorRepoFake struct {
	fake    *coordinatorFake
	path    string
	cleaned bool
}

func (r *coordinatorRepoFake) GetRepo(ctx context.Context, link string) (string, error) {
	id := strings.TrimPrefix(link, "https://github.com/example/")
	if link != coordinatorJob(id).RepoURL {
		r.fake.t.Errorf("fetch URL = %q", link)
	}
	r.path = "/repos/" + id
	return r.path, r.fake.call(ctx, "fetch")
}

func (r *coordinatorRepoFake) CleanupRepo() error {
	if r.cleaned {
		r.fake.t.Error("repository cleaned more than once")
	}
	r.cleaned = true
	return r.fake.call(nil, "cleanup")
}

func (f *coordinatorFake) Create(ctx context.Context, j *job.Job) error {
	if j.Status != job.StatusQueued {
		f.t.Errorf("created status = %q, want queued", j.Status)
	}
	if err := f.call(ctx, "queued"); err != nil {
		return err
	}
	return f.MemoryJobStore.Create(ctx, j)
}

func (f *coordinatorFake) UpdateStatus(ctx context.Context, id string, status job.Status) error {
	if err := f.call(ctx, string(status)); err != nil {
		return err
	}
	return f.MemoryJobStore.UpdateStatus(ctx, id, status)
}

func (f *coordinatorFake) Get(ctx context.Context, id string) (*job.Job, error) {
	if err := f.call(ctx, "get"); err != nil {
		return nil, err
	}
	return f.MemoryJobStore.Get(ctx, id)
}

func (f *coordinatorFake) SetImageRef(ctx context.Context, id, ref string) error {
	if ref != coordinatorImage(id) {
		f.t.Errorf("saved image = %q, want %q", ref, coordinatorImage(id))
	}
	if err := f.call(ctx, "imageRef"); err != nil {
		return err
	}
	return f.MemoryJobStore.SetImageRef(ctx, id, ref)
}

func (f *coordinatorFake) Build(ctx context.Context, repoPath, dockerfilePath, id string) (string, error) {
	if repoPath != "/repos/"+id || dockerfilePath != coordinatorJob(id).DockerfilePath {
		f.t.Errorf("build inputs = %q, %q, %q", repoPath, dockerfilePath, id)
	}
	if err := f.call(ctx, "build"); err != nil {
		return "", err
	}
	return coordinatorImage(id), nil
}

func (f *coordinatorFake) Push(ctx context.Context, ref string) error {
	id := strings.TrimPrefix(ref, "ghcr.io/example/jobs:")
	f.checkStored(id, ref, job.StatusBuilding)
	return f.call(ctx, "push")
}

func (f *coordinatorFake) Run(ctx context.Context, spec job.JobSpec) error {
	if spec != (job.JobSpec{JobID: spec.JobID, ImageRef: coordinatorImage(spec.JobID)}) {
		f.t.Errorf("dispatch spec = %+v", spec)
	}
	f.checkStored(spec.JobID, spec.ImageRef, job.StatusRunning)
	return f.call(ctx, "dispatch")
}

func (f *coordinatorFake) checkStored(id, ref string, status job.Status) {
	f.t.Helper()
	j, err := f.MemoryJobStore.Get(f.t.Context(), id)
	if err != nil || j.ImageRef != ref || j.Status != status {
		f.t.Errorf("stored job before image operation = %+v, %v; want %q, %q", j, err, ref, status)
	}
}

func TestServiceRunLifecycleAndFailures(t *testing.T) {
	steps := []string{"queued", "repo", "cloning", "fetch", "building", "build", "imageRef", "push", "running", "dispatch", "cleanup", "completed"}
	primary := errors.New("stage failed")
	cleanupErr := errors.New("cleanup failed")
	stateErr := errors.New("failed status could not be saved")
	for _, stage := range []string{"success", "queued", "cloning", "fetch", "building", "build", "imageRef", "push", "running", "dispatch", "cleanup", "completed", "failed", "joined failures"} {
		t.Run(stage, func(t *testing.T) {
			f := &coordinatorFake{MemoryJobStore: NewMemoryJobStore(), t: t, ctx: t.Context(), failures: make(map[string]error)}
			stop := stage
			switch stage {
			case "success":
			case "failed":
				stop = "dispatch"
				f.failures[stop], f.failures["failed"] = primary, stateErr
			case "joined failures":
				stop = "fetch"
				f.failures[stop], f.failures["cleanup"], f.failures["failed"] = primary, cleanupErr, stateErr
			default:
				f.failures[stop] = primary
			}
			j := coordinatorJob("job-1")
			err := f.service().Run(f.ctx, &j)
			want := slices.Clone(steps)
			if stage == "success" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				for _, failure := range f.failures {
					if !errors.Is(err, failure) {
						t.Errorf("Run error = %v, want joined failure %v", err, failure)
					}
				}
				want = want[:slices.Index(steps, stop)+1]
				if stop != "queued" {
					if !slices.Contains(want, "cleanup") {
						want = append(want, "cleanup")
					}
					want = append(want, "failed")
				}
			}
			if !slices.Equal(f.calls, want) {
				t.Errorf("calls = %v, want %v", f.calls, want)
			}
			stored, getErr := f.MemoryJobStore.Get(t.Context(), j.ID)
			if stop == "queued" {
				if !errors.Is(getErr, ErrJobNotFound) || len(f.repos) != 0 {
					t.Errorf("create failure left job/repository: %+v, %v, %d repos", stored, getErr, len(f.repos))
				}
				return
			}
			wantStatus := job.StatusFailed
			if stage == "success" {
				wantStatus = job.StatusCompleted
			}
			if j.Status != wantStatus || len(f.repos) != 1 || !f.repos[0].cleaned {
				t.Errorf("final job/repository = %+v, %+v", j, f.repos)
			}
			wantJob := coordinatorJob(j.ID)
			wantJob.Status = wantStatus
			if slices.Index(steps, stop) > slices.Index(steps, "imageRef") || stage == "success" {
				wantJob.ImageRef = coordinatorImage(j.ID)
			}
			if stage == "failed" {
				wantJob.Status = job.StatusRunning
			} else if stage == "joined failures" {
				wantJob.Status = job.StatusCloning
			}
			if getErr != nil || *stored != wantJob {
				t.Errorf("stored job = %+v, %v; want %+v", stored, getErr, wantJob)
			}
		})
	}
}

func TestServiceRunRejectsInvalidJobsAndCanceledContext(t *testing.T) {
	for _, field := range []string{"id", "repoUrl", "dockerfilePath", "canceled"} {
		for _, value := range []string{"", " \t\n"} {
			t.Run(field+"/"+value, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				j := coordinatorJob("job-1")
				switch field {
				case "id":
					j.ID = value
				case "repoUrl":
					j.RepoURL = value
				case "dockerfilePath":
					j.DockerfilePath = value
				case "canceled":
					cancel()
				}
				original := j
				f := &coordinatorFake{MemoryJobStore: NewMemoryJobStore(), t: t, ctx: ctx}
				err := f.service().Run(ctx, &j)
				if err == nil || (field == "canceled" && !errors.Is(err, context.Canceled)) {
					t.Errorf("Run error = %v", err)
				}
				if len(f.calls) != 0 || j != original {
					t.Errorf("rejected job performed operations or changed input: %v, %+v", f.calls, j)
				}
			})
		}
	}
}

func TestServiceRunCancellationRecordsFailure(t *testing.T) {
	for _, stage := range []string{"cloning", "fetch", "building", "build", "imageRef", "push", "running", "dispatch", "cleanup", "completed"} {
		t.Run(stage, func(t *testing.T) {
			type contextKey struct{}
			ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), contextKey{}, "request-1"), time.Minute)
			defer cancel()
			var stateCtx context.Context
			f := &coordinatorFake{MemoryJobStore: NewMemoryJobStore(), t: t, ctx: ctx}
			f.before = func(callCtx context.Context, called string) {
				if called == stage {
					cancel()
				}
				if called == "failed" {
					stateCtx = callCtx
					deadline, ok := callCtx.Deadline()
					if callCtx == ctx || callCtx.Err() != nil || callCtx.Value(contextKey{}) != "request-1" || !ok || !deadline.After(time.Now()) {
						t.Errorf("failed status did not use a live, bounded, value-preserving detached context")
					}
				}
			}
			j := coordinatorJob("job-1")
			if err := f.service().Run(ctx, &j); !errors.Is(err, context.Canceled) {
				t.Errorf("Run error = %v, want cancellation", err)
			}
			stored, err := f.MemoryJobStore.Get(t.Context(), j.ID)
			if err != nil || stored.Status != job.StatusFailed || j.Status != job.StatusFailed {
				t.Errorf("canceled job = %+v, %v; input = %+v", stored, err, j)
			}
			if stateCtx == nil || stateCtx.Err() != context.Canceled {
				t.Error("failure context was not released")
			}
			if len(f.repos) != 1 || !f.repos[0].cleaned || f.calls[len(f.calls)-1] != "failed" {
				t.Errorf("canceled job did not clean up and record failure: %v", f.calls)
			}
			if slices.Contains([]string{"cloning", "fetch", "building", "build", "imageRef", "push", "running"}, stage) && slices.Contains(f.calls, "dispatch") {
				t.Errorf("dispatched after cancellation at %s: %v", stage, f.calls)
			}
		})
	}
}

func TestServiceRunConcurrentRepositoryIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	fetched := make(chan struct{}, 2)
	release := make(chan struct{})
	f := &coordinatorFake{MemoryJobStore: NewMemoryJobStore(), t: t, ctx: ctx}
	f.before = func(callCtx context.Context, stage string) {
		if stage == "fetch" {
			fetched <- struct{}{}
			select {
			case <-release:
			case <-callCtx.Done():
			}
		}
	}
	s := f.service()
	results := make(chan error, 2)
	for _, id := range []string{"job-1", "job-2"} {
		go func() {
			j := coordinatorJob(id)
			results <- s.Run(ctx, &j)
		}()
	}
	for range 2 {
		select {
		case <-fetched:
		case <-ctx.Done():
			t.Fatal("concurrent repository fetches did not start")
		}
	}
	close(release)
	for range 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
			t.Fatal("concurrent jobs did not finish")
		}
	}
	if len(f.repos) != 2 || f.repos[0] == f.repos[1] || f.repos[0].path == f.repos[1].path || !f.repos[0].cleaned || !f.repos[1].cleaned {
		t.Errorf("jobs shared a repository or skipped cleanup: %+v", f.repos)
	}
	for _, id := range []string{"job-1", "job-2"} {
		want := coordinatorJob(id)
		want.Status, want.ImageRef = job.StatusCompleted, coordinatorImage(id)
		got, err := f.MemoryJobStore.Get(t.Context(), id)
		if err != nil || *got != want {
			t.Errorf("job %s = %+v, %v; want %+v", id, got, err, want)
		}
	}
}
