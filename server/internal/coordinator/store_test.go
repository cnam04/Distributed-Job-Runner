package coordinator

import (
	"context"
	"distributed-job-runner/internal/job"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMemoryJobStoreLifecycleAndCopies(t *testing.T) {
	s := NewMemoryJobStore()
	ctx := t.Context()
	want := coordinatorJob("job-1")
	want.Status = job.StatusQueued
	input := want
	if err := s.Create(ctx, &input); err != nil {
		t.Fatal(err)
	}
	input.RepoURL, input.Status, input.ImageRef = "changed", job.StatusFailed, "changed"
	got, err := s.Get(ctx, want.ID)
	if err != nil || *got != want {
		t.Fatalf("Create did not copy input: %+v, %v; want %+v", got, err, want)
	}
	got.DockerfilePath, got.Status, got.ImageRef = "changed", job.StatusFailed, "changed"
	duplicate := want
	duplicate.RepoURL = "replacement"
	if err := s.Create(ctx, &duplicate); err == nil {
		t.Error("duplicate job accepted")
	}
	again, err := s.Get(ctx, want.ID)
	if err != nil || *again != want || again == got {
		t.Fatalf("Get or duplicate Create changed stored job: %+v, %v; want %+v", again, err, want)
	}
	snapshot := *again
	if err := s.SetImageRef(ctx, want.ID, coordinatorImage(want.ID)); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateStatus(ctx, want.ID, job.StatusRunning); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendLog(ctx, want.ID, "first line"); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendLog(ctx, want.ID, ""); err != nil {
		t.Fatal(err)
	}
	want.ImageRef, want.Status, want.Logs = coordinatorImage(want.ID), job.StatusRunning, "first line\n\n"
	updated, err := s.Get(ctx, want.ID)
	if err != nil || *updated != want {
		t.Errorf("updated job = %+v, %v; want %+v", updated, err, want)
	}
	if *again != snapshot {
		t.Error("store update changed an earlier Get snapshot")
	}
}

func TestMemoryJobStoreMissingAndCanceled(t *testing.T) {
	s := NewMemoryJobStore()
	want := coordinatorJob("job-1")
	want.Status = job.StatusQueued
	if err := s.Create(t.Context(), &want); err != nil {
		t.Fatal(err)
	}
	ops := []struct {
		name string
		run  func(context.Context, string) error
	}{
		{"create", func(ctx context.Context, id string) error {
			j := coordinatorJob(id)
			return s.Create(ctx, &j)
		}},
		{"get", func(ctx context.Context, id string) error {
			got, err := s.Get(ctx, id)
			if err != nil && got != nil {
				t.Errorf("Get returned a job on error: %+v", got)
			}
			return err
		}},
		{"status", func(ctx context.Context, id string) error { return s.UpdateStatus(ctx, id, job.StatusFailed) }},
		{"imageRef", func(ctx context.Context, id string) error { return s.SetImageRef(ctx, id, "changed") }},
		{"log", func(ctx context.Context, id string) error { return s.AppendLog(ctx, id, "changed") }},
	}
	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			if op.name != "create" {
				if err := op.run(t.Context(), "missing"); !errors.Is(err, ErrJobNotFound) {
					t.Errorf("missing job error = %v, want ErrJobNotFound", err)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			for _, id := range []string{want.ID, "missing"} {
				if err := op.run(ctx, id); !errors.Is(err, context.Canceled) {
					t.Errorf("canceled operation on %s = %v, want context.Canceled", id, err)
				}
			}
		})
	}
	got, err := s.Get(t.Context(), want.ID)
	if err != nil || *got != want {
		t.Errorf("canceled operations changed job: %+v, %v; want %+v", got, err, want)
	}
	if got, err := s.Get(t.Context(), "missing"); got != nil || !errors.Is(err, ErrJobNotFound) {
		t.Errorf("canceled Create inserted job: %+v, %v", got, err)
	}
}

func TestMemoryJobStoreConcurrentAccess(t *testing.T) {
	s := NewMemoryJobStore()
	ctx := t.Context()
	shared := coordinatorJob("shared")
	if err := s.Create(ctx, &shared); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var duplicateWins atomic.Int32
	for i := range 16 {
		wg.Go(func() {
			<-start
			duplicate := coordinatorJob("duplicate")
			if err := s.Create(ctx, &duplicate); err == nil {
				duplicateWins.Add(1)
			}
			j := coordinatorJob(fmt.Sprintf("job-%d", i))
			if err := s.Create(ctx, &j); err != nil {
				t.Error(err)
				return
			}
			for range 20 {
				for _, id := range []string{j.ID, shared.ID} {
					if err := s.UpdateStatus(ctx, id, job.StatusRunning); err != nil {
						t.Error(err)
					}
					if err := s.SetImageRef(ctx, id, coordinatorImage(id)); err != nil {
						t.Error(err)
					}
					got, err := s.Get(ctx, id)
					if err != nil {
						t.Error(err)
						return
					}
					got.Status, got.ImageRef = job.StatusFailed, "local mutation"
				}
			}
		})
	}
	close(start)
	wg.Wait()
	if duplicateWins.Load() != 1 {
		t.Errorf("concurrent duplicate Create successes = %d, want 1", duplicateWins.Load())
	}
	for i := range 17 {
		id := "shared"
		if i < 16 {
			id = fmt.Sprintf("job-%d", i)
		}
		want := coordinatorJob(id)
		want.Status, want.ImageRef = job.StatusRunning, coordinatorImage(id)
		got, err := s.Get(ctx, id)
		if err != nil || *got != want {
			t.Errorf("concurrently updated job = %+v, %v; want %+v", got, err, want)
		}
	}
}
