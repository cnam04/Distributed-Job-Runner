package worker

import (
	"bytes"
	"context"
	"distributed-job-runner/internal/job"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

var testSpec = job.JobSpec{JobID: "job-1", ImageRef: "ghcr.io/example/job:job-1"}

type fakeRunner struct {
	t        *testing.T
	mu       sync.Mutex
	calls    []string
	contexts map[string]context.Context
	onCall   func(context.Context, string) error
	logLine  string
}

func (r *fakeRunner) call(ctx context.Context, stage string) error {
	r.mu.Lock()
	r.calls = append(r.calls, stage)
	if r.contexts == nil {
		r.contexts = make(map[string]context.Context)
	}
	r.contexts[stage] = ctx
	r.mu.Unlock()
	if r.onCall != nil {
		return r.onCall(ctx, stage)
	}
	return nil
}

func (r *fakeRunner) Pull(ctx context.Context, imageRef string) error {
	if imageRef != testSpec.ImageRef {
		r.t.Errorf("pull image = %q, want %q", imageRef, testSpec.ImageRef)
	}
	return r.call(ctx, "pull")
}

func (r *fakeRunner) Create(ctx context.Context, imageRef string) (string, error) {
	if imageRef != testSpec.ImageRef {
		r.t.Errorf("create image = %q, want %q", imageRef, testSpec.ImageRef)
	}
	if err := r.call(ctx, "create"); err != nil {
		return "", err
	}
	return "container-1", nil
}

func (r *fakeRunner) containers(ctx context.Context, ids []string, stage string) error {
	if !slices.Equal(ids, []string{"container-1"}) {
		r.t.Errorf("%s container IDs = %v, want [container-1]", stage, ids)
	}
	return r.call(ctx, stage)
}

func (r *fakeRunner) Start(ctx context.Context, ids []string) error {
	return r.containers(ctx, ids, "start")
}

func (r *fakeRunner) Logs(ctx context.Context, containerID, jobID string, sink LogSink) error {
	if containerID != "container-1" || jobID != testSpec.JobID {
		r.t.Errorf("logs container/job = %q/%q", containerID, jobID)
	}
	if err := r.call(ctx, "logs"); err != nil {
		return err
	}
	line := r.logLine
	if line == "" {
		line = "container output"
	}
	return sink.Write(ctx, jobID, line)
}

func (r *fakeRunner) Wait(ctx context.Context, ids []string) error {
	return r.containers(ctx, ids, "wait")
}

func (r *fakeRunner) Remove(ctx context.Context, ids []string) error {
	return r.containers(ctx, ids, "remove")
}

type fakeSink struct {
	onWrite func(context.Context, string, string) error
}

func (s fakeSink) Write(ctx context.Context, jobID, line string) error {
	if s.onWrite != nil {
		return s.onWrite(ctx, jobID, line)
	}
	return ctx.Err()
}

func TestRunLifecycle(t *testing.T) {
	type contextKey struct{}
	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), contextKey{}, "request-1"), 5*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	logsStarted := make(chan struct{})
	waitFinished := make(chan struct{})
	logsFinished := make(chan struct{})
	runner := &fakeRunner{t: t}
	runner.onCall = func(callCtx context.Context, stage string) error {
		if callCtx.Value(contextKey{}) != "request-1" {
			t.Errorf("%s lost request context value", stage)
		}
		if callCtx.Err() != nil {
			t.Errorf("%s context canceled: %v", stage, callCtx.Err())
		}
		callDeadline, ok := callCtx.Deadline()
		if stage == "remove" {
			remaining := time.Until(callDeadline)
			if !ok || remaining <= 29*time.Second || remaining > 30*time.Second {
				t.Errorf("cleanup deadline remaining = %v, want 30 seconds", remaining)
			}
			select {
			case <-logsFinished:
			default:
				t.Error("removed container before logs finished")
			}
		} else if !ok || !callDeadline.Equal(deadline) {
			t.Errorf("%s did not preserve request deadline", stage)
		}
		if stage == "wait" {
			select {
			case <-logsStarted:
				close(waitFinished)
			case <-callCtx.Done():
				return callCtx.Err()
			}
		}
		return nil
	}
	sink := fakeSink{onWrite: func(logCtx context.Context, jobID, line string) error {
		if jobID != testSpec.JobID || line != "container output" {
			t.Errorf("sink received %q/%q", jobID, line)
		}
		close(logsStarted)
		select {
		case <-waitFinished:
		case <-logCtx.Done():
			return logCtx.Err()
		}
		if logCtx.Err() != nil {
			t.Error("successful wait canceled logs before they drained")
		}
		close(logsFinished)
		return nil
	}}
	service := Service{Runner: runner, Logs: sink}
	if err := service.Run(ctx, testSpec); err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) != 6 || !slices.Equal(runner.calls[:3], []string{"pull", "create", "start"}) || runner.calls[5] != "remove" {
		t.Fatalf("lifecycle calls = %v", runner.calls)
	}
	if !slices.Contains(runner.calls[3:5], "logs") || !slices.Contains(runner.calls[3:5], "wait") {
		t.Errorf("missing concurrent logs/wait calls: %v", runner.calls)
	}
	for _, stage := range []string{"pull", "create", "start"} {
		if runner.contexts[stage] != ctx {
			t.Errorf("%s did not receive caller context", stage)
		}
	}
	if runner.contexts["logs"] != runner.contexts["wait"] {
		t.Error("logs and wait did not share cancellation context")
	}
	if runner.contexts["remove"].Err() != context.Canceled {
		t.Error("cleanup context was not released")
	}
}

func TestRunStageFailures(t *testing.T) {
	for _, stage := range []string{"pull", "create", "start", "logs", "wait", "remove"} {
		t.Run(stage, func(t *testing.T) {
			primary := errors.New(stage + " failed")
			cleanup := errors.New("cleanup failed")
			runner := &fakeRunner{t: t, onCall: func(_ context.Context, called string) error {
				if called == stage {
					return primary
				}
				if called == "remove" {
					return cleanup
				}
				return nil
			}}
			service := Service{Runner: runner, Logs: fakeSink{}}
			err := service.Run(t.Context(), testSpec)
			if !errors.Is(err, primary) {
				t.Fatalf("Run error = %v, want primary failure %v", err, primary)
			}
			if stage == "pull" || stage == "create" {
				want := []string{"pull"}
				if stage == "create" {
					want = append(want, "create")
				}
				if !slices.Equal(runner.calls, want) {
					t.Errorf("calls after failure = %v, want %v", runner.calls, want)
				}
				return
			}
			if runner.calls[len(runner.calls)-1] != "remove" {
				t.Errorf("cleanup was not last: %v", runner.calls)
			}
			if stage != "remove" && !errors.Is(err, cleanup) {
				t.Errorf("Run did not join cleanup failure: %v", err)
			}
			if stage == "start" && !slices.Equal(runner.calls, []string{"pull", "create", "start", "remove"}) {
				t.Errorf("start failure continued running: %v", runner.calls)
			}
			if stage != "start" && len(runner.calls) != 6 {
				t.Errorf("calls after failure = %v, want full lifecycle", runner.calls)
			}
		})
	}
}

func TestRunValidatesSpec(t *testing.T) {
	for _, spec := range []job.JobSpec{
		{},
		{JobID: testSpec.JobID},
		{ImageRef: testSpec.ImageRef},
		{JobID: " \t", ImageRef: testSpec.ImageRef},
		{JobID: testSpec.JobID, ImageRef: " \n"},
	} {
		runner := &fakeRunner{t: t}
		service := Service{Runner: runner, Logs: fakeSink{}}
		if err := service.Run(t.Context(), spec); err == nil {
			t.Errorf("accepted invalid spec: %+v", spec)
		}
		if len(runner.calls) != 0 {
			t.Errorf("invalid spec called runner: %v", runner.calls)
		}
	}
}

func TestRunAlreadyCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	runner := &fakeRunner{t: t}
	service := Service{Runner: runner, Logs: fakeSink{}}
	if err := service.Run(ctx, testSpec); !errors.Is(err, context.Canceled) {
		t.Errorf("Run error = %v, want context.Canceled", err)
	}
	if len(runner.calls) != 0 {
		t.Errorf("canceled request called runner: %v", runner.calls)
	}
}

func TestRunExitErrorDrainsLogs(t *testing.T) {
	exitErr := &ExitError{ContainerID: "container-1", StatusCode: 17}
	if got := exitErr.Error(); got != "container container-1 exited with status 17" {
		t.Fatalf("exit error message = %q", got)
	}
	for _, test := range []struct {
		name    string
		waitErr error
	}{
		{name: "direct", waitErr: exitErr},
		{name: "wrapped", waitErr: fmt.Errorf("wait: %w", exitErr)},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				releaseLogs := make(chan struct{})
				logsFinished := make(chan struct{})
				const finalStderr = "Traceback: final stderr before container exit"
				var output []string
				runner := &fakeRunner{t: t, logLine: finalStderr, onCall: func(callCtx context.Context, stage string) error {
					switch stage {
					case "wait":
						return test.waitErr
					case "logs":
						<-releaseLogs
						return callCtx.Err()
					case "remove":
						select {
						case <-logsFinished:
						default:
							t.Error("removed container before final stderr drained")
						}
					}
					return nil
				}}
				sink := fakeSink{onWrite: func(logCtx context.Context, jobID, line string) error {
					defer close(logsFinished)
					if err := logCtx.Err(); err != nil {
						t.Errorf("final stderr context canceled: %v", err)
						return err
					}
					if jobID != testSpec.JobID {
						t.Errorf("final stderr job ID = %q", jobID)
					}
					output = append(output, line)
					return nil
				}}
				service := Service{Runner: runner, Logs: sink}
				result := make(chan error, 1)
				go func() { result <- service.Run(t.Context(), testSpec) }()
				synctest.Wait()
				// Run has handled the wait error and is now blocked joining logs.
				if runner.contexts["logs"].Err() != nil {
					t.Error("container exit canceled the log follower")
				}
				if len(output) != 0 || slices.Contains(runner.calls, "remove") {
					t.Error("logs or cleanup ran before final stderr was released")
				}
				close(releaseLogs)
				synctest.Wait()
				err := <-result
				var got *ExitError
				if !errors.Is(err, exitErr) || !errors.As(err, &got) || got != exitErr {
					t.Errorf("Run error = %v, want original exit error", err)
				}
				if errors.Is(err, context.Canceled) {
					t.Errorf("container exit added spurious cancellation: %v", err)
				}
				if !slices.Equal(output, []string{finalStderr}) || runner.calls[len(runner.calls)-1] != "remove" {
					t.Errorf("final stderr/cleanup = %v/%v", output, runner.calls)
				}
			})
		})
	}
}

func TestRunLogFailureCancelsWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	logErr := errors.New("sink failed")
	waitStarted := make(chan struct{})
	waitFinished := make(chan struct{})
	logsFinished := make(chan struct{})
	runner := &fakeRunner{t: t, onCall: func(callCtx context.Context, stage string) error {
		if stage == "wait" {
			close(waitStarted)
			<-callCtx.Done()
			if callCtx.Err() != context.Canceled {
				t.Errorf("log failure did not cancel wait: %v", callCtx.Err())
			}
			close(waitFinished)
			return callCtx.Err()
		}
		if stage == "remove" {
			for _, finished := range []chan struct{}{waitFinished, logsFinished} {
				select {
				case <-finished:
				default:
					t.Error("removed container before wait/logs finished")
				}
			}
		}
		return nil
	}}
	sink := fakeSink{onWrite: func(logCtx context.Context, _, _ string) error {
		defer close(logsFinished)
		select {
		case <-waitStarted:
			return logErr
		case <-logCtx.Done():
			return logCtx.Err()
		}
	}}
	service := Service{Runner: runner, Logs: sink}
	err := service.Run(ctx, testSpec)
	if !errors.Is(err, logErr) || !errors.Is(err, context.Canceled) {
		t.Errorf("Run error = %v, want sink failure and canceled wait", err)
	}
	if ctx.Err() != nil {
		t.Errorf("log failure canceled caller context: %v", ctx.Err())
	}
}

func TestRunCancellationJoinsLogsAndDetachesCleanup(t *testing.T) {
	for _, cause := range []string{"wait", "caller"} {
		t.Run(cause, func(t *testing.T) {
			type contextKey struct{}
			testCtx, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			ctx, cancel := context.WithCancel(context.WithValue(testCtx, contextKey{}, "request-2"))
			defer cancel()
			waitErr := errors.New("wait transport failed")
			logsStarted := make(chan struct{})
			waitStarted := make(chan struct{})
			logsCanceled := make(chan struct{})
			releaseLogs := make(chan struct{})
			logsFinished := make(chan struct{})
			removed := make(chan struct{})
			runner := &fakeRunner{t: t, onCall: func(callCtx context.Context, stage string) error {
				switch stage {
				case "logs":
					close(logsStarted)
					<-callCtx.Done()
					if callCtx.Err() != context.Canceled {
						t.Errorf("logs were not canceled promptly: %v", callCtx.Err())
					}
					close(logsCanceled)
					select {
					case <-releaseLogs:
					case <-testCtx.Done():
						t.Error("timed out joining logs")
					}
					close(logsFinished)
					return callCtx.Err()
				case "wait":
					close(waitStarted)
					select {
					case <-logsStarted:
					case <-callCtx.Done():
						return callCtx.Err()
					}
					if cause == "wait" {
						return waitErr
					}
					<-callCtx.Done()
					return callCtx.Err()
				case "remove":
					select {
					case <-logsFinished:
					default:
						t.Error("removed container before log goroutine finished")
					}
					if callCtx.Err() != nil || callCtx.Value(contextKey{}) != "request-2" {
						t.Errorf("cleanup did not detach cancellation/preserve values: %v", callCtx.Err())
					}
					deadline, ok := callCtx.Deadline()
					if remaining := time.Until(deadline); !ok || remaining <= 29*time.Second || remaining > 30*time.Second {
						t.Errorf("cleanup deadline remaining = %v, want 30 seconds", remaining)
					}
					close(removed)
				}
				return nil
			}}
			service := Service{Runner: runner, Logs: fakeSink{}}
			result := make(chan error, 1)
			go func() { result <- service.Run(ctx, testSpec) }()
			if cause == "caller" {
				select {
				case <-waitStarted:
				case <-ctx.Done():
					t.Fatal("wait did not start")
				}
				select {
				case <-logsStarted:
				case <-ctx.Done():
					t.Fatal("logs did not start")
				}
				cancel()
			}
			select {
			case <-logsCanceled:
			case <-testCtx.Done():
				t.Fatal("log follower was not canceled")
			}
			select {
			case <-removed:
				t.Error("cleanup did not wait for log goroutine to join")
			default:
			}
			close(releaseLogs)
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) || (cause == "wait" && !errors.Is(err, waitErr)) {
					t.Errorf("Run error = %v, want cancellation and primary failure", err)
				}
			case <-testCtx.Done():
				t.Fatal("Run did not finish after logs joined")
			}
			select {
			case <-removed:
			default:
				t.Error("container was not cleaned up")
			}
			if runner.contexts["remove"].Err() != context.Canceled {
				t.Error("cleanup timeout context was not released")
			}
		})
	}
}

func TestConsoleLogSink(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	sink := ConsoleLogSink{}
	if err := sink.Write(t.Context(), testSpec.JobID, "hello"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "[job-1] hello\n") {
		t.Errorf("console output = %q, want job-prefixed line", output.String())
	}
	before := output.String()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := sink.Write(ctx, testSpec.JobID, "not printed"); !errors.Is(err, context.Canceled) {
		t.Errorf("Write error = %v, want context.Canceled", err)
	}
	if output.String() != before {
		t.Error("canceled sink wrote a log line")
	}
}
