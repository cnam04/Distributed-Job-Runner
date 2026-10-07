package dockerservice

import (
	"archive/tar"
	"context"
	"distributed-job-runner/internal/worker"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/moby/moby/client"
)

func testSDKClient(t *testing.T, handler http.HandlerFunc) *client.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = strings.TrimPrefix(r.URL.Path, "/v1.55")
		w.Header().Set("Content-Type", "application/json")
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	api, err := client.New(client.WithHost(server.URL), client.WithHTTPClient(server.Client()), client.WithAPIVersion("1.55"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { api.Close() })
	return api
}

type sinkFunc func(context.Context, string, string) error

func (f sinkFunc) Write(ctx context.Context, jobID, line string) error {
	return f(ctx, jobID, line)
}

func logFrame(stream byte, line string) []byte {
	frame := make([]byte, 8+len(line))
	frame[0] = stream
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(line)))
	copy(frame[8:], line)
	return frame
}

func TestRunnerCreateStart(t *testing.T) {
	calls := make(chan string, 3)
	api := testSDKClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls <- r.Method + " " + r.URL.Path
		if r.URL.Path == "/containers/create" {
			name := r.URL.Query().Get("name")
			if !strings.HasPrefix(name, "distributed-job-runner-") || name == "distributed-job-runner-" || name != strings.ToLower(name) {
				t.Errorf("invalid container name: %q", name)
			}
			var request struct{ Image string }
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Image != "example:job" {
				t.Errorf("create image = %q, err = %v", request.Image, err)
			}
			w.WriteHeader(http.StatusCreated)
			io.WriteString(w, `{"Id":"created"}`)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	runner := &Runner{ApiClient: api}
	id, err := runner.Create(t.Context(), "example:job")
	if err != nil || id != "created" {
		t.Fatalf("created id=%q err=%v", id, err)
	}
	if err := runner.Start(t.Context(), []string{id, "second"}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"POST /containers/create", "POST /containers/created/start", "POST /containers/second/start"} {
		if got := <-calls; got != want {
			t.Fatalf("call = %q, want %q", got, want)
		}
	}
}

func TestRunnerCreatePreCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if id, err := (&Runner{}).Create(ctx, "example:job"); id != "" || !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled create: id=%q err=%v", id, err)
	}
}

func TestRunnerCreateCanceledCleansUpByName(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	createdName := make(chan string, 1)
	var live atomic.Bool
	api := testSDKClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/containers/create":
			io.Copy(io.Discard, r.Body)
			createdName <- r.URL.Query().Get("name")
			live.Store(true)
			w.WriteHeader(http.StatusCreated)
			w.(http.Flusher).Flush()
			close(started)
			<-r.Context().Done()
		case r.Method == http.MethodDelete:
			name := <-createdName
			if name == "" || r.URL.Path != "/containers/"+name || r.URL.Query().Get("force") != "1" {
				t.Errorf("incorrect cleanup request: %s, created name=%q", r.URL, name)
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if !errors.Is(ctx.Err(), context.Canceled) || r.Context().Err() != nil {
				t.Errorf("cleanup did not use a detached context: caller=%v cleanup=%v", ctx.Err(), r.Context().Err())
			}
			live.Store(false)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	})
	runner := &Runner{ApiClient: api}
	result := make(chan error, 1)
	go func() {
		id, err := runner.Create(ctx, "example:job")
		if id != "" {
			t.Errorf("canceled create returned ID %q", id)
		}
		result <- err
	}()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("creation error was lost: %v", err)
	}
	if live.Load() {
		t.Fatal("canceled create leaked a daemon container")
	}
}

func TestRunnerCreateFailureCleanupErrors(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			createdName := make(chan string, 1)
			api := testSDKClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost && r.URL.Path == "/containers/create" {
					createdName <- r.URL.Query().Get("name")
					w.WriteHeader(http.StatusBadRequest)
					io.WriteString(w, `{"message":"create failed"}`)
					return
				}
				name := <-createdName
				if name == "" || r.Method != http.MethodDelete || r.URL.Path != "/containers/"+name || r.URL.Query().Get("force") != "1" {
					t.Errorf("incorrect cleanup request: %s %s, created name=%q", r.Method, r.URL, name)
				}
				w.WriteHeader(status)
				io.WriteString(w, `{"message":"cleanup failed"}`)
			})
			id, err := (&Runner{ApiClient: api}).Create(t.Context(), "example:job")
			if id != "" || err == nil || !strings.Contains(err.Error(), "create failed") {
				t.Fatalf("creation error was lost: id=%q err=%v", id, err)
			}
			if got, want := strings.Contains(err.Error(), "cleanup failed"), status != http.StatusNotFound; got != want {
				t.Fatalf("cleanup error included=%v, want %v: %v", got, want, err)
			}
			if len(createdName) != 0 {
				t.Fatal("failed create did not attempt cleanup")
			}
		})
	}
}

func TestRunnerLogs(t *testing.T) {
	api := testSDKClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/containers/container/logs" || r.URL.Query().Get("stdout") != "1" || r.URL.Query().Get("stderr") != "1" || r.URL.Query().Get("follow") != "1" {
			t.Errorf("incorrect log request: %s", r.URL)
		}
		w.Header().Set("Content-Type", "application/vnd.docker.raw-stream")
		w.Write(logFrame(1, "hello "))
		w.Write(logFrame(2, "stderr\n"))
		w.Write(logFrame(1, "world\nsecond\n"))
		w.Write(logFrame(2, "last line"))
	})
	runner := &Runner{ApiClient: api}
	var lines []string
	ctx := t.Context()
	sink := sinkFunc(func(got context.Context, jobID, line string) error {
		if got != ctx || jobID != "job" {
			t.Errorf("sink context/job ID not propagated: %q", jobID)
		}
		lines = append(lines, line)
		return nil
	})
	if err := runner.Logs(ctx, "container", "job", sink); err != nil {
		t.Fatal(err)
	}
	slices.Sort(lines)
	if want := []string{"hello world", "last line", "second", "stderr"}; !slices.Equal(lines, want) {
		t.Fatalf("lines = %q, want %q", lines, want)
	}
}

func TestRunnerLogsSinkError(t *testing.T) {
	closed := make(chan struct{})
	api := testSDKClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write(logFrame(1, "one\ntwo\n"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(closed)
	})
	runner := &Runner{ApiClient: api}
	failure := errors.New("sink failed")
	calls := 0
	err := runner.Logs(t.Context(), "container", "job", sinkFunc(func(context.Context, string, string) error {
		calls++
		return failure
	}))
	if !errors.Is(err, failure) || calls != 1 {
		t.Fatalf("sink error = %v, writes = %d", err, calls)
	}
	<-closed
}

func TestRunnerLogsDaemonError(t *testing.T) {
	api := testSDKClient(t, func(w http.ResponseWriter, r *http.Request) { w.Write(logFrame(3, "daemon failure")) })
	runner := &Runner{ApiClient: api}
	err := runner.Logs(t.Context(), "container", "job", sinkFunc(func(context.Context, string, string) error { return nil }))
	if err == nil || !strings.Contains(err.Error(), "daemon failure") {
		t.Fatalf("expected demultiplexer error, got %v", err)
	}
}

func TestRunnerWaitJoinsFailures(t *testing.T) {
	calls := make(chan string, 4)
	api := testSDKClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls <- r.URL.Path
		switch r.URL.Path {
		case "/containers/first/wait":
			io.WriteString(w, `{"StatusCode":2}`)
		case "/containers/second/wait":
			io.WriteString(w, `{"StatusCode":3,"Error":{"Message":"execution failed"}}`)
		case "/containers/third/wait":
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, `{"message":"wait failed"}`)
		case "/containers/fourth/wait":
			io.WriteString(w, `{"StatusCode":0}`)
		default:
			http.NotFound(w, r)
		}
	})
	runner := &Runner{ApiClient: api}
	err := runner.Wait(t.Context(), []string{"first", "second", "third", "fourth"})
	if err == nil {
		t.Fatal("expected joined wait errors")
	}
	var exitErr *worker.ExitError
	if !errors.As(err, &exitErr) || exitErr.ContainerID != "first" || exitErr.StatusCode != 2 {
		t.Fatalf("expected typed exit error for first container with status 2, got %+v: %v", exitErr, err)
	}
	for _, text := range []string{"first exited with status 2", "second exited with status 3", "execution failed", "third", "wait failed"} {
		if !strings.Contains(err.Error(), text) {
			t.Errorf("error %q missing %q", err, text)
		}
	}
	if len(calls) != 4 {
		t.Fatalf("waited for %d containers, want 4", len(calls))
	}
}

func TestRunnerWaitSuccess(t *testing.T) {
	api := testSDKClient(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"StatusCode":0}`) })
	if err := (&Runner{ApiClient: api}).Wait(t.Context(), []string{"first", "second"}); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRemoveTriesEveryContainer(t *testing.T) {
	calls := make(chan string, 3)
	api := testSDKClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls <- r.URL.Path
		if r.Method != http.MethodDelete || r.URL.Query().Get("force") != "1" {
			t.Errorf("incorrect remove request: %s %s", r.Method, r.URL)
		}
		if r.URL.Path != "/containers/third" {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, `{"message":"remove failed"}`)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	runner := &Runner{ApiClient: api}
	err := runner.Remove(t.Context(), []string{"first", "second", "third"})
	if err == nil || !strings.Contains(err.Error(), "first") || !strings.Contains(err.Error(), "second") {
		t.Fatalf("expected both removal errors, got %v", err)
	}
	for _, want := range []string{"/containers/first", "/containers/second", "/containers/third"} {
		if got := <-calls; got != want {
			t.Fatalf("remove call = %q, want %q", got, want)
		}
	}
	if err := runner.Remove(t.Context(), nil); err != nil {
		t.Fatalf("empty cleanup failed: %v", err)
	}
}

func TestBuilderBuildResponses(t *testing.T) {
	for _, test := range []struct {
		name, body, wantError string
		status                int
	}{
		{"success", `{"stream":"built"}`, "", http.StatusOK},
		{"build error", `{"errorDetail":{"message":"invalid Dockerfile"}}`, "invalid Dockerfile", http.StatusOK},
		{"malformed JSON", "not JSON", "read build response", http.StatusOK},
		{"API error", `{"message":"build failed"}`, "build failed", http.StatusInternalServerError},
	} {
		t.Run(test.name, func(t *testing.T) {
			api := testSDKClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/build" || r.URL.Query().Get("t") != ghcrRepository+":job-123" || r.URL.Query().Get("dockerfile") != "Dockerfile" {
					t.Errorf("incorrect build request: %s %s", r.Method, r.URL)
				}
				archive := tar.NewReader(r.Body)
				found := false
				for {
					header, err := archive.Next()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Error(err)
						break
					}
					if header.Name == "Dockerfile" {
						found = true
					}
				}
				if !found {
					t.Error("build context lacks Dockerfile")
				}
				w.WriteHeader(test.status)
				io.WriteString(w, test.body)
			})
			builder := &Builder{ApiClient: api}
			ref, err := builder.Build(t.Context(), dockerFixture(t, "FROM scratch\nCOPY marker /marker\n"), "Dockerfile", "job-123")
			if test.wantError == "" {
				if err != nil || ref != ghcrRepository+":job-123" {
					t.Fatalf("ref=%q err=%v", ref, err)
				}
			} else if err == nil || ref != "" || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("expected %q without image reference, got ref=%q err=%v", test.wantError, ref, err)
			}
		})
	}
}

func TestBuilderValidatesPathsAndTags(t *testing.T) {
	repo := dockerFixture(t, "FROM scratch\n")
	outside := filepath.Join(t.TempDir(), "Dockerfile")
	if err := os.WriteFile(outside, []byte("FROM scratch\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(repo, "escape")); err != nil {
		t.Fatal(err)
	}
	builder := &Builder{}
	for _, path := range []string{"", ".", "../Dockerfile", outside, "escape", "missing", `..\Dockerfile`} {
		if ref, err := builder.Build(t.Context(), repo, path, "job"); err == nil || ref != "" {
			t.Errorf("expected path %q to be rejected: ref=%q err=%v", path, ref, err)
		}
	}
	for _, tag := range []string{"", ".job", "-job", "job/image", "job:tag", "job with spaces", strings.Repeat("a", 129)} {
		if ref, err := builder.Build(t.Context(), repo, "Dockerfile", tag); err == nil || ref != "" {
			t.Errorf("expected job tag %q to be rejected: ref=%q err=%v", tag, ref, err)
		}
	}
}

func TestDockerOperationsCanceled(t *testing.T) {
	t.Setenv(ghcrWriteTokenEnv, "write-secret")
	t.Setenv(ghcrReadTokenEnv, "read-secret")
	for _, operation := range []string{"build", "push", "pull", "create", "start", "logs", "wait", "remove", "remove image"} {
		for _, stream := range []bool{false, true} {
			if stream && (operation == "start" || operation == "remove") {
				continue
			}
			t.Run(operation+map[bool]string{false: "/request", true: "/stream"}[stream], func(t *testing.T) {
				started := make(chan struct{})
				api := testSDKClient(t, func(w http.ResponseWriter, r *http.Request) {
					if operation == "create" && r.Method == http.MethodDelete {
						w.WriteHeader(http.StatusNoContent)
						return
					}
					io.Copy(io.Discard, r.Body)
					if stream {
						w.WriteHeader(http.StatusOK)
						w.(http.Flusher).Flush()
					}
					close(started)
					<-r.Context().Done()
				})
				builder, runner := &Builder{ApiClient: api}, &Runner{ApiClient: api}
				repo := dockerFixture(t, "FROM scratch\nCOPY marker /marker\n")
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				result := make(chan error, 1)
				go func() {
					var err error
					switch operation {
					case "build":
						_, err = builder.Build(ctx, repo, "Dockerfile", "job")
					case "push":
						err = builder.Push(ctx, ghcrRepository+":job")
					case "pull":
						err = runner.Pull(ctx, ghcrRepository+":job")
					case "create":
						_, err = runner.Create(ctx, "example:job")
					case "start":
						err = runner.Start(ctx, []string{"container"})
					case "logs":
						err = runner.Logs(ctx, "container", "job", sinkFunc(func(context.Context, string, string) error { return nil }))
					case "wait":
						err = runner.Wait(ctx, []string{"container"})
					case "remove":
						err = runner.Remove(ctx, []string{"container"})
					case "remove image":
						err = builder.RemoveLocalImage(ctx, "example:job")
					}
					result <- err
				}()
				<-started
				cancel()
				if err := <-result; !errors.Is(err, context.Canceled) {
					t.Fatalf("expected cancellation, got %v", err)
				}
			})
		}
	}
}

func TestPushPullStreamErrors(t *testing.T) {
	t.Setenv(ghcrWriteTokenEnv, "write-secret")
	t.Setenv(ghcrReadTokenEnv, "read-secret")
	api := testSDKClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Registry-Auth") == "" {
			t.Error("missing registry credentials")
		}
		io.WriteString(w, `{"errorDetail":{"message":"registry failed"}}`)
	})
	for _, err := range []error{(&Builder{ApiClient: api}).Push(t.Context(), ghcrRepository+":job"), (&Runner{ApiClient: api}).Pull(t.Context(), ghcrRepository+":job")} {
		if err == nil || !strings.Contains(err.Error(), "registry failed") {
			t.Fatalf("expected registry stream error, got %v", err)
		}
	}
}
