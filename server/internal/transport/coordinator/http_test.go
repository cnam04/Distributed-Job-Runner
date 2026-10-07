package coordinatorhttp_test

import (
	"bytes"
	"context"
	"distributed-job-runner/internal/coordinator"
	"distributed-job-runner/internal/job"
	coordinatorhttp "distributed-job-runner/internal/transport/coordinator"
	workerhttp "distributed-job-runner/internal/transport/worker"
	"distributed-job-runner/internal/worker"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

const validJobJSON = `{"repoUrl":"https://github.com/example/project","dockerfilePath":"Dockerfile"}`

type fakeRepo struct {
	get     func(context.Context, string) (string, error)
	cleanup func() error
}

func (r fakeRepo) GetRepo(ctx context.Context, link string) (string, error) {
	return r.get(ctx, link)
}

func (r fakeRepo) CleanupRepo() error { return r.cleanup() }

type fakeBuilder struct {
	build func(context.Context, string, string, string) (string, error)
	push  func(context.Context, string) error
}

func (b fakeBuilder) Build(ctx context.Context, repoPath, dockerfilePath, jobID string) (string, error) {
	return b.build(ctx, repoPath, dockerfilePath, jobID)
}

func (b fakeBuilder) Push(ctx context.Context, imageRef string) error {
	return b.push(ctx, imageRef)
}

type fakeRunner struct {
	t        *testing.T
	imageRef string
	onCall   func(context.Context, string) error
}

func (r fakeRunner) Pull(ctx context.Context, imageRef string) error {
	if imageRef != r.imageRef {
		r.t.Errorf("pull image = %q, want %q", imageRef, r.imageRef)
	}
	return r.onCall(ctx, "pull")
}

func (r fakeRunner) Create(ctx context.Context, imageRef string) (string, error) {
	if imageRef != r.imageRef {
		r.t.Errorf("create image = %q, want %q", imageRef, r.imageRef)
	}
	return "worker-container", r.onCall(ctx, "create")
}

func (r fakeRunner) containers(ctx context.Context, ids []string, stage string) error {
	if !slices.Equal(ids, []string{"worker-container"}) {
		r.t.Errorf("%s container IDs = %v, want [worker-container]", stage, ids)
	}
	return r.onCall(ctx, stage)
}

func (r fakeRunner) Start(ctx context.Context, ids []string) error {
	return r.containers(ctx, ids, "start")
}

func (r fakeRunner) Logs(ctx context.Context, containerID, jobID string, sink worker.LogSink) error {
	if containerID != "worker-container" {
		r.t.Errorf("logs container = %q, want worker-container", containerID)
	}
	if err := r.onCall(ctx, "logs"); err != nil {
		return err
	}
	return sink.Write(ctx, jobID, "container output")
}

func (r fakeRunner) Wait(ctx context.Context, ids []string) error {
	return r.containers(ctx, ids, "wait")
}

func (r fakeRunner) Remove(ctx context.Context, ids []string) error {
	return r.containers(ctx, ids, "remove")
}

type sinkFunc func(context.Context, string, string) error

func (f sinkFunc) Write(ctx context.Context, jobID, line string) error {
	return f(ctx, jobID, line)
}

func TestHTTPJobLifecycle(t *testing.T) {
	for _, test := range []struct {
		name       string
		workerErr  error
		wantStatus job.Status
	}{
		{"completed", nil, job.StatusCompleted},
		{"worker failure", errors.New("container exited with status 17"), job.StatusFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			type contextKey struct{}
			checkContext := func(ctx context.Context, want string) {
				if ctx.Value(contextKey{}) != want || ctx.Err() != nil {
					t.Errorf("%s request context not propagated: value=%v err=%v", want, ctx.Value(contextKey{}), ctx.Err())
				}
			}
			const imageRef = "ghcr.io/example/project:built-image"
			calls := make(chan string, 32)
			buildIDs := make(chan string, 32)
			dispatches := make(chan job.JobSpec, 32)
			logEntries := make(chan [2]string, 32)
			logsWritten := make(chan struct{})
			repo := fakeRepo{
				get: func(ctx context.Context, link string) (string, error) {
					checkContext(ctx, "coordinator")
					calls <- "fetch"
					if link != "https://github.com/example/project" {
						t.Errorf("repo URL = %q", link)
					}
					return "/fake/repo", ctx.Err()
				},
				cleanup: func() error {
					calls <- "cleanup"
					return nil
				},
			}
			builder := fakeBuilder{
				build: func(ctx context.Context, repoPath, dockerfilePath, jobID string) (string, error) {
					checkContext(ctx, "coordinator")
					calls <- "build"
					buildIDs <- jobID
					if repoPath != "/fake/repo" || dockerfilePath != "Dockerfile" || jobID == "" {
						t.Errorf("build arguments = %q/%q/%q", repoPath, dockerfilePath, jobID)
					}
					return imageRef, ctx.Err()
				},
				push: func(ctx context.Context, ref string) error {
					checkContext(ctx, "coordinator")
					calls <- "push"
					if ref != imageRef {
						t.Errorf("push image = %q, want %q", ref, imageRef)
					}
					return ctx.Err()
				},
			}
			runner := fakeRunner{t: t, imageRef: imageRef, onCall: func(ctx context.Context, stage string) error {
				checkContext(ctx, "worker")
				calls <- stage
				if stage == "wait" {
					select {
					case <-logsWritten:
						return test.workerErr
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return ctx.Err()
			}}
			sink := sinkFunc(func(ctx context.Context, jobID, line string) error {
				checkContext(ctx, "worker")
				logEntries <- [2]string{jobID, line}
				close(logsWritten)
				return ctx.Err()
			})
			store := coordinator.NewMemoryJobStore()
			workerHandler := workerhttp.NewHandler(&worker.Service{Runner: runner, Logs: sink})
			workerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.RequestURI() != "/jobs/run" || r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("worker request = %s %s, content type %q", r.Method, r.URL, r.Header.Get("Content-Type"))
				}
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				var wireSpec map[string]string
				if err := json.Unmarshal(body, &wireSpec); err != nil || len(wireSpec) != 2 || wireSpec["jobId"] == "" || wireSpec["imageRef"] != imageRef {
					t.Errorf("worker must receive only jobId/imageRef: %s, err=%v", body, err)
				}
				spec := job.JobSpec{JobID: wireSpec["jobId"], ImageRef: wireSpec["imageRef"]}
				dispatches <- spec
				stored, err := store.Get(r.Context(), spec.JobID)
				if err != nil || stored.Status != job.StatusRunning || stored.ImageRef != imageRef {
					t.Errorf("job at dispatch = %+v, err=%v", stored, err)
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
				workerHandler.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), contextKey{}, "worker")))
			}))
			defer workerServer.Close()
			service := &coordinator.Service{
				NewRepo:    func() coordinator.RepoFetcher { return repo },
				Images:     builder,
				Workers:    &workerhttp.Client{URL: workerServer.URL, HTTP: workerServer.Client(), Log: store.AppendLog},
				Jobs:       store,
				RunContext: context.WithValue(ctx, contextKey{}, "coordinator"),
			}
			handler := coordinatorhttp.NewHandler(service)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handler.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), contextKey{}, "coordinator")))
			}))
			defer server.Close()
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/jobs", strings.NewReader(validJobJSON+"\n"))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusCreated || response.Header.Get("Content-Type") != "application/json" {
				t.Fatalf("POST response = %s, headers=%v, body=%s", response.Status, response.Header, body)
			}
			var created struct {
				job.Job
				Error string `json:"error"`
			}
			if err := json.Unmarshal(body, &created); err != nil {
				t.Fatal(err)
			}
			if created.ID == "" || created.RepoURL != "https://github.com/example/project" || created.DockerfilePath != "Dockerfile" || created.ImageRef != "" || created.Status != job.StatusQueued {
				t.Fatalf("created job = %+v", created)
			}
			if created.Error != "" {
				t.Errorf("queued job error = %q", created.Error)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(body, &fields); err != nil {
				t.Fatal(err)
			}
			for key := range fields {
				if !slices.Contains([]string{"id", "repoUrl", "dockerfilePath", "imageRef", "status", "logs", "error"}, key) {
					t.Errorf("coordinator exposed worker container details: field %q", key)
				}
			}
			location := response.Header.Get("Location")
			if location != "/jobs/"+created.ID {
				t.Fatalf("Location = %q, want /jobs/%s", location, created.ID)
			}
			var fetched job.Job
			for {
				request, err = http.NewRequestWithContext(ctx, http.MethodGet, server.URL+location, nil)
				if err != nil {
					t.Fatal(err)
				}
				response, err = server.Client().Do(request)
				if err != nil {
					t.Fatal(err)
				}
				if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "application/json" {
					response.Body.Close()
					t.Fatalf("GET response = %s, headers=%v", response.Status, response.Header)
				}
				err = json.NewDecoder(response.Body).Decode(&fetched)
				response.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				if fetched.Status == job.StatusCompleted || fetched.Status == job.StatusFailed {
					break
				}
				select {
				case <-time.After(10 * time.Millisecond):
				case <-ctx.Done():
					t.Fatal("job did not reach a terminal status")
				}
			}
			stored, err := store.Get(ctx, created.ID)
			if err != nil || fetched.ID != created.ID || fetched.RepoURL != created.RepoURL || fetched.DockerfilePath != created.DockerfilePath || fetched.ImageRef != imageRef || fetched.Status != test.wantStatus || *stored != fetched {
				t.Fatalf("completed GET/store disagree: created=%+v fetched=%+v stored=%+v err=%v", created.Job, fetched, stored, err)
			}
			logSuffix := fmt.Sprintf(" [%s] container output", created.ID)
			logLine := strings.TrimSuffix(fetched.Logs, "\n")
			if !strings.HasSuffix(logLine, logSuffix) {
				t.Errorf("stored log = %q, want timestamp followed by %q", fetched.Logs, logSuffix)
			} else if _, err := time.Parse("2006/01/02 15:04:05", strings.TrimSuffix(logLine, logSuffix)); err != nil {
				t.Errorf("stored log timestamp = %q: %v", fetched.Logs, err)
			}
			if len(buildIDs) != 1 || len(dispatches) != 1 || len(logEntries) != 1 {
				t.Fatalf("build/dispatch/log counts = %d/%d/%d, want 1/1/1", len(buildIDs), len(dispatches), len(logEntries))
			}
			if got := <-buildIDs; got != created.ID {
				t.Errorf("build job ID = %q, want %q", got, created.ID)
			}
			if got := <-dispatches; got != (job.JobSpec{JobID: created.ID, ImageRef: imageRef}) {
				t.Errorf("dispatched spec = %+v", got)
			}
			if got := <-logEntries; got != ([2]string{created.ID, "container output"}) {
				t.Errorf("log sink entry = %q", got)
			}
			var gotCalls []string
			for len(calls) > 0 {
				gotCalls = append(gotCalls, <-calls)
			}
			wantCalls := []string{"fetch", "build", "push", "pull", "create", "start", "logs", "wait", "remove", "cleanup"}
			sorted := slices.Clone(gotCalls)
			slices.Sort(sorted)
			slices.Sort(wantCalls)
			if !slices.Equal(sorted, wantCalls) {
				t.Fatalf("lifecycle calls = %v, want each operation exactly once", gotCalls)
			}
			if !slices.Equal(gotCalls[:6], []string{"fetch", "build", "push", "pull", "create", "start"}) || !slices.Equal(gotCalls[8:], []string{"remove", "cleanup"}) {
				t.Errorf("lifecycle order = %v", gotCalls)
			}
		})
	}
}

type failingStore struct {
	coordinator.JobStore
	err     error
	creates int
	gets    []string
}

func (s *failingStore) Create(context.Context, *job.Job) error {
	s.creates++
	return s.err
}

func (s *failingStore) Get(_ context.Context, id string) (*job.Job, error) {
	s.gets = append(s.gets, id)
	return nil, s.err
}

func TestPostJobsValidation(t *testing.T) {
	requestJSON := func(repoURL, dockerfilePath string) string {
		body, err := json.Marshal(map[string]string{"repoUrl": repoURL, "dockerfilePath": dockerfilePath})
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}
	const repoURL = "https://github.com/example/project"
	const invalidJSON = "invalid job JSON"
	const invalidURL = "repoUrl must be a GitHub repository URL"
	const invalidPath = "dockerfilePath must be repository-relative"
	for _, test := range []struct{ name, body, message string }{
		{"empty body", "", invalidJSON},
		{"malformed JSON", "{", invalidJSON},
		{"trailing JSON", validJobJSON + `{}`, invalidJSON},
		{"trailing garbage", validJobJSON + ` garbage`, invalidJSON},
		{"oversized JSON", validJobJSON + strings.Repeat(" ", 1<<20), invalidJSON},
		{"wrong field type", `{"repoUrl":42,"dockerfilePath":"Dockerfile"}`, invalidJSON},
		{"array", `[]`, invalidJSON},
		{"null", `null`, invalidURL},
		{"missing fields", `{}`, invalidURL},
		{"missing repoUrl", `{"dockerfilePath":"Dockerfile"}`, invalidURL},
		{"missing dockerfilePath", `{"repoUrl":"https://github.com/example/project"}`, invalidPath},
		{"blank repoUrl", requestJSON(" \t", "Dockerfile"), invalidURL},
		{"blank dockerfilePath", requestJSON(repoURL, " \t"), invalidPath},
		{"absolute Dockerfile", requestJSON(repoURL, "/tmp/Dockerfile"), invalidPath},
		{"parent traversal", requestJSON(repoURL, "../Dockerfile"), invalidPath},
		{"nested traversal", requestJSON(repoURL, "build/../../Dockerfile"), invalidPath},
		{"Windows absolute", requestJSON(repoURL, `C:\repo\Dockerfile`), invalidPath},
		{"Windows separators", requestJSON(repoURL, `build\Dockerfile`), invalidPath},
		{"Windows traversal", requestJSON(repoURL, `..\Dockerfile`), invalidPath},
		{"relative URL", requestJSON("github.com/example/project", "Dockerfile"), invalidURL},
		{"malformed URL", requestJSON("https://github.com/%zz/project", "Dockerfile"), invalidURL},
		{"wrong scheme", requestJSON("file://github.com/example/project", "Dockerfile"), invalidURL},
		{"wrong host", requestJSON("https://example.com/example/project", "Dockerfile"), invalidURL},
		{"host suffix", requestJSON("https://github.com.evil.test/example/project", "Dockerfile"), invalidURL},
		{"URL credentials", requestJSON("https://user@github.com/example/project", "Dockerfile"), invalidURL},
		{"URL port", requestJSON("https://github.com:123/example/project", "Dockerfile"), invalidURL},
		{"missing repository path", requestJSON("https://github.com", "Dockerfile"), invalidURL},
		{"missing repository name", requestJSON("https://github.com/example", "Dockerfile"), invalidURL},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &failingStore{err: errors.New("invalid request reached the service")}
			handler := coordinatorhttp.NewHandler(&coordinator.Service{Jobs: store})
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(test.body)))
			if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), test.message) {
				t.Errorf("response = %d %q, want 400 containing %q", response.Code, response.Body.String(), test.message)
			}
			if store.creates != 0 || len(store.gets) != 0 || response.Header().Get("Location") != "" {
				t.Errorf("invalid request reached service: creates=%d gets=%v Location=%q", store.creates, store.gets, response.Header().Get("Location"))
			}
		})
	}
}

func TestCoordinatorMethods(t *testing.T) {
	for _, test := range []struct{ method, path, allow string }{
		{http.MethodGet, "/jobs", "POST"},
		{http.MethodPut, "/jobs", "POST"},
		{http.MethodDelete, "/jobs", "POST"},
		{http.MethodOptions, "/jobs", "POST"},
		{http.MethodPost, "/jobs/existing", "GET, HEAD"},
		{http.MethodPut, "/jobs/existing", "GET, HEAD"},
		{http.MethodDelete, "/jobs/existing", "GET, HEAD"},
	} {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			store := &failingStore{err: errors.New("unsupported method reached service")}
			response := httptest.NewRecorder()
			coordinatorhttp.NewHandler(&coordinator.Service{Jobs: store}).ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
			if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != test.allow {
				t.Errorf("response = %d, Allow=%q, want 405, Allow=%q", response.Code, response.Header().Get("Allow"), test.allow)
			}
			if store.creates != 0 || len(store.gets) != 0 {
				t.Errorf("unsupported method reached service: creates=%d gets=%v", store.creates, store.gets)
			}
		})
	}
}

func TestGetJobErrors(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		status int
	}{
		{"not found", coordinator.ErrJobNotFound, http.StatusNotFound},
		{"wrapped not found", fmt.Errorf("lookup: %w", coordinator.ErrJobNotFound), http.StatusNotFound},
		{"store failure", errors.New("store unavailable"), http.StatusInternalServerError},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &failingStore{err: test.err}
			response := httptest.NewRecorder()
			coordinatorhttp.NewHandler(&coordinator.Service{Jobs: store}).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/jobs/missing-job", nil))
			if response.Code != test.status || response.Body.String() != test.err.Error()+"\n" {
				t.Errorf("response = %d %q, want %d %q", response.Code, response.Body.String(), test.status, test.err.Error()+"\n")
			}
			if !slices.Equal(store.gets, []string{"missing-job"}) || store.creates != 0 {
				t.Errorf("store calls: gets=%v creates=%d", store.gets, store.creates)
			}
		})
	}
}

func TestPostJobServiceError(t *testing.T) {
	store := &failingStore{err: errors.New("create job failed")}
	response := httptest.NewRecorder()
	coordinatorhttp.NewHandler(&coordinator.Service{Jobs: store}).ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(validJobJSON)))
	if response.Code != http.StatusInternalServerError || response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("response = %d, headers=%v, body=%s", response.Code, response.Header(), response.Body.String())
	}
	var body struct {
		job.Job
		Error string `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if store.creates != 1 || body.ID == "" || body.Error != store.err.Error() || response.Header().Get("Location") != "/jobs/"+body.ID {
		t.Errorf("service failure response = %+v, creates=%d Location=%q", body, store.creates, response.Header().Get("Location"))
	}
}
