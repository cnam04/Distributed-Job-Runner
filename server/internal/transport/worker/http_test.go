package workerhttp

import (
	"context"
	"distributed-job-runner/internal/job"
	"distributed-job-runner/internal/worker"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

var testSpec = job.JobSpec{JobID: "job-1", ImageRef: "ghcr.io/example/job:job-1"}

const validSpecJSON = `{"jobId":"job-1","imageRef":"ghcr.io/example/job:job-1"}`

type fakeRunner struct {
	mu      sync.Mutex
	calls   []string
	failure error
}

func (r *fakeRunner) call(stage string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, stage)
	return r.failure
}

func (r *fakeRunner) Pull(context.Context, string) error { return r.call("pull") }

func (r *fakeRunner) Create(context.Context, string) (string, error) {
	return "container", r.call("create")
}

func (r *fakeRunner) Start(context.Context, []string) error { return r.call("start") }

func (r *fakeRunner) Logs(context.Context, string, string, worker.LogSink) error {
	return r.call("logs")
}

func (r *fakeRunner) Wait(context.Context, []string) error { return r.call("wait") }

func (r *fakeRunner) Remove(context.Context, []string) error { return r.call("remove") }

func TestWorkerValidation(t *testing.T) {
	for _, test := range []struct{ name, body string }{
		{"empty body", ""},
		{"malformed JSON", "{"},
		{"trailing JSON", validSpecJSON + `{}`},
		{"trailing garbage", validSpecJSON + ` garbage`},
		{"oversized JSON", validSpecJSON + strings.Repeat(" ", 1<<20)},
		{"array", `[]`},
		{"null", `null`},
		{"missing fields", `{}`},
		{"missing jobId", `{"imageRef":"ghcr.io/example/job:job-1"}`},
		{"missing imageRef", `{"jobId":"job-1"}`},
		{"blank jobId", `{"jobId":" \t\n","imageRef":"image:job"}`},
		{"blank imageRef", `{"jobId":"job-1","imageRef":" \t\n"}`},
		{"null jobId", `{"jobId":null,"imageRef":"image:job"}`},
		{"null imageRef", `{"jobId":"job-1","imageRef":null}`},
		{"wrong field type", `{"jobId":42,"imageRef":"image:job"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := &fakeRunner{failure: errors.New("invalid spec reached runner")}
			response := httptest.NewRecorder()
			NewHandler(&worker.Service{Runner: runner}).ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/jobs/run", strings.NewReader(test.body)))
			if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "jobId and imageRef are required in valid JSON") {
				t.Errorf("response = %d %q, want validation error 400", response.Code, response.Body.String())
			}
			if len(runner.calls) != 0 {
				t.Errorf("invalid spec called runner: %v", runner.calls)
			}
		})
	}
}

func TestWorkerMethods(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete, http.MethodOptions} {
		t.Run(method, func(t *testing.T) {
			runner := &fakeRunner{failure: errors.New("unsupported method reached runner")}
			response := httptest.NewRecorder()
			NewHandler(&worker.Service{Runner: runner}).ServeHTTP(response, httptest.NewRequest(method, "/jobs/run", strings.NewReader(validSpecJSON)))
			if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != "POST" {
				t.Errorf("response = %d, Allow=%q, want 405, Allow=POST", response.Code, response.Header().Get("Allow"))
			}
			if len(runner.calls) != 0 {
				t.Errorf("unsupported method called runner: %v", runner.calls)
			}
		})
	}
}

func TestWorkerServiceError(t *testing.T) {
	runner := &fakeRunner{failure: errors.New("pull denied")}
	response := httptest.NewRecorder()
	NewHandler(&worker.Service{Runner: runner}).ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/jobs/run", strings.NewReader(validSpecJSON)))
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/x-ndjson" || response.Body.String() != "{\"error\":\"pull denied\"}\n" {
		t.Errorf("response = %d %q, want streamed service error", response.Code, response.Body.String())
	}
	if !slices.Equal(runner.calls, []string{"pull"}) {
		t.Errorf("calls after service failure = %v, want [pull]", runner.calls)
	}
}

func TestClientResponses(t *testing.T) {
	for _, test := range []struct {
		name    string
		status  int
		body    string
		message string
	}{
		{"completed", http.StatusOK, "{\"done\":true}\n", ""},
		{"stream ended", http.StatusOK, "{\"line\":\"partial\"}\n", "worker stream ended before completion"},
		{"streamed failure", http.StatusOK, "{\"error\":\"container exited with status 17\"}\n", "container exited with status 17"},
		{"created is not completion", http.StatusCreated, "created", "created"},
		{"accepted is not completion", http.StatusAccepted, "accepted", "accepted"},
		{"bad request", http.StatusBadRequest, "invalid spec", "invalid spec"},
		{"not found", http.StatusNotFound, "worker endpoint missing", "worker endpoint missing"},
		{"worker failed", http.StatusInternalServerError, " \ncontainer exited with status 17\n ", "container exited with status 17"},
		{"unavailable", http.StatusServiceUnavailable, "worker unavailable", "worker unavailable"},
		{"bounded error body", http.StatusBadGateway, strings.Repeat("x", 4096) + "ignored suffix", strings.Repeat("x", 4096)},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.RequestURI() != "/jobs/run" || r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("request = %s %s, content type %q", r.Method, r.URL, r.Header.Get("Content-Type"))
				}
				body, err := io.ReadAll(r.Body)
				if err != nil || string(body) != validSpecJSON {
					t.Errorf("request spec = %s, err=%v, want %s", body, err, validSpecJSON)
				}
				w.WriteHeader(test.status)
				io.WriteString(w, test.body)
			}))
			defer server.Close()
			client := Client{URL: server.URL + "///", HTTP: server.Client()}
			err := client.Run(t.Context(), testSpec)
			if test.message == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if test.status == http.StatusOK {
				if err == nil || err.Error() != test.message {
					t.Errorf("Run error = %v, want %q", err, test.message)
				}
				return
			}
			want := fmt.Sprintf("worker returned %d %s: %s", test.status, http.StatusText(test.status), test.message)
			if err == nil || err.Error() != want {
				t.Errorf("Run error = %v, want %q", err, want)
			}
		})
	}
}

func TestClientStreamsLogs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "{\"line\":\"first\"}\n{\"line\":\"\"}\n{\"done\":true}\n")
	}))
	defer server.Close()
	var logs []string
	client := Client{URL: server.URL, HTTP: server.Client(), Log: func(_ context.Context, jobID, line string) error {
		if jobID != testSpec.JobID {
			t.Errorf("log job ID = %q, want %q", jobID, testSpec.JobID)
		}
		logs = append(logs, line)
		return nil
	}}
	if err := client.Run(t.Context(), testSpec); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(logs, []string{"first", ""}) {
		t.Errorf("streamed logs = %q", logs)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type notifyingBody struct {
	io.ReadCloser
	onRead func()
}

func (b notifyingBody) Read(p []byte) (int, error) {
	b.onRead()
	return b.ReadCloser.Read(p)
}

func TestClientCancellation(t *testing.T) {
	for _, responseStarted := range []bool{false, true} {
		name := "waiting for headers"
		if responseStarted {
			name = "active response"
		}
		t.Run(name, func(t *testing.T) {
			testCtx, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			started := make(chan struct{})
			bodyRead := make(chan struct{})
			serverCanceled := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if responseStarted {
					w.WriteHeader(http.StatusInternalServerError)
					io.WriteString(w, "partial failure message")
					w.(http.Flusher).Flush()
				}
				close(started)
				select {
				case <-r.Context().Done():
					close(serverCanceled)
				case <-testCtx.Done():
					t.Error("server request did not observe cancellation")
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(testCtx)
			defer cancel()
			httpClient := server.Client()
			if responseStarted {
				base := httpClient.Transport
				httpClient = &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
					response, err := base.RoundTrip(r)
					if err == nil {
						response.Body = notifyingBody{ReadCloser: response.Body, onRead: sync.OnceFunc(func() { close(bodyRead) })}
					}
					return response, err
				})}
			}
			client := Client{URL: server.URL, HTTP: httpClient}
			result := make(chan error, 1)
			go func() { result <- client.Run(ctx, testSpec) }()
			select {
			case <-started:
			case <-testCtx.Done():
				t.Fatal("worker request did not start")
			}
			if responseStarted {
				select {
				case <-bodyRead:
				case <-testCtx.Done():
					t.Fatal("client did not start reading the active response")
				}
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("Run error = %v, want context.Canceled", err)
				}
				if responseStarted && (err == nil || !strings.HasPrefix(err.Error(), "read worker response: ")) {
					t.Errorf("Run error = %v, want cancellation during response body read", err)
				}
			case <-testCtx.Done():
				t.Fatal("client did not stop after cancellation")
			}
			select {
			case <-serverCanceled:
			case <-testCtx.Done():
				t.Fatal("client cancellation did not cancel the worker request")
			}
		})
	}
}

func TestClientInvalidURL(t *testing.T) {
	client := Client{URL: "http://[::1", HTTP: &http.Client{}}
	if err := client.Run(t.Context(), testSpec); err == nil || !strings.Contains(err.Error(), "missing ']' in host") {
		t.Errorf("Run error = %v, want invalid URL error", err)
	}
}
