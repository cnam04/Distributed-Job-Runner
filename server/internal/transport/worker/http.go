package workerhttp

import (
	"bytes"
	"context"
	"distributed-job-runner/internal/job"
	"distributed-job-runner/internal/worker"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// satisfies the LogSink interface for the worker logs
type logSinkFunc func(context.Context, string, string) error

func (f logSinkFunc) Write(ctx context.Context, jobID, line string) error {
	return f(ctx, jobID, line)
}

func NewHandler(service *worker.Service) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs/run", func(w http.ResponseWriter, r *http.Request) {
		var spec job.JobSpec
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil || json.Unmarshal(body, &spec) != nil || strings.TrimSpace(spec.JobID) == "" || strings.TrimSpace(spec.ImageRef) == "" {
			http.Error(w, "jobId and imageRef are required in valid JSON", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		if flusher != nil {
			flusher.Flush()
		}
		encoder := json.NewEncoder(w)
		runService := *service
		// implementation of the worker logSink. It prints logs to stdout AND streams them to Coordinator
		runService.Logs = logSinkFunc(func(ctx context.Context, jobID, line string) error {
			if service.Logs != nil {
				// use the ConsoleLogSink Write implementation
				if err := service.Logs.Write(ctx, jobID, line); err != nil {
					return err
				}
			}
			// write to http response body
			lineCopy := fmt.Sprintf("%s [%s] %s", time.Now().Format("2006/01/02 15:04:05"), jobID, line)
			if err := encoder.Encode(job.LogEvent{Line: &lineCopy}); err != nil {
				return err
			}
			// send through the connection
			if flusher != nil {
				flusher.Flush()
			}
			return nil
		})
		event := job.LogEvent{Done: true}

		// run call the worker service run method for the current job
		if err := runService.Run(r.Context(), spec); err != nil {
			event = job.LogEvent{Error: err.Error()}
		}
		if encoder.Encode(event) == nil && flusher != nil {
			flusher.Flush()
		}
	})
	return mux
}

// Client
type Client struct {
	URL  string
	HTTP *http.Client
	Log  func(context.Context, string, string) error
}

func (c *Client) Run(ctx context.Context, spec job.JobSpec) error {
	// build jobspec into api req format
	body, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimRight(c.URL, "/")+"/jobs/run",
		bytes.NewReader(body),
	)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")

	// synchronously send api request to the worker
	response, err := c.HTTP.Do(request)
	if err != nil {
		return fmt.Errorf("dispatch job: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		message, err := io.ReadAll(io.LimitReader(response.Body, 4096))
		if err != nil {
			return fmt.Errorf("read worker response: %w", err)
		}
		return fmt.Errorf("worker returned %s: %s", response.Status, strings.TrimSpace(string(message)))
	}

	// read responses through the post response untl job is finished
	decoder := json.NewDecoder(response.Body)
	for {
		var event job.LogEvent
		if err := decoder.Decode(&event); err != nil {
			if errors.Is(err, io.EOF) {
				return errors.New("worker stream ended before completion")
			}
			return fmt.Errorf("read worker response: %w", err)
		}
		if event.Line != nil && c.Log != nil {
			// log the response
			err := c.Log(ctx, spec.JobID, *event.Line)
			if err != nil {
				return fmt.Errorf("store worker log: %w", err)
			}
		}
		if event.Error != "" {
			return errors.New(event.Error)
		}
		if event.Done {
			return nil
		}
	}
}
