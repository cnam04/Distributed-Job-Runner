package workerhttp

import (
	"bytes"
	"context"
	"distributed-job-runner/internal/job"
	"distributed-job-runner/internal/worker"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func NewHandler(service *worker.Service) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs/run", func(w http.ResponseWriter, r *http.Request) {
		var spec job.JobSpec
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil || json.Unmarshal(body, &spec) != nil || strings.TrimSpace(spec.JobID) == "" || strings.TrimSpace(spec.ImageRef) == "" {
			http.Error(w, "jobId and imageRef are required in valid JSON", http.StatusBadRequest)
			return
		}
		if err := service.Run(r.Context(), spec); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

// ponytail: one worker and synchronous completion; add scheduling and async callbacks when jobs outlive HTTP requests.
type Client struct {
	URL  string
	HTTP *http.Client
}

func (c *Client) Run(ctx context.Context, spec job.JobSpec) error {
	body, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.URL, "/")+"/jobs/run", bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.HTTP.Do(request)
	if err != nil {
		return fmt.Errorf("dispatch job: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		message, err := io.ReadAll(io.LimitReader(response.Body, 4096))
		if err != nil {
			return fmt.Errorf("read worker response: %w", err)
		}
		return fmt.Errorf("worker returned %s: %s", response.Status, strings.TrimSpace(string(message)))
	}
	return nil
}
