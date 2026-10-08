package coordinatorhttp

import (
	"crypto/rand"
	"distributed-job-runner/internal/coordinator"
	gitservice "distributed-job-runner/internal/infrastructure/github"
	"distributed-job-runner/internal/job"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
)

type handler struct {
	service *coordinator.Service
}

func NewHandler(service *coordinator.Service) http.Handler {
	handler := &handler{service: service}

	mux := http.NewServeMux()
	// job submit
	mux.HandleFunc("POST /jobs", handler.handleSubmitJob)

	// get job data
	mux.HandleFunc("GET /jobs/{id}", handler.handleGetJobById)
	return mux
}

func (handler *handler) handleGetJobById(w http.ResponseWriter, r *http.Request) {
	j, err := handler.service.Jobs.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, coordinator.ErrJobNotFound) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(j)
}

func (handler *handler) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var request struct {
		RepoURL        string `json:"repoUrl"`
		DockerfilePath string `json:"dockerfilePath"`
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil || json.Unmarshal(body, &request) != nil {
		http.Error(w, "invalid job JSON", http.StatusBadRequest)
		return
	}
	if _, _, err := gitservice.ParseGithubLink(request.RepoURL); err != nil {
		http.Error(w, "repoUrl must be a GitHub repository URL", http.StatusBadRequest)
		return
	}
	if !filepath.IsLocal(request.DockerfilePath) || strings.Contains(request.DockerfilePath, "\\") || strings.TrimSpace(request.DockerfilePath) == "" {
		http.Error(w, "dockerfilePath must be repository-relative", http.StatusBadRequest)
		return
	}
	j := job.Job{ID: rand.Text(), RepoURL: request.RepoURL, DockerfilePath: request.DockerfilePath}
	err = handler.service.Submit(r.Context(), &j)
	status := http.StatusCreated
	message := ""
	if err != nil {
		status, message = http.StatusInternalServerError, err.Error()
	}
	w.Header().Set("Location", "/jobs/"+j.ID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(struct {
		job.Job
		Error string `json:"error,omitempty"`
	}{Job: j, Error: message})
}
