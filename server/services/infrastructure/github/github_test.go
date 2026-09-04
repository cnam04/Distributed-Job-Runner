package gitservice

import (
	"archive/zip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-github/v90/github"
)

func TestParseGithubLink(t *testing.T) {
	user, repo, err := parseGithubLink("https://github.com/example/project")
	if err != nil || user != "example" || repo != "project" {
		t.Fatalf("got user=%q repo=%q err=%v", user, repo, err)
	}
}

func TestParseGithubLinkRejectsOtherHosts(t *testing.T) {
	if _, _, err := parseGithubLink("https://example.com/project"); err == nil {
		t.Fatal("expected an error")
	}
}

func TestCleanupRepo(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "repo", "nested")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	job := JobRepo{DirPath: filepath.Dir(dir)}
	if err := job.CleanupRepo(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(job.DirPath); !os.IsNotExist(err) {
		t.Fatalf("expected repository directory to be removed, got %v", err)
	}
	if err := job.CleanupRepo(); err != nil {
		t.Fatalf("second cleanup failed: %v", err)
	}
}

func TestUnzipRepoRejectsZipSlip(t *testing.T) {
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "repo.zip")
	archive, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(archive)
	file, err := writer.Create("../escaped.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("escaped")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}

	job := JobRepo{DirPath: filepath.Join(dir, "repo")}
	if err := job.unzipRepo(zipPath); err == nil {
		t.Fatal("expected zip-slip path to be rejected")
	}
	if _, err := os.Stat(filepath.Join(dir, "escaped.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected no file outside extraction directory, got %v", err)
	}
}

func TestRemoveImageRegistry(t *testing.T) {
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/user/packages/container/distributed-job-runner/versions":
			io.WriteString(w, `[{"id":42,"metadata":{"container":{"tags":["123"]}}}]`)
		case r.Method == http.MethodDelete && r.URL.Path == "/user/packages/container/distributed-job-runner/versions/42":
			deleted = true
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := github.NewClient(github.WithHTTPClient(server.Client()), github.WithURLs(&server.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := removeImageRegistry(context.Background(), client, "ghcr.io/cnam04/distributed-job-runner:123"); err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("expected matching package version to be deleted")
	}
}
