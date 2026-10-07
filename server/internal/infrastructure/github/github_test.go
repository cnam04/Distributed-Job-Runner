package gitservice

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

func TestParseGithubLink(t *testing.T) {
	for _, link := range []string{"https://github.com/example/project", "https://github.com/example/project/", "https://github.com/example/project.git"} {
		user, repo, err := ParseGithubLink(link)
		if err != nil || user != "example" || repo != "project" {
			t.Fatalf("%s: got user=%q repo=%q err=%v", link, user, repo, err)
		}
	}
}

func TestParseGithubLinkRejectsOtherHosts(t *testing.T) {
	for _, link := range []string{
		"", "https://github.com", "https://github.com/example", "https://github.com/example/",
		"https://example.com/project", "https://example.com/github.com/example/project",
		"https://github.com.evil.test/example/project", "https://github.com@evil.test/example/project",
		"https://evil.test@github.com/example/project", "https://github.com:123/example/project",
		"github.com/example/project", "file://github.com/example/project", "https://github.com/example/..",
		"https://github.com/example/project/tree/main", "https://github.com/example/project?query=1",
		"https://github.com/example/project#fragment", "https://github.com/example/.git", "https://github.com/%zz/project",
	} {
		if _, _, err := ParseGithubLink(link); err == nil {
			t.Errorf("expected %q to be rejected", link)
		}
	}
}

func TestCleanupRepo(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "repo", "nested")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte("test"), 0o644); err != nil {
		t.Fatal(err)
	}
	job := JobRepo{DirPath: filepath.Dir(dir), RepoPath: dir}
	removed := job.DirPath
	if err := job.CleanupRepo(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(removed); !os.IsNotExist(err) {
		t.Fatalf("expected repository directory to be removed, got %v", err)
	}
	if job.DirPath != "" || job.RepoPath != "" {
		t.Fatal("cleanup retained repository paths")
	}
	if err := job.CleanupRepo(); err != nil {
		t.Fatalf("second cleanup failed: %v", err)
	}
}

func repoZip(t *testing.T, names ...string) []byte {
	t.Helper()
	var archive bytes.Buffer
	writer := zip.NewWriter(&archive)
	for _, name := range names {
		file, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if name[len(name)-1] != '/' {
			if _, err := io.WriteString(file, "test"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return archive.Bytes()
}

func TestUnzipRepoRejectsZipSlip(t *testing.T) {
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "repo.zip")
	if err := os.WriteFile(zipPath, repoZip(t, "../escaped.txt"), 0o600); err != nil {
		t.Fatal(err)
	}
	job := JobRepo{DirPath: filepath.Join(dir, "repo")}
	if err := job.unzipRepo(t.Context(), zipPath); err == nil {
		t.Fatal("expected zip-slip path to be rejected")
	}
	if _, err := os.Stat(filepath.Join(dir, "escaped.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected no file outside extraction directory, got %v", err)
	}
}

func TestUnzipRepoRejectsSymlinks(t *testing.T) {
	var archive bytes.Buffer
	writer := zip.NewWriter(&archive)
	header := &zip.FileHeader{Name: "project/link"}
	header.SetMode(os.ModeSymlink | 0o777)
	file, err := writer.CreateHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(file, "../target"); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "repo.zip")
	if err := os.WriteFile(zipPath, archive.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	repo := JobRepo{DirPath: dir}
	if err := repo.unzipRepo(t.Context(), zipPath); err == nil {
		t.Fatal("expected unsupported symlink to be rejected")
	}
	if _, err := os.Lstat(filepath.Join(dir, "project", "link")); !os.IsNotExist(err) {
		t.Fatalf("unsupported link was extracted: %v", err)
	}
}

func TestUnzipRepoRoot(t *testing.T) {
	for _, names := range [][]string{{"project/", "project/Dockerfile"}, {"project/Dockerfile", "project/src/main.go"}, {"Dockerfile", "src/main.go"}} {
		t.Run(names[0], func(t *testing.T) {
			dir := t.TempDir()
			zipPath := filepath.Join(dir, "repo.zip")
			if err := os.WriteFile(zipPath, repoZip(t, names...), 0o600); err != nil {
				t.Fatal(err)
			}
			job := JobRepo{DirPath: dir}
			if err := job.unzipRepo(t.Context(), zipPath); err != nil {
				t.Fatal(err)
			}
			want := filepath.Join(dir, "project")
			if names[0] == "Dockerfile" {
				want = dir
			}
			if job.RepoPath != want {
				t.Fatalf("root = %q, want %q", job.RepoPath, want)
			}
			if data, err := os.ReadFile(filepath.Join(job.RepoPath, "Dockerfile")); err != nil || string(data) != "test" {
				t.Fatalf("Dockerfile contents = %q, err = %v", data, err)
			}
			if _, err := os.Stat(zipPath); !os.IsNotExist(err) {
				t.Fatalf("archive retained: %v", err)
			}
		})
	}
}

func TestUnzipRepoCanceled(t *testing.T) {
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "repo.zip")
	if err := os.WriteFile(zipPath, repoZip(t, "project/file"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	job := JobRepo{DirPath: dir}
	if err := job.unzipRepo(ctx, zipPath); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "project")); !os.IsNotExist(err) {
		t.Fatalf("canceled extraction wrote files: %v", err)
	}
}

type localTransport struct {
	base   http.RoundTripper
	target *url.URL
}

func (tr localTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	if r.URL.Host == "api.github.com" {
		r.URL.Scheme, r.URL.Host = tr.target.Scheme, tr.target.Host
	}
	return tr.base.RoundTrip(r)
}

func routeGitHubTo(t *testing.T, server *httptest.Server) {
	t.Helper()
	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	previous := http.DefaultTransport
	http.DefaultTransport = localTransport{base: server.Client().Transport, target: target}
	t.Cleanup(func() { http.DefaultTransport = previous })
}

func TestGetRepo(t *testing.T) {
	archive := repoZip(t, "project/Dockerfile")
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/example/project/zipball":
			http.Redirect(w, r, server.URL+"/repo.zip", http.StatusFound)
		case "/repo.zip":
			w.Write(archive)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	routeGitHubTo(t, server)
	job := &JobRepo{}
	t.Cleanup(func() {
		if err := job.CleanupRepo(); err != nil {
			t.Error(err)
		}
	})
	root, err := job.GetRepo(t.Context(), "https://github.com/example/project")
	if err != nil {
		t.Fatal(err)
	}
	if root != job.RepoPath || root != filepath.Join(job.DirPath, "project") {
		t.Fatalf("returned root %q, paths %+v", root, job)
	}
}

func TestGetRepoCleansUpFailures(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusBadGateway} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			temp := t.TempDir()
			t.Setenv("TMPDIR", temp)
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/repos/example/project/zipball" {
					http.Redirect(w, r, server.URL+"/repo.zip", http.StatusFound)
					return
				}
				w.WriteHeader(status)
				io.WriteString(w, "not a zip")
			}))
			defer server.Close()
			routeGitHubTo(t, server)
			job := &JobRepo{}
			if root, err := job.GetRepo(t.Context(), "https://github.com/example/project"); err == nil || root != "" {
				t.Fatalf("failed fetch: root=%q err=%v", root, err)
			}
			entries, err := os.ReadDir(temp)
			if err != nil || len(entries) != 0 || job.DirPath != "" || job.RepoPath != "" {
				t.Fatalf("failed fetch retained temporary files: %v, %+v, %v", entries, job, err)
			}
		})
	}
}

func TestRepositoryRequestsCanceled(t *testing.T) {
	for _, archiveAPI := range []bool{true, false} {
		t.Run(map[bool]string{true: "archive API", false: "raw download"}[archiveAPI], func(t *testing.T) {
			started := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(started)
				<-r.Context().Done()
			}))
			defer server.Close()
			routeGitHubTo(t, server)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			job := &JobRepo{DirPath: t.TempDir()}
			result := make(chan error, 1)
			go func() {
				if archiveAPI {
					_, err := job.GetRepo(ctx, "https://github.com/example/project")
					result <- err
				} else {
					result <- job.downloadRepo(ctx, server.URL)
				}
			}()
			<-started
			cancel()
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatalf("expected cancellation, got %v", err)
			}
		})
	}
}
