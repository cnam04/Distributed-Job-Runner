package gitservice

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/google/go-github/v90/github"
)

// JobRepo owns one temporary source directory and its extracted repository root.
type JobRepo struct {
	DirPath  string
	RepoPath string
}

func (job *JobRepo) GetRepo(ctx context.Context, link string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	user, repoName, err := ParseGithubLink(link)
	if err != nil {
		return "", err
	}
	client, err := github.NewClient()
	if err != nil {
		return "", err
	}
	archiveURL, _, err := client.Repositories.GetArchiveLink(ctx, user, repoName, github.Zipball, nil, 5)
	if err != nil {
		return "", err
	}
	job.DirPath, err = os.MkdirTemp("", "job-runner-*")
	if err != nil {
		return "", err
	}
	if err := job.downloadRepo(ctx, archiveURL.String()); err != nil {
		return "", errors.Join(err, job.CleanupRepo())
	}
	return job.RepoPath, nil
}

func (job *JobRepo) CleanupRepo() error {
	if err := os.RemoveAll(job.DirPath); err != nil {
		return err
	}
	job.DirPath, job.RepoPath = "", ""
	return nil
}

var githubRepoPath = regexp.MustCompile(`^/([A-Za-z0-9-]+)/([A-Za-z0-9_.-]+)/?$`)

func ParseGithubLink(link string) (string, string, error) {
	u, err := url.Parse(link)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || !strings.EqualFold(u.Host, "github.com") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", "", errors.New("invalid GitHub repository URL")
	}
	fields := githubRepoPath.FindStringSubmatch(u.Path)
	if fields == nil {
		return "", "", errors.New("invalid GitHub repository URL")
	}
	repo := strings.TrimSuffix(fields[2], ".git")
	if repo == "" || repo == "." || repo == ".." {
		return "", "", errors.New("invalid GitHub repository name")
	}
	return fields[1], repo, nil
}

func (job *JobRepo) downloadRepo(ctx context.Context, link string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, link, nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("download repository: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("download repository: bad status: %s", response.Status)
	}
	if err := os.MkdirAll(job.DirPath, 0o700); err != nil {
		return err
	}
	zipPath := filepath.Join(job.DirPath, "repo.zip")
	out, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, response.Body)
	if err := errors.Join(copyErr, out.Close()); err != nil {
		return fmt.Errorf("save repository archive: %w", err)
	}
	return job.unzipRepo(ctx, zipPath)
}

func (job *JobRepo) unzipRepo(ctx context.Context, file string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r, err := zip.OpenReader(file)
	if err != nil {
		return err
	}
	defer r.Close()
	if len(r.File) == 0 {
		return errors.New("repository archive is empty")
	}
	repoRoot := ""
	for _, f := range r.File {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !filepath.IsLocal(f.Name) || strings.Contains(f.Name, `\`) {
			return fmt.Errorf("invalid zip path: %q", f.Name)
		}
		path := filepath.Join(job.DirPath, f.Name)
		top, _, nested := strings.Cut(f.Name, "/")
		root := job.DirPath
		if top != "." && (nested || f.FileInfo().IsDir()) {
			root = filepath.Join(job.DirPath, top)
		}
		if repoRoot == "" {
			repoRoot = root
		} else if repoRoot != root {
			repoRoot = job.DirPath
		}
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(path, 0o755); err != nil {
				return err
			}
			continue
		}
		// ponytail: regular files and directories only; use os.Root for safe extraction when symlinked sources are needed.
		if !f.Mode().IsRegular() {
			return fmt.Errorf("unsupported zip file mode: %q", f.Name)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		source, err := f.Open()
		if err != nil {
			return err
		}
		destination, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, f.Mode().Perm())
		if err != nil {
			source.Close()
			return err
		}
		_, copyErr := io.Copy(destination, contextReader{ctx: ctx, Reader: source})
		if err := errors.Join(copyErr, source.Close(), destination.Close()); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Remove(file); err != nil {
		return err
	}
	job.RepoPath = repoRoot
	return nil
}

type contextReader struct {
	ctx context.Context
	io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}
