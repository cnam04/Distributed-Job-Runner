// methods for retrieving and managing repository source
// code for a given job.
package gitservice

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/go-github/v90/github"
)

// contains DirPath, repopath
// DirPath -> path to the temp directory
//
//	ex: blah/blah/blah/job-runner-4008147453
//
// repopath -> path to the repo root folder
//
//	ex: blah/blah/blah/job-runner-4008147453/cnam04-Restaurants-to-Pantries--Hackathon-Spring-2026--0d9f1a4
type JobRepo struct {
	DirPath  string
	RepoPath string
}

func NewJobRepo() JobRepo {
	return JobRepo{}
}

// Use the api client to get the repository zip file
//
//	and download it to the filepath
func (job *JobRepo) GetRepo(link string) error {
	client, err := github.NewClient()
	if err != nil {
		return err
	}

	// parse request fields from the link
	user, repoName, err := parseGithubLink(link)
	if err != nil {
		return err
	}
	// send request to get download url
	url, _, err := client.Repositories.GetArchiveLink(
		context.Background(),
		user,
		repoName,
		github.Zipball,
		nil,
		5,
	)
	if err != nil {
		return err
	}

	// download the repo to test-jobs
	jobDir, err := os.MkdirTemp("", "job-runner-*")
	if err != nil {
		return err
	}
	job.DirPath = jobDir + "/"
	err = job.downloadRepo(url.String())
	if err != nil {
		return err
	}
	return nil
}

// use this to get rid of the repo when done with it
func (job *JobRepo) CleanupRepo() error {
	err := os.RemoveAll(job.DirPath)
	if err != nil {
		return err
	}
	return nil
}

// example link: https://github.com/username/project-name
func parseGithubLink(link string) (user string, repoName string, err error) {
	_, after, found := strings.Cut(link, "github.com/")
	if !found {
		return "", "", errors.New("Not a github link")
	}
	fields := strings.Split(after, "/")

	return fields[0], fields[1], nil
}

// streams data from a URL directly to a local zipfile path
func (job *JobRepo) downloadRepo(url string) error {
	// 1. Create the local destination directory and filepath
	os.MkdirAll(job.DirPath, 0755)
	filepath := job.DirPath + "repo.zip"
	out, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer out.Close()

	// 2. Send the HTTP GET request
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to send GET request: %v", err)
	}
	defer resp.Body.Close()

	// 3. Verify the server responded with a successful status code
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	// 4. Stream the server response body directly into the local file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to save file contents: %v", err)
	}

	err = job.unzipRepo(filepath)
	if err != nil {
		return fmt.Errorf("Failed to unzip: %v", err)
	}

	return nil
}

// extract zipfile path
func (job *JobRepo) unzipRepo(file string) error {
	r, err := zip.OpenReader(file)
	if err != nil {
		return err
	}
	defer r.Close()

	// preserve the directory structure by creating necessary directories
	for i, f := range r.File {
		// protect from zip-slipping
		if !filepath.IsLocal(f.Name) {
			return fmt.Errorf("invalid zip path: %q", f.Name)
		}
		path := filepath.Join(job.DirPath, f.Name)
		// collect the repo path within the temp directory
		if i == 0 {
			job.RepoPath = path
		}
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(path, os.ModePerm); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(path), os.ModePerm); err != nil {
			return err
		}

		srcFile, err := f.Open()
		if err != nil {
			return err
		}

		destFile, err := os.Create(path)
		if err != nil {
			srcFile.Close()
			return err
		}

		_, err = io.Copy(destFile, srcFile)

		srcFile.Close()
		destFile.Close()

		if err != nil {
			return err
		}
	}

	// remove zip file
	os.Remove(file)

	return nil
}
