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

// contains dirpath, repopath
// dirpath -> path to the temp directory
//
//	ex: blah/blah/blah/job-runner-4008147453
//
// repopath -> path to the repo root folder
//
//	ex: blah/blah/blah/job-runner-4008147453/cnam04-Restaurants-to-Pantries--Hackathon-Spring-2026--0d9f1a4
type JobRepo struct {
	dirPath  string
	repoPath string
}

func newJobRepo() JobRepo {
	return JobRepo{}
}

// TODO: Refactor to use a jobrepo struct
//  or something so that you dont have to pass a bunch of
// 	random strings. Then you can defer a call JobRepo.CleanupRepo after
// 	I'd attach the methods to the struct using pointers so they modify
//	struct fields. Then i don't have to pass dirs as return vals

// Use the api client to get the repository zip file
//
//	and download it to the filepath
func GetRepo(link string) (repoPath string, err error) {
	client, err := github.NewClient()
	if err != nil {
		return "", err
	}

	// parse request fields from the link
	user, repoName, err := parseGithubLink(link)
	if err != nil {
		return "", err
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
		return "", err
	}

	// download the repo to test-jobs
	jobDir, err := os.MkdirTemp("", "job-runner-*")
	fmt.Println(jobDir)
	downloadRepo(jobDir+"/", url.String())
	return repoPath, nil
}

// use this to get rid of the repo when done with it
// TODO: Implement this
func CleanupRepo() {

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
func downloadRepo(dirPath string, url string) (repoPath string, err error) {
	// 1. Create the local destination directory and filepath
	os.MkdirAll(dirPath, 0755)
	filepath := dirPath + "repo.zip"
	out, err := os.Create(filepath)
	if err != nil {
		return "", fmt.Errorf("failed to create file: %v", err)
	}
	defer out.Close()

	// 2. Send the HTTP GET request
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to send GET request: %v", err)
	}
	defer resp.Body.Close()

	// 3. Verify the server responded with a successful status code
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("bad status: %s", resp.Status)
	}

	// 4. Stream the server response body directly into the local file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to save file contents: %v", err)
	}

	repoPath, err = unzipRepo(filepath, dirPath)
	if err != nil {
		return "", fmt.Errorf("Failed to unzip: %v", err)
	}

	return repoPath, nil
}

// extract zipfile path
// TODO: Add zip slip protection
func unzipRepo(file string, dirPath string) (repoPath string, err error) {
	r, err := zip.OpenReader(file)
	if err != nil {
		return "", err
	}
	defer r.Close()

	// preserve the directory structure by creating necessary directories
	repoPath = ""
	for i, f := range r.File {
		path := filepath.Join(dirPath, f.Name)
		// collect the repo path within the temp directory
		if i == 0 {
			repoPath = path
		}
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(path, os.ModePerm); err != nil {
				return "", err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(path), os.ModePerm); err != nil {
			return "", err
		}

		srcFile, err := f.Open()
		if err != nil {
			return "", err
		}

		destFile, err := os.Create(path)
		if err != nil {
			srcFile.Close()
			return "", err
		}

		_, err = io.Copy(destFile, srcFile)

		srcFile.Close()
		destFile.Close()

		if err != nil {
			return "", err
		}
	}

	// remove zip file
	os.Remove(file)

	return repoPath, nil
}
