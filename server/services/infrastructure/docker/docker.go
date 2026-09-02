// TODO: Pass context throughout

// This service will spin up a docker client, which will then be used by all
// of the resulting functions. This service can be represented as a struct that contains
// a JobRepo stuct, a docker SDK client struct, and docker image ID.
package dockerservice

import (
	"context"
	gitservice "distributed-job-runner/services/infrastructure/github"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/moby/go-archive"
	"github.com/moby/moby/api/types/jsonstream"
	"github.com/moby/moby/client"
)

type DockerService struct {
	JobRepo     gitservice.JobRepo
	ApiClient   *client.Client
	imageID     string
	containerID string
}

// Creates new dockerservice object that will interact with the repo
func NewDockerService(job gitservice.JobRepo, ApiClient *client.Client) (dockerService DockerService, err error) {
	return DockerService{
		JobRepo:   job,
		ApiClient: ApiClient,
	}, nil
}

func dockerfilePathResolves(absoluteDockerfilePath string) bool {
	_, err := os.Stat(absoluteDockerfilePath)
	if err == nil {
		return true // File exists
	}
	if errors.Is(err, os.ErrNotExist) {
		return false // File explicitly does not exist
	}
	// File may exist but has a different error (e.g., permission denied)
	return false
}

// TODO: This implementation should be changed before being used with untrusted repositories.
// During the build process, arbitrary commands can be run at a root level because docker has root
// level access to its host. A solution to this would be to run image building inside workder VMs running rootless docker.
// This should be implemented before allowing untrusted repositories to be run.

// BuildImage(): Follow the provided relative path in the repo to find dockerfile
// then use the dockerfile to build a docker image

// BuildImage needs to:
// 1) Create a tar stream rooted at JobRepo.RepoPath
// 2) Pass the stream directly to ImageBuild
// 3) Build once
func (dockerService *DockerService) BuildImage(repoPath string, dockerfilePath string) error {
	if !dockerfilePathResolves(filepath.Join(repoPath, dockerfilePath)) {
		return errors.New("Dockerfile path doesn't resolve")
	}
	// Create job repo tar stream
	tar, err := archive.TarWithOptions(repoPath, &archive.TarOptions{})
	if err != nil {
		return err
	}

	// this will eventually be removed when we start passing around context
	ctx := context.Background()

	// Set image build options for docker image. (Will be fleshed out later.)
	imageTag := fmt.Sprintf("distributed-job-runner:%d", time.Now().UnixNano())
	opts := client.ImageBuildOptions{
		Dockerfile: dockerfilePath,
		Remove:     true,
		Tags:       []string{imageTag},
	}

	// Use docker sdk to build the image from the tar
	res, err := dockerService.ApiClient.ImageBuild(ctx, tar, opts)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	decoder := json.NewDecoder(res.Body)
	for {
		var message jsonstream.Message
		if err := decoder.Decode(&message); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		if message.Error != nil {
			return message.Error
		}
		fmt.Print(message.Stream)
	}
	dockerService.imageID = imageTag

	return nil
}

// TODO: Add periodic build cache pruning as a background task.
// Build cache stays left over even if an image is deleted.

// remove the docker image for the given job
func (dockerService *DockerService) CleanupImage() error {
	ctx := context.Background()
	removeResult, err := dockerService.ApiClient.ImageRemove(ctx, dockerService.imageID, client.ImageRemoveOptions{})
	if err != nil {
		return err
	}
	resultJSON, err := json.Marshal(removeResult.Items)
	if err != nil {
		return err
	}
	fmt.Printf("DELETE IMAGE RESULT: %s\n", resultJSON)
	return nil
}

// CreateContainer(): Use the image from the previous step in order to create a
// docker container. This method will probably be called more than once per job
// once we are distributing jobs across multiple containers
func (dockerService *DockerService) CreateContainer() error {
	ctx := context.Background()
	result, err := dockerService.ApiClient.ContainerCreate(ctx, client.ContainerCreateOptions{
		Image: dockerService.imageID,
	})
	if err != nil {
		return err
	}
	dockerService.containerID = result.ID

	return nil
}

// StartContainer(): Start up a previously created container so that the program
// begins executing

// Wait(): Waits until the container finishes running and gives its completion result.
// the job runner will use this to know whether a job failed or not

// LogOutut(): Read stdout/stderr produced by a running container. Output will likely go
// to a job_logs table in mysql db

// RemoveContainer():
