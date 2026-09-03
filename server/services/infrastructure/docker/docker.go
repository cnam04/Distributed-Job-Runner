// TODO: Pass context throughout

// TODO: Update BuildImage to push image to a registry that CreateContainer pulls from, rather than just using the id tag. This will allow

// This service will spin up a docker client, which will then be used by all
// of the resulting functions. This service can be represented as a struct that contains
// a JobRepo stuct, a docker SDK client struct, and docker image ID.
package dockerservice

import (
	"bufio"
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
	JobRepo      gitservice.JobRepo
	ApiClient    *client.Client
	imageID      string
	containerIDs []string
}

// Creates new dockerservice object that will interact with the repo
func NewDockerService(job gitservice.JobRepo, ApiClient *client.Client) (dockerService DockerService, err error) {
	ids := make([]string, 0, 0)
	return DockerService{
		JobRepo:      job,
		ApiClient:    ApiClient,
		imageID:      "",
		containerIDs: ids,
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
	imageTag := fmt.Sprintf("distributed-job-runner-image-%d", time.Now().UnixNano())
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
	// if imageID doesn't exist, that means this method is being called while a working image doesn't exist
	if dockerService.imageID == "" {
		return errors.New("No image specified to create container from. Image may not have been created.")
	}

	ctx := context.Background()
	containerTag := fmt.Sprintf("%v-CONTAINER-%d", dockerService.imageID, time.Now().UnixNano())
	opts := client.ContainerCreateOptions{
		Image: dockerService.imageID,
		Name:  containerTag,
	}
	result, err := dockerService.ApiClient.ContainerCreate(ctx, opts)
	if err != nil {
		return err
	}
	dockerService.containerIDs = append(dockerService.containerIDs, result.ID)

	return nil
}

// StartContainer(): Start up all previously created containers so that the program
// begins executing
func (dockerService *DockerService) StartContainers() error {
	if len(dockerService.containerIDs) == 0 {
		return errors.New("No containers have been created to start.")
	}

	ctx := context.Background()
	for _, containerID := range dockerService.containerIDs {
		if _, err := dockerService.ApiClient.ContainerStart(ctx, containerID, client.ContainerStartOptions{}); err != nil {
			return err
		}
	}

	return nil
}

// Wait(): Waits until the container finishes running and gives its completion result.
// the job runner will use this to know whether a job failed or not
// When a job is run, the LogOutput method is spawned as a goroutine, and the main routine is blocked on wait until the job is complete

// TODO: Pass real context to this method rather than instantiating context
func (dockerService *DockerService) Wait() error {
	if len(dockerService.containerIDs) == 0 {
		return errors.New("No containers have been created to wait for.")
	}

	ctx := context.Background()
	var waitErrors []error

	// Wait for all containers to be finished running and collect errors
	for _, containerID := range dockerService.containerIDs {
		// ContainerWait exposes two channels, Result and Error
		waitResult := dockerService.ApiClient.ContainerWait(ctx, containerID, client.ContainerWaitOptions{})
		select {
		case result := <-waitResult.Result:
			if result.Error != nil {
				waitErrors = append(waitErrors, fmt.Errorf("container %s: %s", containerID, result.Error.Message))
			} else if result.StatusCode != 0 {
				waitErrors = append(waitErrors, fmt.Errorf("container %s exited with status %d", containerID, result.StatusCode))
			}
		case err := <-waitResult.Error:
			if err != nil {
				waitErrors = append(waitErrors, fmt.Errorf("waiting for container %s: %w", containerID, err))
			}
		case <-ctx.Done():
			return errors.Join(append(waitErrors, ctx.Err())...)
		}
	}

	return errors.Join(waitErrors...)
}

// LogOutut(): Read stdout/stderr produced by a running container. Output will likely go
// to a job_logs table in mysql db
// For now, logs will just be printed.
// TODO: Send logs to mysql db
// TODO: Replace context declaration with parent context
func (dockerService *DockerService) LogOutput(containerListIndex int) error {
	if len(dockerService.containerIDs) == 0 {
		return errors.New("No containers have been created to remove.")
	}
	if containerListIndex < 0 || containerListIndex > (len(dockerService.containerIDs)-1) {
		return errors.New("containerListIndex out of bounds")
	}

	ctx := context.Background()
	opts := client.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Details:    true,
	}
	containerLogsResult, err := dockerService.ApiClient.ContainerLogs(ctx, dockerService.containerIDs[containerListIndex], opts)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, containerLogsResult.Close())
	}()

	// Create a new scanner wrapped around the ReadCloser (containerLogsResult implements the ReadCloser interface)
	scanner := bufio.NewScanner(containerLogsResult)

	// Read line by line
	for scanner.Scan() {
		line := scanner.Text() // Retrieves the line as a string
		fmt.Println(line)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

// RemoveContainer():
func (dockerService *DockerService) RemoveContainer() error {
	if len(dockerService.containerIDs) == 0 {
		return errors.New("No containers have been created to remove.")
	}

	ctx := context.Background()
	for len(dockerService.containerIDs) > 0 {
		if _, err := dockerService.ApiClient.ContainerRemove(ctx, dockerService.containerIDs[0], client.ContainerRemoveOptions{Force: true}); err != nil {
			return err
		}
		dockerService.containerIDs = dockerService.containerIDs[1:]
	}

	return nil
}
