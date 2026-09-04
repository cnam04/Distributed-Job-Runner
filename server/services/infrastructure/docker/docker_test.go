package dockerservice

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/moby/moby/client"
)

func setupTestDockerService(t *testing.T) (repoPath string, dockerService DockerService) {
	apiClient, err := client.New(client.FromEnv)
	t.Cleanup(func() { apiClient.Close() })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := apiClient.Ping(context.Background(), client.PingOptions{}); err != nil {
		t.Skipf("Docker is unavailable: %v", err)
	}

	repoPath = filepath.Join("..", "..", "..", "..", "testjobs", "python-data-analysis")
	dockerService = DockerService{ApiClient: apiClient}

	return repoPath, dockerService
}

func setupTestDockerServiceWithImage(t *testing.T) (repoPath string, dockerService DockerService) {
	repoPath, dockerService = setupTestDockerService(t)
	if err := dockerService.BuildImage(repoPath, "Dockerfile"); err != nil {
		t.Fatal(err)
	}
	return repoPath, dockerService
}

func TestDockerImageBuildandCleanup(t *testing.T) {
	_, dockerService := setupTestDockerServiceWithImage(t)
	if err := dockerService.CleanupImage(); err != nil {
		t.Fatal(err)
	}
}

func TestDockerContainerLifecycle(t *testing.T) {
	_, dockerService := setupTestDockerServiceWithImage(t)
	if err := dockerService.CreateContainer(); err != nil {
		t.Fatal(err)
	}
	if err := dockerService.StartContainers(); err != nil {
		t.Fatal(err)
	}
	if err := dockerService.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := dockerService.RemoveContainer(); err != nil {
		t.Fatal(err)
	}
	if len(dockerService.containerIDs) != 0 {
		t.Fatal("removed container ID was retained")
	}
	if err := dockerService.CleanupImage(); err != nil {
		t.Fatal(err)
	}
}

func TestDockerContainerCreateNoImage(t *testing.T) {
	_, dockerService := setupTestDockerService(t)
	if err := dockerService.CreateContainer(); err != nil {
		expected := "No image specified to create container from. Image may not have been created."
		if err.Error() != expected {
			t.Errorf("expected error message %q, got %q", expected, err.Error())
		}
	}
}

func TestMultipleContainerLifecycle(t *testing.T) {
	_, dockerService := setupTestDockerServiceWithImage(t)

	for range 3 {
		if err := dockerService.CreateContainer(); err != nil {
			t.Fatal(err)
		}
	}

	// Start every container before following its output.
	if err := dockerService.StartContainers(); err != nil {
		t.Fatal(err)
	}

	// Stream each container's logs concurrently and retain any errors.
	logErrors := make(chan error, len(dockerService.containerIDs))
	for i := range dockerService.containerIDs {
		go func(index int) {
			logErrors <- dockerService.LogOutput(index)
		}(i)
	}

	// Wait for all containers, then join every logging goroutine before cleanup.
	waitErr := dockerService.Wait()
	for range dockerService.containerIDs {
		if err := <-logErrors; err != nil {
			t.Errorf("reading container logs: %v", err)
		}
	}
	if waitErr != nil {
		t.Fatal(waitErr)
	}

	if err := dockerService.RemoveContainer(); err != nil {
		t.Fatal(err)
	}
	if len(dockerService.containerIDs) != 0 {
		t.Fatal("removed container IDs were retained")
	}
	if err := dockerService.CleanupImage(); err != nil {
		t.Fatal(err)
	}
}

func TestDockerImageBuildFailure(t *testing.T) {
	_, dockerService := setupTestDockerService(t)
	repoPath := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoPath, "Dockerfile"), []byte("INVALID\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := dockerService.BuildImage(repoPath, "Dockerfile"); err == nil {
		t.Fatal("expected build error")
	}
	if dockerService.imageRef != "" {
		t.Fatal("failed build assigned an image ID")
	}
}
