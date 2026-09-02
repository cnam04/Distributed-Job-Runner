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

func TestDockerImageBuildandCleanup(t *testing.T) {
	repoPath, dockerService := setupTestDockerService(t)
	if err := dockerService.BuildImage(repoPath, "Dockerfile"); err != nil {
		t.Fatal(err)
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
	if dockerService.imageID != "" {
		t.Fatal("failed build assigned an image ID")
	}
}
