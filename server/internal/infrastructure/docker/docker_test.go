package dockerservice

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

func setupTestDockerService(t *testing.T) (context.Context, Builder, Runner) {
	t.Helper()
	apiClient, err := client.New(client.FromEnv)
	if err != nil {
		t.Skipf("Docker is unavailable: %v", err)
	}
	t.Cleanup(func() { apiClient.Close() })
	pingCtx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if _, err := apiClient.Ping(pingCtx, client.PingOptions{}); err != nil {
		t.Skipf("Docker is unavailable: %v", err)
	}
	ctx, stop := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(stop)
	return ctx, Builder{ApiClient: apiClient}, Runner{ApiClient: apiClient}
}

func dockerFixture(t *testing.T, dockerfile string) string {
	t.Helper()
	repo := t.TempDir()
	if err := os.WriteFile(filepath.Join(repo, "Dockerfile"), []byte(dockerfile), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "marker"), []byte("test"), 0o600); err != nil {
		t.Fatal(err)
	}
	return repo
}

func setupTestDockerServiceWithImage(t *testing.T, runnable bool) (context.Context, Builder, Runner, string) {
	t.Helper()
	ctx, builder, runner := setupTestDockerService(t)
	dockerfile := "FROM scratch\nCOPY marker /marker\n"
	if runnable {
		base := os.Getenv("DOCKER_TEST_BASE_IMAGE")
		if base == "" {
			base = "python:3.13-slim"
		}
		if _, err := builder.ApiClient.ImageInspect(ctx, base); err != nil {
			t.Skipf("no local %s base image; set DOCKER_TEST_BASE_IMAGE to a local shell-capable image: %v", base, err)
		}
		dockerfile = fmt.Sprintf("FROM %s\nCMD [\"sh\", \"-c\", \"echo stdout-line; echo stderr-line >&2\"]\n", base)
	}
	imageRef, err := builder.Build(ctx, dockerFixture(t, dockerfile), "Dockerfile", "test-"+strings.ToLower(rand.Text()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := builder.RemoveLocalImage(cleanupCtx, imageRef); err != nil && !errdefs.IsNotFound(err) {
			t.Errorf("cleanup image: %v", err)
		}
	})
	return ctx, builder, runner, imageRef
}

func requireRegistryTests(t *testing.T) {
	t.Helper()
	if os.Getenv("RUN_GHCR_TESTS") != "1" {
		t.Skip("set RUN_GHCR_TESTS=1 to enable real GHCR push/delete tests")
	}
	for _, name := range []string{ghcrWriteTokenEnv, ghcrReadTokenEnv} {
		if os.Getenv(name) == "" {
			t.Fatalf("%s must be set for GHCR tests", name)
		}
	}
}

func pushTestImage(t *testing.T, ctx context.Context, builder *Builder, imageRef string) {
	t.Helper()
	// Register before push so even a partially failed push gets a cleanup attempt.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := builder.RemoveImageFromRegistry(cleanupCtx, imageRef); err != nil {
			t.Errorf("cleanup registry image: %v", err)
		}
	})
	if err := builder.Push(ctx, imageRef); err != nil {
		t.Fatal(err)
	}
}

func TestDockerImageBuildAndRemoveLocal(t *testing.T) {
	ctx, builder, _, imageRef := setupTestDockerServiceWithImage(t, false)
	if err := builder.RemoveLocalImage(ctx, imageRef); err != nil {
		t.Fatal(err)
	}
}

func TestDockerImageBuildPushAndRemove(t *testing.T) {
	requireRegistryTests(t)
	ctx, builder, _, imageRef := setupTestDockerServiceWithImage(t, false)
	pushTestImage(t, ctx, &builder, imageRef)
	if err := builder.RemoveLocalImage(ctx, imageRef); err != nil {
		t.Fatal(err)
	}
}

func TestDockerImageBuildPushPullCleanup(t *testing.T) {
	requireRegistryTests(t)
	ctx, builder, runner, imageRef := setupTestDockerServiceWithImage(t, false)
	pushTestImage(t, ctx, &builder, imageRef)
	if err := builder.RemoveLocalImage(ctx, imageRef); err != nil {
		t.Fatal(err)
	}
	if err := runner.Pull(ctx, imageRef); err != nil {
		t.Fatal(err)
	}
	if err := builder.RemoveLocalImage(ctx, imageRef); err != nil {
		t.Fatal(err)
	}
}

type collectedLogs struct {
	mu    sync.Mutex
	lines map[string]int
}

func (sink *collectedLogs) Write(ctx context.Context, jobID, line string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.lines[jobID+":"+line]++
	return nil
}

func testContainerLifecycle(t *testing.T, count int, registry bool) {
	t.Helper()
	ctx, builder, runner, imageRef := setupTestDockerServiceWithImage(t, true)
	if registry {
		pushTestImage(t, ctx, &builder, imageRef)
		if err := builder.RemoveLocalImage(ctx, imageRef); err != nil {
			t.Fatal(err)
		}
		if err := runner.Pull(ctx, imageRef); err != nil {
			t.Fatal(err)
		}
	}
	var containerIDs []string
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, id := range containerIDs {
			if err := runner.Remove(cleanupCtx, []string{id}); err != nil && !errdefs.IsNotFound(err) {
				t.Errorf("cleanup container: %v", err)
			}
		}
	})
	for range count {
		id, err := runner.Create(ctx, imageRef)
		if err != nil {
			t.Fatal(err)
		}
		containerIDs = append(containerIDs, id)
	}
	if err := runner.Start(ctx, containerIDs); err != nil {
		t.Fatal(err)
	}
	sink := &collectedLogs{lines: make(map[string]int)}
	logErrors := make(chan error, len(containerIDs))
	for _, id := range containerIDs {
		go func() { logErrors <- runner.Logs(ctx, id, "test-job", sink) }()
	}
	waitErr := runner.Wait(ctx, containerIDs)
	for range containerIDs {
		if err := <-logErrors; err != nil {
			t.Errorf("reading container logs: %v", err)
		}
	}
	if waitErr != nil {
		t.Fatal(waitErr)
	}
	for _, line := range []string{"stdout-line", "stderr-line"} {
		if got := sink.lines["test-job:"+line]; got != count {
			t.Errorf("%q logged %d times, want %d", line, got, count)
		}
	}
	if err := runner.Remove(ctx, containerIDs); err != nil {
		t.Fatal(err)
	}
	for _, id := range containerIDs {
		if _, err := runner.ApiClient.ContainerInspect(ctx, id, client.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
			t.Fatalf("removed container %s still exists: %v", id, err)
		}
	}
	if err := builder.RemoveLocalImage(ctx, imageRef); err != nil {
		t.Fatal(err)
	}
}

func TestDockerContainerLifecycle(t *testing.T) {
	testContainerLifecycle(t, 1, false)
}

func TestMultipleContainerLifecycle(t *testing.T) {
	testContainerLifecycle(t, 3, false)
}

func TestRegistryContainerLifecycles(t *testing.T) {
	requireRegistryTests(t)
	for _, count := range []int{1, 3} {
		t.Run(fmt.Sprint(count), func(t *testing.T) { testContainerLifecycle(t, count, true) })
	}
}

func TestDockerContainerCreateNoImage(t *testing.T) {
	runner := &Runner{}
	if id, err := runner.Create(t.Context(), ""); err == nil || id != "" {
		t.Fatalf("expected empty-image error, got id=%q err=%v", id, err)
	}
}

func TestDockerImageBuildFailure(t *testing.T) {
	ctx, builder, _ := setupTestDockerService(t)
	if ref, err := builder.Build(ctx, dockerFixture(t, "INVALID\n"), "Dockerfile", "test-failed"); err == nil || ref != "" {
		t.Fatalf("expected build error without image reference, got ref=%q err=%v", ref, err)
	}
}
