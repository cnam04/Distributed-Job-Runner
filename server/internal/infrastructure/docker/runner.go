package dockerservice

import (
	"bufio"
	"context"
	"crypto/rand"
	"distributed-job-runner/internal/worker"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/client"
)

type Runner struct {
	ApiClient *client.Client
}

func (r *Runner) Pull(ctx context.Context, imageRef string) error {
	if imageRef == "" {
		return errors.New("no image specified to pull")
	}
	auth, err := ghcrAuth(true)
	if err != nil {
		return err
	}
	result, err := r.ApiClient.ImagePull(ctx, imageRef, client.ImagePullOptions{RegistryAuth: auth})
	if err != nil {
		return fmt.Errorf("start image pull: %w", err)
	}
	if err := result.Wait(ctx); err != nil {
		return fmt.Errorf("pull image: %w", err)
	}
	return ctx.Err()
}

func (r *Runner) Create(ctx context.Context, imageRef string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if imageRef == "" {
		return "", errors.New("no image specified to create container from")
	}
	name := "distributed-job-runner-" + strings.ToLower(rand.Text())
	result, err := r.ApiClient.ContainerCreate(ctx, client.ContainerCreateOptions{Image: imageRef, Name: name})
	if err != nil {
		// Docker may have created the container even if its response was lost.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		cleanupErr := r.Remove(cleanupCtx, []string{name})
		if errdefs.IsNotFound(cleanupErr) {
			cleanupErr = nil
		}
		return "", errors.Join(err, cleanupErr)
	}
	return result.ID, nil
}

func (r *Runner) Start(ctx context.Context, containerIDs []string) error {
	if len(containerIDs) == 0 {
		return errors.New("no containers specified to start")
	}
	for _, id := range containerIDs {
		if _, err := r.ApiClient.ContainerStart(ctx, id, client.ContainerStartOptions{}); err != nil {
			return fmt.Errorf("start container %s: %w", id, err)
		}
	}
	return nil
}

func (r *Runner) Logs(ctx context.Context, containerID, jobID string, sink worker.LogSink) error {
	if sink == nil {
		return errors.New("log sink is required")
	}
	logs, err := r.ApiClient.ContainerLogs(ctx, containerID, client.ContainerLogsOptions{
		ShowStdout: true, ShowStderr: true, Follow: true,
	})
	if err != nil {
		return err
	}
	// Keep partial stdout and stderr lines separate; Create does not enable TTY.
	stdout, stdoutWriter := io.Pipe()
	stderr, stderrWriter := io.Pipe()
	done := make(chan struct{})
	var scanners sync.WaitGroup
	var sinkMu sync.Mutex
	var sinkError error
	scanErrors := make(chan error, 2)
	scan := func(reader io.Reader) {
		defer scanners.Done()
		scanner := bufio.NewScanner(reader)
		// ponytail: log lines are capped at 1 MiB; use bufio.Reader if jobs emit longer lines.
		scanner.Buffer(make([]byte, 4096), 1024*1024)
		for scanner.Scan() {
			sinkMu.Lock()
			if sinkError == nil {
				sinkError = ctx.Err()
				if sinkError == nil {
					sinkError = sink.Write(ctx, jobID, scanner.Text())
				}
			}
			err := sinkError
			sinkMu.Unlock()
			if err != nil {
				scanErrors <- err
				return
			}
		}
		scanErrors <- errors.Join(ctx.Err(), scanner.Err())
	}
	scanners.Add(2)
	go scan(stdout)
	go scan(stderr)
	go func() {
		defer close(done)
		_, err := stdcopy.StdCopy(stdoutWriter, stderrWriter, logs)
		stdoutWriter.CloseWithError(err)
		stderrWriter.CloseWithError(err)
	}()
	defer func() {
		stdout.Close()
		stderr.Close()
		logs.Close()
		<-done
		scanners.Wait()
	}()
	for range 2 {
		if err := <-scanErrors; err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) Wait(ctx context.Context, containerIDs []string) error {
	if len(containerIDs) == 0 {
		return errors.New("no containers specified to wait for")
	}
	var waitErrors []error
	for _, id := range containerIDs {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(waitErrors, err)...)
		}
		result := r.ApiClient.ContainerWait(ctx, id, client.ContainerWaitOptions{})
		// The SDK cancels the request with ctx; consume its unbuffered result.
		select {
		case response := <-result.Result:
			if response.Error != nil {
				waitErrors = append(waitErrors, fmt.Errorf("container %s: %s", id, response.Error.Message))
			}
			if response.StatusCode != 0 {
				waitErrors = append(waitErrors, &worker.ExitError{ContainerID: id, StatusCode: response.StatusCode})
			}
		case err := <-result.Error:
			if err != nil {
				waitErrors = append(waitErrors, fmt.Errorf("waiting for container %s: %w", id, err))
			}
		}
	}
	return errors.Join(append(waitErrors, ctx.Err())...)
}

func (r *Runner) Remove(ctx context.Context, containerIDs []string) error {
	var removeErrors []error
	for _, id := range containerIDs {
		if _, err := r.ApiClient.ContainerRemove(ctx, id, client.ContainerRemoveOptions{Force: true}); err != nil {
			removeErrors = append(removeErrors, fmt.Errorf("remove container %s: %w", id, err))
		}
	}
	return errors.Join(removeErrors...)
}
