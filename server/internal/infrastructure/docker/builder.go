package dockerservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/moby/go-archive"
	"github.com/moby/moby/api/types/jsonstream"
	"github.com/moby/moby/client"
)

type Builder struct {
	ApiClient *client.Client
}

// Building untrusted Dockerfiles still requires an isolated, rootless Docker host.
func (b *Builder) Build(ctx context.Context, repoPath, dockerfilePath, jobID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if !jobTag.MatchString(jobID) {
		return "", errors.New("job ID is not a valid image tag")
	}
	if !filepath.IsLocal(dockerfilePath) || strings.Contains(dockerfilePath, `\`) {
		return "", errors.New("Dockerfile path must be repository-relative")
	}
	repoPath, err := filepath.Abs(repoPath)
	if err != nil {
		return "", err
	}
	repoPath, err = filepath.EvalSymlinks(repoPath)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(filepath.Join(repoPath, dockerfilePath))
	if err != nil {
		return "", fmt.Errorf("resolve Dockerfile: %w", err)
	}
	relative, err := filepath.Rel(repoPath, resolved)
	if err != nil || !filepath.IsLocal(relative) {
		return "", errors.New("Dockerfile path escapes repository")
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("Dockerfile must be a regular file")
	}
	tar, err := archive.TarWithOptions(repoPath, &archive.TarOptions{})
	if err != nil {
		return "", err
	}
	defer tar.Close()
	imageRef := ghcrRepository + ":" + jobID
	result, err := b.ApiClient.ImageBuild(ctx, tar, client.ImageBuildOptions{
		Dockerfile: filepath.ToSlash(filepath.Clean(dockerfilePath)),
		Remove:     true,
		Tags:       []string{imageRef},
	})
	if err != nil {
		return "", fmt.Errorf("build image: %w", err)
	}
	defer result.Body.Close()
	decoder := json.NewDecoder(result.Body)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		var message jsonstream.Message
		err := decoder.Decode(&message)
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if errors.Is(err, io.EOF) {
			return imageRef, nil
		}
		if err != nil {
			return "", fmt.Errorf("read build response: %w", err)
		}
		if message.Error != nil {
			return "", fmt.Errorf("build image: %w", message.Error)
		}
	}
}

func (b *Builder) Push(ctx context.Context, imageRef string) error {
	if imageRef == "" {
		return errors.New("no image specified to push")
	}
	auth, err := ghcrAuth(false)
	if err != nil {
		return err
	}
	result, err := b.ApiClient.ImagePush(ctx, imageRef, client.ImagePushOptions{RegistryAuth: auth})
	if err != nil {
		return fmt.Errorf("start image push: %w", err)
	}
	if err := result.Wait(ctx); err != nil {
		return fmt.Errorf("push image: %w", err)
	}
	return ctx.Err()
}

func (b *Builder) RemoveLocalImage(ctx context.Context, imageRef string) error {
	if imageRef == "" {
		return errors.New("no image specified to remove")
	}
	_, err := b.ApiClient.ImageRemove(ctx, imageRef, client.ImageRemoveOptions{})
	return err
}
