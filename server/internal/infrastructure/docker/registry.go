package dockerservice

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/google/go-github/v90/github"
	"github.com/moby/moby/api/pkg/authconfig"
	"github.com/moby/moby/api/types/registry"
)

const (
	ghcrRepository    = "ghcr.io/cnam04/distributed-job-runner"
	ghcrWriteTokenEnv = "READ_WRITE_GHCR_TOKEN"
	ghcrReadTokenEnv  = "READ_ONLY_GHCR_TOKEN"
	ghcrUsername      = "cnam04"
)

var jobTag = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)

func ghcrToken(readOnly bool) (string, error) {
	name := ghcrWriteTokenEnv
	if readOnly {
		name = ghcrReadTokenEnv
	}
	if token := os.Getenv(name); token != "" {
		return token, nil
	}
	return "", fmt.Errorf("%s is not set", name)
}

func ghcrAuth(readOnly bool) (string, error) {
	token, err := ghcrToken(readOnly)
	if err != nil {
		return "", err
	}
	return authconfig.Encode(registry.AuthConfig{
		Username: ghcrUsername, Password: token, ServerAddress: "ghcr.io",
	})
}

func (b *Builder) RemoveImageFromRegistry(ctx context.Context, imageRef string) error {
	token, err := ghcrToken(false)
	if err != nil {
		return err
	}
	client, err := github.NewClient(github.WithAuthToken(token))
	if err != nil {
		return err
	}
	return removeImageRegistry(ctx, client, imageRef)
}

func removeImageRegistry(ctx context.Context, client *github.Client, imageRef string) error {
	tag, ok := strings.CutPrefix(imageRef, ghcrRepository+":")
	if !ok || !jobTag.MatchString(tag) {
		return errors.New("image must be a tagged job-runner GHCR image")
	}
	for version, err := range client.Users.ListPackageVersionsIter(ctx, "container", "distributed-job-runner", nil) {
		if err != nil {
			return fmt.Errorf("list package versions: %w", err)
		}
		metadata, ok := version.GetMetadata()
		if !ok || !slices.Contains(metadata.GetContainer().GetTags(), tag) {
			continue
		}
		if _, err := client.Users.PackageDeleteVersion(ctx, "", "container", "distributed-job-runner", version.GetID()); err != nil {
			return fmt.Errorf("delete package version: %w", err)
		}
		return nil
	}
	return fmt.Errorf("image tag %q not found in registry", tag)
}
