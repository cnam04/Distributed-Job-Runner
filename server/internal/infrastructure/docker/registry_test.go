package dockerservice

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-github/v90/github"
)

func TestRemoveImageRegistry(t *testing.T) {
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/user/packages/container/distributed-job-runner/versions":
			io.WriteString(w, `[{"id":41,"metadata":{"container":{"tags":["other"]}}},{"id":42,"metadata":{"container":{"tags":["123"]}}}]`)
		case r.Method == http.MethodDelete && r.URL.Path == "/user/packages/container/distributed-job-runner/versions/42":
			deleted = true
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := github.NewClient(github.WithHTTPClient(server.Client()), github.WithURLs(&server.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := removeImageRegistry(t.Context(), client, ghcrRepository+":123"); err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("expected matching package version to be deleted")
	}
	if err := removeImageRegistry(t.Context(), client, ghcrRepository+":missing"); err == nil {
		t.Fatal("expected missing tag error")
	}
	for _, ref := range []string{"", ghcrRepository + ":", "other:123", ghcrRepository + ":123/other"} {
		if err := removeImageRegistry(t.Context(), client, ref); err == nil {
			t.Errorf("expected invalid image %q to be rejected", ref)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := removeImageRegistry(ctx, client, ghcrRepository+":123"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func TestGHCRToken(t *testing.T) {
	t.Setenv(ghcrWriteTokenEnv, "write-secret")
	t.Setenv(ghcrReadTokenEnv, "read-secret")
	for _, readOnly := range []bool{false, true} {
		want, name := "write-secret", ghcrWriteTokenEnv
		if readOnly {
			want, name = "read-secret", ghcrReadTokenEnv
		}
		if token, err := ghcrToken(readOnly); err != nil || token != want {
			t.Fatalf("token = %q, err = %v", token, err)
		}
		t.Setenv(name, "")
		if token, err := ghcrToken(readOnly); err == nil || token != "" || !strings.Contains(err.Error(), name) {
			t.Fatalf("missing token = %q, err = %v", token, err)
		}
	}
}
