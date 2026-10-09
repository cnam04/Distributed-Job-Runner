package main

import (
	"context"
	"distributed-job-runner/internal/coordinator"
	dockerservice "distributed-job-runner/internal/infrastructure/docker"
	gitservice "distributed-job-runner/internal/infrastructure/github"
	"distributed-job-runner/internal/transport"
	coordinatorhttp "distributed-job-runner/internal/transport/coordinator"
	workerhttp "distributed-job-runner/internal/transport/worker"
	"flag"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/moby/moby/client"
)

func main() {
	addr := flag.String("addr", ":3000", "coordinator HTTP address")
	workerURL := flag.String("worker-url", "http://localhost:8081", "worker HTTP URL")
	flag.Parse()
	u, err := url.Parse(*workerURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		log.Fatal("worker-url must be an HTTP base URL")
	}
	if err := run(*addr, *workerURL); err != nil {
		log.Fatal(err)
	}
}

func run(addr, workerURL string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	apiClient, err := client.New(client.FromEnv)
	if err != nil {
		return err
	}
	defer apiClient.Close()
	jobs := coordinator.NewMemoryJobStore()
	service := &coordinator.Service{
		NewRepo: func() coordinator.RepoFetcher { return &gitservice.JobRepo{} },
		Images:  &dockerservice.Builder{ApiClient: apiClient},
		// currently, Workers refers to a singlar worker client that is hardcoded
		Workers:    &workerhttp.Client{URL: workerURL, HTTP: &http.Client{}, Log: jobs.AppendLog},
		Jobs:       jobs,
		RunContext: ctx,
	}
	log.Printf("Coordinator listening on %s, worker %s", addr, workerURL)
	return transport.Serve(ctx, addr, coordinatorhttp.NewHandler(service))
}
