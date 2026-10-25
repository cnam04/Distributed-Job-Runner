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
	"strings"
	"syscall"

	"github.com/moby/moby/client"
)

func main() {
	coordinatorAddrInput := flag.String("addr", ":3000", "coordinator HTTP address")
	workerURLInput := flag.String(
		"worker-url",
		"http://localhost:8081",
		"worker HTTP URL. For many, separate with a comma. Ex: (http://localhost:8081,http://localhost:8082,http://localhost:8083)",
	)
	flag.Parse()

	// get all worker addresses
	workerUrls := parseWorkers(workerURLInput)

	if err := run(*coordinatorAddrInput, workerUrls); err != nil {
		log.Fatal(err)
	}
}

func run(addr string, workerURLs []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	apiClient, err := client.New(client.FromEnv)
	if err != nil {
		return err
	}
	defer apiClient.Close()
	jobs := coordinator.NewMemoryJobStore()
	service := &coordinator.Service{
		NewRepo:    func() coordinator.RepoFetcher { return &gitservice.JobRepo{} },
		Images:     &dockerservice.Builder{ApiClient: apiClient},
		Workers:    newWorkers(workerURLs, jobs),
		Jobs:       jobs,
		RunContext: ctx,
	}
	log.Printf("Coordinator listening on %s", addr)
	for i, url := range workerURLs {
		log.Printf("Worker %d: %s ", i, url)
	}
	log.Println()

	return transport.Serve(ctx, addr, coordinatorhttp.NewHandler(service))
}

func newWorkers(workerUrls []string, jobs *coordinator.MemoryJobStore) []coordinator.WorkerClient {
	var workerClients []coordinator.WorkerClient
	for workerID, url := range workerUrls {
		workerClients = append(workerClients, &workerhttp.Client{
			URL:  url,
			HTTP: &http.Client{},
			Log: func(ctx context.Context, jobID, taskID, line string) error {
				return jobs.AppendLog(ctx, jobID, workerID, taskID, line)
			},
		})
	}
	return workerClients
}

func parseWorkers(urlList *string) []string {
	workerUrls := strings.Split(*urlList, ",")

	for _, w := range workerUrls {
		u, err := url.Parse(w)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			log.Fatal("worker-url must be an HTTP base URL")
		}
	}

	return workerUrls
}
