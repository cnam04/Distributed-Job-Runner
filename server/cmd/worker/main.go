package main

import (
	"context"
	dockerservice "distributed-job-runner/internal/infrastructure/docker"
	"distributed-job-runner/internal/transport"
	workerhttp "distributed-job-runner/internal/transport/worker"
	"distributed-job-runner/internal/worker"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/moby/moby/client"
)

func main() {
	addr := flag.String("addr", ":8081", "worker HTTP address")
	flag.Parse()
	if err := run(*addr); err != nil {
		log.Fatal(err)
	}
}

func run(addr string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	log.SetOutput(os.Stdout)
	apiClient, err := client.New(client.FromEnv)
	if err != nil {
		return err
	}
	defer apiClient.Close()
	service := &worker.Service{
		Runner: &dockerservice.Runner{ApiClient: apiClient},
		Logs:   worker.ConsoleLogSink{},
	}
	log.Printf("Worker listening on %s", addr)
	return transport.Serve(ctx, addr, workerhttp.NewHandler(service))
}
