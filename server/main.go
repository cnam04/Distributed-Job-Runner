package main

import (
	dockerservice "distributed-job-runner/services/infrastructure/docker"
	gitservice "distributed-job-runner/services/infrastructure/github"
	"fmt"
	"time"

	"github.com/moby/moby/client"
)

// This is currently basic functional test code.
// This will be replaces later with the rest api and db instantiation
func main() {
	repo := "https://github.com/cnam04/Restaurants-to-Pantries--Hackathon-Spring-2026-"
	currentJob := gitservice.NewJobRepo()

	err := currentJob.GetRepo(repo)
	if err != nil {
		panic(err)
	}

	defer currentJob.CleanupRepo()

	fmt.Println(currentJob.RepoPath)
	fmt.Println(currentJob.DirPath)

	apiClient, err := client.New()
	if err != nil {
		panic(err)
	}

	dockerService, err := dockerservice.NewDockerService(currentJob, apiClient)
	if err != nil {
		panic(err)
	}

	dockerService.BuildImage(currentJob.RepoPath, "client/Dockerfile")

	time.Sleep(15 * time.Second)

}
