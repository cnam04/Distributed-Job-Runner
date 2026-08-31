package main

import (
	gitservice "distributed-job-runner/services/infrastructure/github"
	"fmt"
	"time"
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

	time.Sleep(30 * time.Second)

}
