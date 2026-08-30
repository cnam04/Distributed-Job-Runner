package main

import (
	gitservice "distributed-job-runner/services/infrastructure/github"
	"fmt"
)

func main() {
	repo := "https://github.com/cnam04/Restaurants-to-Pantries--Hackathon-Spring-2026-"
	currentJob := gitservice.NewJobRepo()

	err := currentJob.GetRepo(repo)
	if err != nil {
		panic(err)
	}

	fmt.Println(currentJob.RepoPath)
	fmt.Println(currentJob.DirPath)
}
