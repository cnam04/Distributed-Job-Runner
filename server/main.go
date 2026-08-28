package main

import (
	gitservice "distributed-job-runner/services/infrastructure/github"
	"fmt"
)

func main() {
	repo := "https://github.com/cnam04/Restaurants-to-Pantries--Hackathon-Spring-2026-"
	repoPath, _ := gitservice.GetRepo(repo)

	fmt.Println(repoPath)

}
