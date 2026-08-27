# Distributed-Job-Runner
Distributed job runner for parellelizable workloads.

Allows for slow scripts to be run over multiple containers on school servers. For example, a student submits a preprocessing script and a dataset containing 50,000 images -> dataset is split into chunks and the script is run on chunks distributed across multiple servers. 

This software both speeds up processing and eliminates the need for SUNY New Paltz students to rely on local computers.

### Problem 
Many students run data analysis pipelines with large datasets and many computations. Students are often running these pipelines locally, with minimal resources. Although data preprocessing pipelines are often parallelized, these jobs are limited by the CPU and memory constraints of students' local machines. Sometimes, student local computers may not be able to handle the job at all. Student laptop capabilities shouldn't interfere with research and learning. 

### Solution 
The Computer Science department at SUNY New Paltz has servers that could potentially be used as shared compute capacity.

Create a software that allows students to submit a dataset and data-preprocessing job that would be run remotely on the schools servers.

### Scope
This project would be used for parallelizable batch workloads where the input dataset can be partitioned into independent chunks, and each chunk can be processed without depending on the results of other chunks.

### High level design
Ideally this framework would be language/framework agnostic: the students write the script and handle cpu core usage/threading, the software handles chunking, execution, deployment, and monitoring.

**Here's how the application would flow:**
1) User submits:
- container / code
- dataset
- resources
- chunking configuration

2) The program examines the dataset, creates work chunks, and provisions a pool of worker containers

4) The program creates a work queue based on chunks. EX:
chunk-001
chunk-002
chunk-003
5) The containers on each server request work
6) A chunk is delegated to that container
7) Container runs the student's program and returns the output
8) If container fails, program may recreate the container and try more attempts for that chunk
9) When the container finishes the work, it requests more work
10) Steps 5-9 are repeated on all containers until all chunks are complete
11) The program aggregates the outputs

### States of a job
It is important for a job to have well-defined states to better understand failures.
Jobs will have 5 states:
1) **Queued** - The job has been accepted but execution hasn't started yet
2) **Cloning** - Servers are cloning the repo in the container
3) **Building** - Servers are going through the process of building the job's binaries
4) **Running** - The actual user code is being run
5) **Succeeded** - The job has finished successfully
6) **Failed** - The job has finished with a failure

## Progress

### Week 0 - Building a client side
Users should be able to submit a job through the web. The first thing I'll be working on is building a simple website that allows the user to submit a form containing the github repo, a Dockerfile path, and a run command through a POST request.

*Something like POST /jobs/*

**On the server side:**

Upon receiving the job API request, the server should:
- validate the submission
- create a Job object
- respond with a Job ID to the client
And asynchronously it should:
- clone the repo
- build the project with docker
- run the project
- just capture stdout/stderr (for now)
- update the job status

**On the client side:**
After submitting the post request, the client should be redirected to a monitoring dashboard for the newly-created job. For now, all this dashboard will contain is the current status of the job ("in progress", "success", "failure"), as well as the stdout, stderr output from running the job. 
- For now, this data will be collected through simple repeated polling of an endpoint like GET /jobs/{id}. No websocket or sse until it works.


**How is job state stored**
The state of a job needs to be stored, so that its state can be updated and queried. State will probably be represented like:

```
=========
jobs
=========
id
repo_url
status
created_at
started_at
completed_at
exit_code
error_msg

=========
job_logs
=========
id
job_id
stream  -> is it stdout or stderr?
message -> the actual log message
created_at -> timestamp 
sequence -> the order of the log messages, so that jobs can be easily sorted through. EX: last sequence printed by client was #38, then client only requests logs with sequence>38 to eliminate overlap
```

**The tech stack**
- _React:_ wide component support, easy to use with changing states
- _SQLite:_ Not much data is being stored, and the biggest priority here is just to have a fast and simple way to query and update state data by eliminating the need for a separate database server. Anything else would be overkill
- _Golang:_ The system will need to manage many concurrent operations like HTTP requests, job execution, log/health monitoring, and distributed workers. Goroutines make concurrent I/O heavy work simple and straightforward without sacrificing performance. 





