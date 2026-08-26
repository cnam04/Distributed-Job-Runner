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


