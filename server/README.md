# Server Skeleton

The coordinator retrieves a GitHub repository, builds one image tagged with the
job ID, pushes it to GHCR, and dispatches a `JobSpec` to one configured worker.

The worker pulls the image, runs one container, streams stdout/stderr through a
`LogSink`, waits for completion, and removes the container. Worker container
operations never run in the coordinator service.

## Run

Run these commands from `server/` in separate terminals. Each process needs
access to its own Docker daemon, configured through the standard Docker
environment variables when necessary.

```sh
# Worker: set READ_ONLY_GHCR_TOKEN in its environment first.
go run ./cmd/worker -addr :8081

# Coordinator: set READ_WRITE_GHCR_TOKEN in its environment first.
go run ./cmd/coordinator -addr :3000 -worker-url http://localhost:8081
```

Tokens must grant access to `ghcr.io/cnam04/distributed-job-runner`, the existing
registry repository. The server no longer reads a working-directory-relative
`.env` file; export credentials before starting each process. For a remote
worker, change `-worker-url` to its reachable HTTP address.

```sh
curl -i http://localhost:3000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"repoUrl":"https://github.com/cnam04/test_repo","dockerfilePath":"Dockerfile"}'

curl http://localhost:3000/jobs/<job-id>
```

`POST /jobs` runs synchronously and returns `201` with the completed job, or
`500` with the failed job and an `error` message. The response includes a
`Location: /jobs/<job-id>` header. Invalid submissions return `400`.
`GET /jobs/{id}` returns the current job or `404`.
As soon as a job is stored, the coordinator logs `Job queued: id=<job-id>`;
use that ID from another terminal to query the job while the POST is running.

`POST /jobs/run` on the worker accepts only `jobId` and `imageRef`. It responds
with `204` after execution and cleanup succeed, or `500` on failure. This keeps
completion reporting on the existing request instead of introducing callbacks.
Client disconnects and process shutdown cancel execution; container cleanup
uses a separate, bounded context so cancellation does not leave it running.

## Test

```sh
go test ./...
go test -race ./...
go vet ./...
go build -o bin/coordinator ./cmd/coordinator
go build -o bin/worker ./cmd/worker
```

Tests use stdlib `testing`, fakes, and `httptest` for orchestration, HTTP dispatch,
repository handling, and the Docker SDK. Local Docker integration tests skip
when Docker is unavailable. Runnable-container tests use an already-installed
`python:3.13-slim` image and skip if it is missing; they do not download it.
Registry integration tests additionally require `RUN_GHCR_TESTS=1` and both
token variables. They push and delete real test images in GHCR.

## Deliberate Limits

- Job state is in-memory, unbounded, and lost on restart.
- One worker and one container per job; no scheduler, chunks, retries, or queue.
- Logs go to the worker console, not the coordinator or a database.
- Images remain on their Docker hosts and in GHCR; image retention is not implemented.
- Source archives may contain regular files and directories, not symlinks or special files.
- Requests remain open for the full job; no asynchronous submission protocol.
- No inter-server authentication or sandboxing: use only trusted repositories
  on trusted networks. Untrusted Dockerfiles require an isolated, rootless build host.
