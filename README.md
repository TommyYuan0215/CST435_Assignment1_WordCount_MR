# CST435 MapReduce Word Count

Brief overview of a distributed word-count assignment implemented twice:

- `grpc/` – Python gRPC services packaged for Docker Compose and Kubernetes.
- `rest/` – Flask-based REST services packaged for Docker Compose.

Refer to the detailed READMEs inside each folder for architecture diagrams, API docs, and troubleshooting tips. This top-level guide simply explains how everything fits together and how to get started.

---

## Prepared by

- Tan Jun Lin (160989)
- Peh Jia Jin (161059)
- Ooi Tze Shen (165229)

---

## Repository Layout

```
CST435_Assignment1_WordCount_MR/
├── grpc/        # gRPC implementation (client, workers, K8s manifests, proto)
├── rest/        # REST implementation (client, workers)
└── README.md    # You are here
```

Each implementation has its own `docker-compose.yml`, Python sources, Dockerfiles, requirements, and README.

---

## Prerequisites

- Docker Engine or Docker Desktop (Compose v2 included).
- Python 3.11+ **only** if you plan to run the services directly, otherwise Docker handles dependencies.
- (Optional) kubectl + a Kubernetes cluster (Minikube, Docker Desktop, etc.) for the gRPC deployment in `grpc/k8s`.

---

## Quick Start

Clone or open this repo, then choose either stack:

### Remote Worker (gRPC) Quick Reference

If you need to run a gRPC worker on a remote host (outside Docker Compose), pull and start the published worker image:

```bash
docker pull wordcount-mapreduce-worker-grpc
docker run -d --name mr_remote_worker \
  -p 50051:50051 \
  -e WORKER_ID=5 \
  wordcount-mapreduce-worker-grpc
```

- Change `WORKER_ID` and the host port mapping as needed.
- Repeat per remote host to spin up additional workers.

From the machine running the gRPC client, point `NUM_WORKERS` and the worker IP overrides to the remote workers. For example:

```bash
cd /home/junlin/CST435_Assignment1_WordCount_MR/grpc
W1_IP=<ip-address> W2_IP=<ip-address> NUM_WORKERS=2 \
  docker compose up --build \
  --scale worker1=0 --scale worker2=0 --scale worker3=0 \
  --scale worker4=0 --scale worker5=0 --scale worker6=0
```

- The `--scale ...=0` flags disable the local worker containers so the client only talks to the remote instances.
- Export additional `Wn_IP` variables if you need more than two remote workers.

To publish updated images to a registry for remote use:

```bash
docker tag <localimage>:latest <repositoryimage>:latest
docker push <repositoryimage>:latest
```

### REST (Flask + HTTP)

```bash
cd ~/CST435_Assignment1_WordCount_MR/rest
NUM_WORKERS=2 docker compose up --build
```

- Adjust `NUM_WORKERS` (1–6) to control how many worker services the client targets.
- Inspect logs with `docker compose logs -f client`.
- See `rest/README.md` for the REST API contract (`/map`, `/reduce`) and troubleshooting flow.

### gRPC (Python gRPC)

```bash
cd ~/CST435_Assignment1_WordCount_MR/grpc
NUM_WORKERS=2 docker compose up --build
```

- Workers expose gRPC on port `50051` (remapped per container).
- `grpc/k8s/wordcount-mr.yaml` spins up 6 worker Deployments plus a client Job; follow `grpc/README.md` for the full Kubernetes workflow (config map, reruns, log collection).

---

## Configuration Highlights

- `NUM_WORKERS`: client-side environment variable in both stacks (default 2, max 6).
- `WORKER_ID`: injected per worker container for logging.
- Input text (`testfile.txt`) lives under each `client/` directory and is copied into the image during build.

---

## Where to Go Next

- `rest/README.md`: REST architecture, endpoints, project structure, troubleshooting.
- `grpc/README.md`: Docker + Kubernetes instructions, ConfigMap tuning, log commands.
- `grpc/proto/wordcount.proto`: RPC contract shared by client and workers.

This top-level README intentionally stays short; dive into the respective subdirectories for complete details and scripts.

