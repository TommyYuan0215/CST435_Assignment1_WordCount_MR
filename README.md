# CST435_ASSIGNMENT1_WordCount_MR

## Prepared by:

- Tan Jun Lin (160989)
- Peh Jia Jin (161059)
- Ooi Tze Shen (165229)

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Prerequisites](#prerequisites)
3. [Running Locally with Docker Compose](#running-locally-with-docker-compose)
4. [Running on Kubernetes](#running-on-kubernetes)
5. [Configuration](#configuration)
6. [Notes](#notes)

---

## Project Overview

This project implements a **MapReduce Word Count application** using Docker and Kubernetes.

- The **workers** perform the Map and Reduce tasks.
- The **client** orchestrates the tasks and aggregates the final results.
- The number of workers is configurable using the `NUM_WORKERS` environment variable.

---

## Prerequisites

- **Docker** installed on your system

  - On **Linux**, you can use **Docker CE** (Community Edition) or **Docker Desktop**
  - On **Windows**, it is recommended to use **Docker Desktop** for full Kubernetes support

- **Docker Compose** installed (usually included with Docker Desktop or can be installed separately on Linux)

- **Kubernetes** enabled (required only if you want to deploy using K8s)
  - On Docker Desktop, you can enable it via the settings panel
  - On Linux, you can use Minikube or Docker Desktop if available

---

## Running Locally with Docker Compose

The application supports dynamic worker configuration. You can specify the number of workers using the `NUM_WORKERS` environment variable.

### Windows (PowerShell)

```powershell
$env:NUM_WORKERS=2; docker compose up --build
```

### Linux / Mac

```bash
NUM_WORKERS=2 docker compose up --build
```

> Adjust the number `2` to the desired number of workers (1–6).

---

## Running on Kubernetes

### Step 1: Build Docker Images

```bash
docker build -t wordcount-mapreduce-worker:latest -f server/Dockerfile .
docker build -t wordcount-mapreduce-client:latest -f client/Dockerfile .
```

---

### Step 2: Deploy WordCount-MR

```bash
kubectl apply -f k8s/wordcount-mr.yaml
```

---

### Step 3: Verify Deployment

```bash
kubectl get all -n wordcount-mr
```

You should see:

- 6 worker pods and their services
- The client job running or completed

---

### Step 4: Check Logs of a Pod

To see the output of the client or any worker pod, use:

```bash
kubectl logs <pod-name> -n wordcount-mr
```

- Replace <pod-name> with the actual pod name you want to check.
- Useful to debug or verify the progress and results of the MapReduce job

---

### Step 5: Configure Number of Workers

The client reads the `NUM_WORKERS` from a ConfigMap inside the `wordcount-mr` namespace. To change the number of workers:

```bash
kubectl edit configmap wordcount-config -n wordcount-mr
```

Update the value:

```yaml
data:
  NUM_WORKERS: "2"
```

- Supported worker count: 1–6
- The client automatically uses the first `NUM_WORKERS` workers; all worker pods will still start

---

### Step 6: Rerun the Client Job

If you want to rerun the client job after changing `NUM_WORKERS`:

```bash
kubectl delete job mr-client-job -n wordcount-mr
kubectl apply -f k8s/wordcount-mr.yaml
```

Or create a one-off rerun:

```bash
kubectl create job --from=job/mr-client-job mr-client-job-rerun -n wordcount-mr
```

---

## Configuration

- `NUM_WORKERS` – sets the number of workers used by the client
- `WORKER_ID` – assigned to each worker via environment variable in the Deployment
- All ports are exposed on **50051** for gRPC communication

---

## Notes

- The default number of workers is **2** if `NUM_WORKERS` is not specified
- The maximum number of workers is limited to 6 only.
- Docker Compose is recommended for local testing.
- This project will be given 2 text file for testing purposes, which including `testfile_30mb.txt` and `testfile_512kb.txt` that already inside the client folder.
- Kubernetes is recommended for more realistic distributed execution and orchestration

---

## References

- Docker: [https://www.docker.com/](https://www.docker.com/)
- Kubernetes: [https://kubernetes.io/](https://kubernetes.io/)
- gRPC: [https://grpc.io/](https://grpc.io/)
