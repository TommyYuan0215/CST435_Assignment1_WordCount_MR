# REST MapReduce Word Count Application

## Prepared by:

- Tan Jun Lin (160989)
- Peh Jia Jin (161059)
- Ooi Tze Shen (165229)

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Running Locally with Docker Compose](#running-locally-with-docker-compose)
5. [Configuration](#configuration)
6. [API Endpoints](#api-endpoints)
7. [Project Structure](#project-structure)
8. [Notes](#notes)
9. [Troubleshooting](#troubleshooting)

---

## Project Overview

This project implements a **MapReduce Word Count application** using **REST API** communication between workers and client. The application is containerized with Docker and orchestrated using Docker Compose.

- The **workers** perform the Map and Reduce tasks via REST endpoints
- The **client** orchestrates the tasks, distributes work, and aggregates the final results
- The number of workers is configurable using the `NUM_WORKERS` environment variable
- Communication uses HTTP POST requests with JSON payloads

---

## Architecture

### Components

1. **Worker Service** (`server/worker.py`)
   - Flask-based REST API server
   - Exposes `/map` and `/reduce` endpoints
   - Processes text chunks and aggregates word counts
   - Runs on port 5000 (internal container port)

2. **Client Service** (`client/client.py`)
   - Coordinates the MapReduce job
   - Splits input data and distributes to workers
   - Collects and aggregates results from workers
   - Uses multi-threading for parallel task execution

### Communication Flow

1. **Map Phase**: Client splits input file into chunks and sends each chunk to a worker via POST to `/map`
2. **Shuffle Phase**: Client groups intermediate results by word (key)
3. **Reduce Phase**: Client sends grouped data to workers via POST to `/reduce` for aggregation
4. **Final Result**: Client collects and displays the final word counts

---

## Prerequisites

- **Docker** installed on your system
  - On **Linux**, you can use **Docker CE** (Community Edition) or **Docker Desktop**
  - On **Windows**, it is recommended to use **Docker Desktop**
  - On **macOS**, use **Docker Desktop**

- **Docker Compose** installed (usually included with Docker Desktop or can be installed separately on Linux)

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

> **Note**: Adjust the number `2` to the desired number of workers (1–6).

### Running in Detached Mode

To run in the background:

```bash
NUM_WORKERS=2 docker compose up --build -d
```

### Viewing Logs

To view logs from all services:

```bash
docker compose logs -f
```

To view logs from a specific service:

```bash
docker compose logs -f client
docker compose logs -f worker1
```

### Stopping the Application

```bash
docker compose down
```

To also remove volumes:

```bash
docker compose down -v
```

---

## Configuration

### Environment Variables

- **`NUM_WORKERS`** (default: 2)
  - Sets the number of workers used by the client
  - Supported range: 1–6
  - The client will use workers `worker1` through `worker<NUM_WORKERS>`

- **`WORKER_ID`** (per worker)
  - Automatically assigned to each worker (1–6)
  - Used for logging and identification

### Ports

- **Worker Ports** (mapped to host):
  - `worker1`: 5001:5000
  - `worker2`: 5002:5000
  - `worker3`: 5003:5000
  - `worker4`: 5004:5000
  - `worker5`: 5005:5000
  - `worker6`: 5006:5000

- **Internal Communication**:
  - All workers listen on port 5000 inside their containers
  - Client connects using Docker service names (`worker1`, `worker2`, etc.) on port 5000

---

## API Endpoints

### Worker Endpoints

#### POST `/map`

Processes a text chunk and returns word counts.

**Request Body**:
```json
{
  "chunk": "text content to process..."
}
```

**Response**:
```json
{
  "word1": 2,
  "word2": 5,
  "word3": 1
}
```

#### POST `/reduce`

Aggregates word counts from multiple map results.

**Request Body**:
```json
{
  "counts": [
    {"word1": 2, "word2": 1},
    {"word1": 3, "word3": 1}
  ]
}
```

**Response**:
```json
{
  "word1": 5,
  "word2": 1,
  "word3": 1
}
```

---

## Project Structure

```
rest/
├── client/
│   ├── client.py          # Main client application
│   ├── Dockerfile         # Client container definition
│   └── testfile.txt       # Input test file
├── server/
│   ├── worker.py          # Worker REST API server
│   └── Dockerfile         # Worker container definition
├── docker-compose.yml     # Docker Compose configuration
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

### Dependencies

- **Flask** (3.0.0): Web framework for worker REST API
- **requests** (2.31.0): HTTP client library for client-to-worker communication

---

## Notes

- The default number of workers is **2** if `NUM_WORKERS` is not specified
- The maximum number of workers is limited to **6** (as defined in `docker-compose.yml`)
- Workers communicate using Docker service names, not `localhost`
- The input file `testfile.txt` should be placed in the `client/` directory
- All text is converted to lowercase before processing
- Only alphanumeric characters are considered for word tokens (punctuation is stripped)
- Docker Compose is recommended for local testing and development
- The application processes text files approximately 5MB in size for testing purposes

---

## Troubleshooting

### File Not Found Error

**Error**: `Error: The input file 'testfile.txt' was not found.`

**Solution**: Ensure `testfile.txt` exists in the `client/` directory. The Dockerfile copies it to the container.

### Connection Refused Errors

**Error**: `Connection refused` when client tries to reach workers

**Solution**: 
- Ensure all worker services are running: `docker compose ps`
- Check that the client is using Docker service names (`worker1`, `worker2`) not `localhost`
- Verify network connectivity: `docker compose logs worker1`

### Empty Results

**Error**: Reduce phase returns empty results or JSON parsing errors

**Solution**:
- Check worker logs for errors: `docker compose logs worker1`
- Verify the reduce endpoint is receiving data in the correct format
- Ensure all workers completed the map phase successfully

### Port Conflicts

**Error**: Port already in use

**Solution**: 
- Check if ports 5001-5006 are already in use
- Modify port mappings in `docker-compose.yml` if needed
- Stop other services using these ports

### Rebuilding Containers

If you make changes to the code, rebuild the containers:

```bash
docker compose up --build
```

Or rebuild without cache:

```bash
docker compose build --no-cache
docker compose up
```

---

## References

- Docker: [https://www.docker.com/](https://www.docker.com/)
- Docker Compose: [https://docs.docker.com/compose/](https://docs.docker.com/compose/)
- Flask: [https://flask.palletsprojects.com/](https://flask.palletsprojects.com/)
- REST API: [https://restfulapi.net/](https://restfulapi.net/)
