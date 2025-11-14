# CST435_ASSIGNMENT1_WordCount_MR

## Prepared by:

- Tan Jun Lin (160989)
- Peh Jia Jin (161059)
- Ooi Tze Shen (165229)

## Instruction to use this repository

### Prerequisites

- Docker and Docker Compose installed on your system

### Running the MapReduce Word Count Application

The application supports dynamic worker configuration. You can specify the number of workers using the `NUM_WORKERS` environment variable.

#### Windows (PowerShell)

```powershell
$env:NUM_WORKERS=3; docker compose up --build
```

#### Linux/Mac

```bash
NUM_WORKERS=3 docker compose up --build
```

### Notes

- The default number of workers is 2 if `NUM_WORKERS` is not specified.
- Supported worker count: 1 to 6 workers.
- The client will automatically use only the first `NUM_WORKERS` workers.
- All worker containers will start, but only the specified number will be used for processing.
- If want to restart the client container, can just click on start button in docker desktop, or using **docker start mr_client -a** to execute again.
