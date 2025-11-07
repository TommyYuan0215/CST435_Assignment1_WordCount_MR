FROM python:3.11-slim
LABEL name="wordcount-mapreduce-worker"

# Set working directory on docker container
WORKDIR /app

# Copy all necessary files into the container
COPY proto/ /app/proto/
COPY server/ /app/server/

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# The worker server will always run on port 50051 inside the container
EXPOSE 50051

# Define the command to run the worker (server) when the container starts
CMD ["python", "-m", "server.worker"]