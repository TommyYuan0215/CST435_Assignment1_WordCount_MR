FROM python:3.11-slim
LABEL name="wordcount-mapreduce-client-grpc"

# Set working directory on docker container
WORKDIR /app

# Copy proto files and client
COPY proto/ /app/proto/
COPY client/ /app/client/

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Define the command to run the worker (server) when the container starts
CMD ["python", "-m", "client.client"]