FROM python:3.11-slim
LABEL name="wordcount-mapreduce-client"

# Set working directory on docker container
WORKDIR /app

# Copy proto files and client
COPY proto/ /app/proto/
COPY client/ /app/client/

# Install gRPC and protobuf dependencies
RUN pip install --no-cache-dir grpcio grpcio-tools

# Define the command to run the worker (server) when the container starts
CMD ["python", "-m", "client.client"]