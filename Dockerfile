FROM python:3.11-slim

# Set working directory on docker container
WORKDIR /app

# Copy all necessary files into the container
COPY . /app/

# Install gRPC and protobuf dependencies
RUN pip install grpcio grpcio-tools

# The worker server will always run on port 50051 inside the container
EXPOSE 50051

# Define the command to run the worker (server) when the container starts
CMD ["python", "worker.py"]