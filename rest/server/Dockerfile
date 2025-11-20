FROM python:3.11-slim
LABEL name="wordcount-mapreduce-worker-rest"

# Set working directory inside the container
WORKDIR /app

# Copy REST worker code
COPY server/ /app/server/

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Expose the REST worker port (default 5000, can override with env)
EXPOSE 5000

# Define the command to run the REST worker
CMD ["python", "-m", "server.worker"]
