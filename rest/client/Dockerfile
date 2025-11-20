FROM python:3.11-slim
LABEL name="wordcount-mapreduce-client-rest"

# Set working directory on docker container
WORKDIR /app

# Copy REST client code
COPY client/ /app/client/

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Define the command to run the REST client
CMD ["python", "-m", "client.client"]

