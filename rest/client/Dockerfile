FROM python:3.11-slim
LABEL name="wordcount-mapreduce-client-rest"

# Set working directory on docker container
WORKDIR /app

# Copy REST client code
COPY client/ /app/client/

# Copy testfile.txt to app root so client can find it
COPY client/testfile.txt /app/testfile.txt

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Define the command to run the REST client
CMD ["python", "-m", "client.client"]

