import os
import grpc
import time
from concurrent import futures
from proto import mapreduce_pb2, mapreduce_pb2_grpc

# Configuration
WORKER_ID = int(os.environ.get('WORKER_ID', 1))
PORT = 50051

class MapReduceServicer(mapreduce_pb2_grpc.MapReduceServiceServicer):
    """MapReduce worker service - handles Map and Reduce tasks."""
    
    def __init__(self):
        self.worker_id = WORKER_ID
    
    def _tokenize_text(self, text):
        """Tokenize text: split by whitespace and filter alphanumeric characters."""
        text = text.lower()
        words = [''.join(filter(str.isalnum, word)) for word in text.split()]
        return [word for word in words if word]
    
    def MapTask(self, request, context):
        """Map phase: tokenize input text and emit (word:1) pairs."""
        start_time = time.perf_counter()
        
        input_text = request.input_data or ""
        print(f"Worker {self.worker_id} received MapTask: '{(input_text[:30])}...'")
        
        # Process: Tokenize and emit key-value pairs
        words = self._tokenize_text(input_text)
        intermediate_results = [f"{word}:1" for word in words]
        
        elapsed = time.perf_counter() - start_time
        print(f"Worker {self.worker_id} MapTask completed: {len(intermediate_results)} pairs in {elapsed:.6f}s")
        
        return mapreduce_pb2.MapResponse(mapped=intermediate_results)
    
    def ReduceTask(self, request, context):
        """Reduce phase: aggregate values for each key."""
        start_time = time.perf_counter()
        
        print(f"Worker {self.worker_id} received ReduceTask")
        
        # Process: Aggregate counts for each key
        counts = {}
        for item in request.mapped_data:
            try:
                key, value_str = item.split(':', 1)
                counts[key] = counts.get(key, 0) + int(value_str)
            except ValueError:
                print(f"Warning: Skipping invalid pair: {item}")
        
        # Format: Create sorted output string
        sorted_results = [f"{key}:{count}" for key, count in sorted(counts.items())]
        result = "\n".join(sorted_results)
        
        elapsed = time.perf_counter() - start_time
        print(f"Worker {self.worker_id} ReduceTask completed: {len(sorted_results)} keys in {elapsed:.6f}s")
        
        return mapreduce_pb2.ReduceResponse(result=result)


def serve():
    """Start the gRPC server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = MapReduceServicer()
    mapreduce_pb2_grpc.add_MapReduceServiceServicer_to_server(servicer, server)
    
    server.add_insecure_port(f'[::]:{PORT}')
    server.start()
    print(f"MapReduce Worker {WORKER_ID} running on port {PORT}...")
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()