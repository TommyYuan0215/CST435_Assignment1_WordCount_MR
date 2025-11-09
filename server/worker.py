import os
import grpc
import time
from concurrent import futures
from proto import mapreduce_pb2, mapreduce_pb2_grpc

# --- Configuration ---
# Get worker ID from environment variable, default to 1 if not set
WORKER_ID = int(os.environ.get('WORKER_ID', 1))
# Base port is 50051, each worker gets assigned sequential ports
PORT = str(50051)  # Within container, always use 50051

class MapReduceServicer(mapreduce_pb2_grpc.MapReduceServiceServicer):
    def __init__(self):
        self.worker_id = WORKER_ID
        
    def MapTask(self, request, context):
        """Processes a chunk of text and emits (word:1) pairs."""
        wall_start = time.perf_counter()
        compute_start = time.perf_counter()

        print(f"Worker {self.worker_id} received MapTask for chunk: '{(request.input_data or '')[:30]}...'")
        
        text = (request.input_data or "").lower()
        # Simple tokenization: split by whitespace and remove non-alphanumeric chars
        words = [
            ''.join(filter(str.isalnum, word)) 
            for word in text.split()
        ]
        
        intermediate_results = []
        for word in words:
            if word:
                # Emit result as a string: "word:1"
                intermediate_results.append(f"{word}:1")

        compute_end = time.perf_counter()
        wall_end = time.perf_counter()
        compute_dur = compute_end - compute_start
        wall_dur = wall_end - wall_start

        print(f"MapTask completed. {len(intermediate_results)} intermediate results generated. "
              f"compute={compute_dur:.6f}s wall={wall_dur:.6f}s")
        
        # Append timing metadata as a special entry so client can parse it
        timing_entry = f"__TIMING__ compute_time={compute_dur:.6f} wall_time={wall_dur:.6f}"
        intermediate_results.append(timing_entry)

        # Return the intermediate data directly to the client (Master)
        return mapreduce_pb2.MapResponse(mapped=intermediate_results)

    def ReduceTask(self, request, context):
        """Aggregates all "key:value" strings and produces a final count."""
        wall_start = time.perf_counter()
        compute_start = time.perf_counter()

        print(f"Worker {self.worker_id} received ReduceTask")
        
        # 1. Group the mapped data for aggregation
        counts = {}
        for item in request.mapped_data:  # e.g., ["apple:1", "apple:1", "banana:1"]
            try:
                # Split and aggregate the count
                key, value_str = item.split(':', 1)
                value = int(value_str)
                counts[key] = counts.get(key, 0) + value
            except ValueError:
                print(f"Warning: Skipping invalid intermediate pair: {item}")

        # 2. Format the final result string
        final_results = []
        for key, count in counts.items():
            final_results.append(f"{key}:{count}")

        compute_end = time.perf_counter()
        wall_end = time.perf_counter()
        compute_dur = compute_end - compute_start
        wall_dur = wall_end - wall_start

        timing_line = f"__TIMING__ compute_time={compute_dur:.6f} wall_time={wall_dur:.6f}"
        final_output = "\n".join(final_results + [timing_line])
        print(f"ReduceTask completed. Final output lines: {len(final_results)} compute={compute_dur:.6f}s wall={wall_dur:.6f}s")
        
        return mapreduce_pb2.ReduceResponse(result=final_output)


def serve():
    # Set up the server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = MapReduceServicer()
    mapreduce_pb2_grpc.add_MapReduceServiceServicer_to_server(servicer, server)
    
    # Start the server on the defined port
    server.add_insecure_port(f'[::]:{PORT}')
    server.start()
    print(f"MapReduce Worker {WORKER_ID} running on port {PORT}...")
    
    try:
        # Keep the server running indefinitely
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()