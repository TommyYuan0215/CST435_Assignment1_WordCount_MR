import grpc
import time
from concurrent import futures
import mapreduce_pb2
import mapreduce_pb2_grpc

# --- Configuration ---
# The port this worker will listen on. 
# Remember to adjust this if running multiple workers (e.g., 50051, 50052, etc.)
PORT = '50051' 

class MapReduceServicer(mapreduce_pb2_grpc.MapReduceServiceServicer):
    
    def MapTask(self, request, context):
        """Processes a chunk of text and emits (word:1) pairs."""
        print(f"Worker received MapTask for chunk: '{request.input_data[:30]}...'")
        
        text = request.input_data.lower()
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

        print(f"MapTask completed. {len(intermediate_results)} intermediate results generated.")
        
        # Return the intermediate data directly to the client (Master)
        return mapreduce_pb2.MapResponse(mapped=intermediate_results)

    def ReduceTask(self, request, context):
        """Aggregates all "key:value" strings and produces a final count."""
        
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
        # Assuming the reducer should return the combined result of its task
        final_results = []
        for key, count in counts.items():
            final_results.append(f"{key}:{count}")

        final_output = "\n".join(final_results)
        print(f"ReduceTask completed. Final output:\n{final_output}")
        
        return mapreduce_pb2.ReduceResponse(result=final_output)


def serve():
    # Set up the server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_MapReduceServiceServicer_to_server(
        MapReduceServicer(), server
    )
    
    # Start the server on the defined port
    server.add_insecure_port(f'[::]:{PORT}')
    server.start()
    print(f"MapReduce Worker Server running on port {PORT}...")
    
    try:
        # Keep the server running indefinitely
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()