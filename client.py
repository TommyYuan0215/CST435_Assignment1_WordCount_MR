import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc
from collections import defaultdict
import os
import time

# --- Configuration ---
# Addresses of workers (service names + internal port in Docker network)
WORKER_ADDRESSES = [
    'worker1:50051',  
    'worker2:50051',  
    'worker3:50051',  
]
NUM_WORKERS = len(WORKER_ADDRESSES)
INPUT_FILE_NAME = "test1.txt" 

def read_input_file(filename):
    """Reads the entire content of the input file."""
    if not os.path.exists(filename):
        raise FileNotFoundError(f"Error: The input file '{filename}' was not found in the current directory.")
        
    print(f"Reading input data from: {filename}")
    with open(filename, 'r', encoding='utf-8') as f:
        return f.read()

def split_input_data(data, num_chunks):
    """Splits the input text roughly equally into the required number of chunks."""
    chunk_size = len(data) // num_chunks
    chunks = []
    
    for i in range(num_chunks):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i < num_chunks - 1 else len(data)
        chunks.append(data[start:end])
        
    return chunks

def run_map_phase(chunks):
    """Initiates MapTasks on the workers and collects all intermediate results."""
    all_intermediate_data = []
    
    stubs = [
        mapreduce_pb2_grpc.MapReduceServiceStub(grpc.insecure_channel(addr))
        for addr in WORKER_ADDRESSES
    ]
    
    print(f"\n--- Starting Map Phase on {NUM_WORKERS} Workers ---")
    
    for i, chunk in enumerate(chunks):
        worker_index = i % NUM_WORKERS  # Round-robin distribution
        stub = stubs[worker_index]
        worker_addr = WORKER_ADDRESSES[worker_index]
        
        print(f"  -> Sending Chunk {i+1} to Worker at {worker_addr}...")
        
        try:
            map_request = mapreduce_pb2.MapRequest(input_data=chunk)
            map_response = stub.MapTask(map_request, timeout=10) 
            
            if map_response and map_response.mapped:
                all_intermediate_data.extend(map_response.mapped)
                print(f"  <- Received {len(map_response.mapped)} results from {worker_addr}.")
            
        except grpc.RpcError as e:
            print(f"!!! Error calling MapTask on {worker_addr}: {e.details()}")
            
    return all_intermediate_data


def run_reduce_phase(intermediate_data):
    """Performs the shuffle, then initiates ReduceTasks."""
    
    # 1. SHUFFLE / GROUPING (Master groups the data by key)
    grouped_data = defaultdict(list)
    for item in intermediate_data:
        try:
            key, value = item.split(':', 1)
            grouped_data[key].append(item)
        except ValueError:
            pass
            
    unique_keys = list(grouped_data.keys())
    print(f"\n--- Shuffle Phase Complete. Found {len(unique_keys)} unique words. ---")
    
    
    # 2. REDUCE
    stubs = [
        mapreduce_pb2_grpc.MapReduceServiceStub(grpc.insecure_channel(addr))
        for addr in WORKER_ADDRESSES
    ]
    
    final_results = {}
    
    print(f"\n--- Starting Reduce Phase on {NUM_WORKERS} Workers ---")
    
    for i, key in enumerate(unique_keys):
        worker_index = i % NUM_WORKERS 
        stub = stubs[worker_index]
        worker_addr = WORKER_ADDRESSES[worker_index]
        
        print(f"  -> Sending ReduceTask for key '{key}' to Worker at {worker_addr}...")
        
        try:
            reduce_request = mapreduce_pb2.ReduceRequest(mapped_data=grouped_data[key])
            reduce_response = stub.ReduceTask(reduce_request, timeout=10)
            
            if reduce_response and reduce_response.result:
                final_results[key] = reduce_response.result.strip()
            
        except grpc.RpcError as e:
            print(f"!!! Error calling ReduceTask on {worker_addr}: {e.details()}")
            
    return final_results


def run_mapreduce():
    # Initialize start_time to ensure it exists for the finally block
    start_time = 0
    try:
        # --- Start Timer ---
        start_time = time.time()
        print(f"\n[TIMER] MapReduce Job started at {time.ctime(start_time)}")
        # -------------------

        # Step A: Read the file content
        input_data = read_input_file(INPUT_FILE_NAME)
        
        # 1. Split Data
        chunks = split_input_data(input_data, NUM_WORKERS)
        print(f"Input text split into {len(chunks)} chunks.")
        
        # 2. Map Phase
        intermediate_data = run_map_phase(chunks)
        
        # 3. Reduce Phase
        final_counts = run_reduce_phase(intermediate_data)
        
        # 4. Final Output
        print("\n==================================")
        print("       FINAL WORD COUNTS        ")
        print("==================================")
        
        parsed_output = {}
        for result_str in final_counts.values():
            for line in result_str.split('\n'):
                if line:
                    try:
                        key, count = line.split(':')
                        parsed_output[key] = int(count)
                    except ValueError:
                        pass
        
        # Sort and print
        for key, count in sorted(parsed_output.items(), key=lambda item: item[1], reverse=True):
            print(f"{key}: {count}")
        print("==================================")

    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # --- Stop Timer and Log Duration ---
        end_time = time.time()
        total_duration = end_time - start_time
        
        print(f"\n[TIMER] MapReduce Job finished at {time.ctime(end_time)}")
        print(f"[TIMER] Total Execution Time: {total_duration:.4f} seconds")
        # -----------------------------------


if __name__ == '__main__':
    if not WORKER_ADDRESSES:
        print("ERROR: Please define at least one worker address in WORKER_ADDRESSES.")
    else:
        run_mapreduce()