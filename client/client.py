import grpc
from proto import mapreduce_pb2, mapreduce_pb2_grpc
from collections import defaultdict
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

# --- Configuration ---
# Addresses of workers (service names + internal port in Docker network)
WORKER_ADDRESSES = [
    'worker1:50051',  
    'worker2:50051',  
    'worker3:50051',  
    'worker4:50051',
    'worker5:50051',
]
NUM_WORKERS = len(WORKER_ADDRESSES)
INPUT_FILE_NAME = "test1.txt" 

TIMING_RE = re.compile(r"__TIMING__\s+compute_time=([0-9.]+)\s+wall_time=([0-9.]+)")

def read_input_file(filename):
    """Reads the entire content of the input file."""
    # Modify path to look in client directory
    filepath = os.path.join('client', filename)
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Error: The input file '{filename}' was not found in the client directory.")
        
    print(f"Reading input data from: {filepath}")
    with open(filepath, 'r', encoding='utf-8') as f:
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

def _parse_map_response(mapped_list):
    compute = 0.0
    wall = 0.0
    filtered = []
    for entry in mapped_list:
        m = TIMING_RE.match(entry) if isinstance(entry, str) else None
        if m:
            compute += float(m.group(1))
            wall += float(m.group(2))
        else:
            filtered.append(entry)
    return filtered, compute, wall

def _parse_reduce_response(result_str):
    compute = 0.0
    wall = 0.0
    if not result_str:
        return {}, compute, wall
    lines = [l for l in result_str.splitlines() if l.strip()]
    if not lines:
        return {}, compute, wall
    last = lines[-1]
    m = TIMING_RE.match(last)
    result_lines = lines
    if m:
        compute = float(m.group(1))
        wall = float(m.group(2))
        result_lines = lines[:-1]
    parsed = {}
    for line in result_lines:
        try:
            k, v = line.split(':', 1)
            parsed[k] = int(v)
        except ValueError:
            pass
    return parsed, compute, wall

def run_map_phase(chunks):
    """Initiates MapTasks on the workers in parallel and collects all intermediate results."""
    all_intermediate_data = []
    map_compute_sum = 0.0
    map_wall_sum = 0.0

    stubs = [
        mapreduce_pb2_grpc.MapReduceServiceStub(grpc.insecure_channel(addr))
        for addr in WORKER_ADDRESSES
    ]
    
    print(f"\n--- Starting Map Phase on {NUM_WORKERS} Workers (parallel) ---")
    wall_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as exe:
        futures = {}
        for i, chunk in enumerate(chunks):
            worker_index = i % NUM_WORKERS
            stub = stubs[worker_index]
            futures[exe.submit(stub.MapTask, mapreduce_pb2.MapRequest(input_data=chunk), 10)] = worker_index

        for fut in as_completed(futures):
            worker_index = futures[fut]
            worker_addr = WORKER_ADDRESSES[worker_index]
            try:
                map_response = fut.result()
                if map_response and map_response.mapped:
                    filtered, c, w = _parse_map_response(list(map_response.mapped))
                    all_intermediate_data.extend(filtered)
                    map_compute_sum += c
                    map_wall_sum += w
                    print(f"  <- Received {len(filtered)} results from {worker_addr} (compute={c:.6f}s wall={w:.6f}s)")
            except grpc.RpcError as e:
                print(f"!!! Error calling MapTask on {worker_addr}: {e.details()}")
            except Exception as e:
                print(f"!!! Unexpected error waiting for MapTask: {e}")

    wall_elapsed = time.perf_counter() - wall_start
    print(f"Map phase complete. wall_elapsed={wall_elapsed:.6f}s compute_sum={map_compute_sum:.6f}s (sum of workers)")
    return all_intermediate_data, map_compute_sum, wall_elapsed

def run_reduce_phase(intermediate_data):
    """Performs the shuffle, then initiates ReduceTasks in parallel."""
    
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
    
    stubs = [
        mapreduce_pb2_grpc.MapReduceServiceStub(grpc.insecure_channel(addr))
        for addr in WORKER_ADDRESSES
    ]
    
    final_results = {}
    reduce_compute_sum = 0.0
    reduce_wall_sum = 0.0

    print(f"\n--- Starting Reduce Phase on {NUM_WORKERS} Workers (parallel, limited concurrency) ---")
    wall_start = time.perf_counter()
    # limit concurrency to NUM_WORKERS to avoid too many simultaneous RPCs
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as exe:
        futures = {}
        for i, key in enumerate(unique_keys):
            worker_index = i % NUM_WORKERS 
            stub = stubs[worker_index]
            futures[exe.submit(stub.ReduceTask, mapreduce_pb2.ReduceRequest(mapped_data=grouped_data[key]), 10)] = (worker_index, key)

        for fut in as_completed(futures):
            worker_index, key = futures[fut]
            worker_addr = WORKER_ADDRESSES[worker_index]
            try:
                reduce_response = fut.result()
                if reduce_response and reduce_response.result is not None:
                    parsed_map, c, w = _parse_reduce_response(reduce_response.result)
                    reduce_compute_sum += c
                    reduce_wall_sum += w
                    # parsed_map may contain only the single key's result
                    if key in parsed_map:
                        final_results[key] = f"{key}:{parsed_map[key]}"
                    else:
                        # fallback: if reducer returned multiple lines, pick the first matching
                        if parsed_map:
                            # pick matching key if present
                            if key in parsed_map:
                                final_results[key] = f"{key}:{parsed_map[key]}"
                            else:
                                # otherwise store aggregated lines as string
                                final_results[key] = "\n".join([f"{k}:{v}" for k,v in parsed_map.items()])
                    print(f"  <- Received Reduce result for '{key}' from {worker_addr} (compute={c:.6f}s wall={w:.6f}s)")
            except grpc.RpcError as e:
                print(f"!!! Error calling ReduceTask on {worker_addr}: {e.details()}")
            except Exception as e:
                print(f"!!! Unexpected error waiting for ReduceTask: {e}")

    wall_elapsed = time.perf_counter() - wall_start
    print(f"Reduce phase complete. wall_elapsed={wall_elapsed:.6f}s compute_sum={reduce_compute_sum:.6f}s (sum of workers)")
    return final_results, reduce_compute_sum, wall_elapsed

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
        
        # 2. Map Phase (parallel)
        intermediate_data, map_compute_sum, map_wall = run_map_phase(chunks)
        
        # 3. Reduce Phase (parallel)
        final_counts, reduce_compute_sum, reduce_wall = run_reduce_phase(intermediate_data)
        
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
        
        total_compute = map_compute_sum + reduce_compute_sum if 'map_compute_sum' in locals() and 'reduce_compute_sum' in locals() else 0.0
        total_wall_stages = (map_wall if 'map_wall' in locals() else 0.0) + (reduce_wall if 'reduce_wall' in locals() else 0.0)
        print(f"\n[TIMER] MapReduce Job finished at {time.ctime(end_time)}")
        print(f"[TIMER] Total Execution Time (wall clock): {total_duration:.4f} seconds")
        print(f"[TIMER] Total Compute Time (sum of worker compute durations): {total_compute:.6f} seconds")
        print(f"[TIMER] Map wall phase elapsed: {(map_wall if 'map_wall' in locals() else 0.0):.6f}s, Reduce wall phase elapsed: {(reduce_wall if 'reduce_wall' in locals() else 0.0):.6f}s")
        # -----------------------------------


if __name__ == '__main__':
    if not WORKER_ADDRESSES:
        print("ERROR: Please define at least one worker address in WORKER_ADDRESSES.")
    else:
        run_mapreduce()