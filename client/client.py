import grpc
from proto import mapreduce_pb2, mapreduce_pb2_grpc
from collections import defaultdict
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
NUM_WORKERS = int(os.environ.get('NUM_WORKERS', '2'))
WORKER_ADDRESSES = [f'worker{i+1}:50051' for i in range(NUM_WORKERS)]
INPUT_FILE_NAME = "test1.txt"

print(f"\n{'='*60}")
print(f"MapReduce Configuration: {NUM_WORKERS} Worker(s)")
print(f"{'='*60}")

def read_input_file(filename):
    """Read input file."""
    filepath = os.path.join('client', filename)
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Error: The input file '{filename}' was not found in the client directory.")
    print(f"Reading input data from: {filepath}")
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()

def split_input_data(data, num_chunks):
    """Split input data into chunks for workers."""
    chunk_size = len(data) // num_chunks
    chunks = []
    for i in range(num_chunks):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i < num_chunks - 1 else len(data)
        chunks.append(data[start:end])
    return chunks

def run_map_phase(chunks):
    """Execute Map phase - send chunks to workers and collect results."""
    stubs = [mapreduce_pb2_grpc.MapReduceServiceStub(grpc.insecure_channel(addr)) for addr in WORKER_ADDRESSES]
    all_intermediate_data = []
    
    print(f"\n[Map Phase] Starting on {NUM_WORKERS} worker(s)...")
    start_time = time.perf_counter()

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {}
        for i, chunk in enumerate(chunks):
            worker_index = i % NUM_WORKERS
            future = executor.submit(stubs[worker_index].MapTask, mapreduce_pb2.MapRequest(input_data=chunk), 10)
            futures[future] = worker_index

        for future in as_completed(futures):
            worker_index = futures[future]
            try:
                response = future.result()
                if response and response.mapped:
                    all_intermediate_data.extend(response.mapped)
            except grpc.RpcError as e:
                worker_addr = WORKER_ADDRESSES[worker_index]
                print(f"!!! Error calling MapTask on {worker_addr}: {e.details()}")
            except Exception as e:
                print(f"!!! Unexpected error: {e}")

    elapsed = time.perf_counter() - start_time
    print(f"[Map Phase] Complete - Time: {elapsed:.6f}s, Results: {len(all_intermediate_data)} pairs")
    return all_intermediate_data, elapsed

def run_reduce_phase(intermediate_data):
    """Execute Reduce phase - shuffle data and send to workers."""
    # Shuffle: Group intermediate data by key
    shuffle_start = time.perf_counter()
    grouped_data = defaultdict(list)
    for item in intermediate_data:
        try:
            key, _ = item.split(':', 1)
            grouped_data[key].append(item)
        except ValueError:
            pass
    
    unique_keys = list(grouped_data.keys())
    shuffle_elapsed = time.perf_counter() - shuffle_start
    print(f"[Shuffle Phase] Complete - Time: {shuffle_elapsed:.6f}s, Unique keys: {len(unique_keys)}")
    
    # Reduce: Send grouped data to workers
    stubs = [mapreduce_pb2_grpc.MapReduceServiceStub(grpc.insecure_channel(addr)) for addr in WORKER_ADDRESSES]
    final_results = {}
    
    print(f"[Reduce Phase] Starting on {NUM_WORKERS} worker(s)...")
    reduce_start = time.perf_counter()
    
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {}
        for i, key in enumerate(unique_keys):
            worker_index = i % NUM_WORKERS
            future = executor.submit(
                stubs[worker_index].ReduceTask,
                mapreduce_pb2.ReduceRequest(mapped_data=grouped_data[key]),
                10
            )
            futures[future] = key

        for future in as_completed(futures):
            key = futures[future]
            try:
                response = future.result()
                if response and response.result:
                    final_results[key] = response.result
            except grpc.RpcError as e:
                print(f"!!! Error calling ReduceTask: {e.details()}")
            except Exception as e:
                print(f"!!! Unexpected error: {e}")

    reduce_elapsed = time.perf_counter() - reduce_start
    print(f"[Reduce Phase] Complete - Time: {reduce_elapsed:.6f}s, Results: {len(final_results)} keys")
    return final_results, reduce_elapsed, shuffle_elapsed

def parse_and_display_results(final_results):
    """Parse and display word counts from server results."""
    all_word_counts = {}
    for result_str in final_results.values():
        for line in result_str.split('\n'):
            line = line.strip()
            if line:
                try:
                    key, count = line.split(':', 1)
                    all_word_counts[key] = int(count)
                except ValueError:
                    pass
    
    sorted_words = sorted(all_word_counts.items(), key=lambda item: item[1], reverse=True)
    for key, count in sorted_words:
        print(f"  {key}: {count}")

def run_mapreduce():
    """Main coordinator - orchestrates MapReduce job."""
    start_time = time.perf_counter()
    map_wall = reduce_wall = shuffle_wall = 0.0
    
    try:
        # Read and split input
        input_data = read_input_file(INPUT_FILE_NAME)
        chunks = split_input_data(input_data, NUM_WORKERS)
        print(f"[Setup] Input split into {len(chunks)} chunk(s)")
        
        # Map phase
        intermediate_data, map_wall = run_map_phase(chunks)
        
        # Reduce phase
        final_results, reduce_wall, shuffle_wall = run_reduce_phase(intermediate_data)
        
        # Display results
        print("\n" + "="*60)
        print("FINAL WORD COUNTS")
        print("="*60)
        parse_and_display_results(final_results)
        print("="*60)
        
    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Performance summary
        end_time = time.perf_counter()
        total_duration = end_time - start_time
        overhead = total_duration - map_wall - shuffle_wall - reduce_wall
        
        print(f"\n{'='*60}")
        print(f"PERFORMANCE SUMMARY - {NUM_WORKERS} Worker(s)")
        print(f"{'='*60}")
        print(f"Total Execution Time:     {total_duration:.6f} seconds")
        print(f"  Map Phase:             {map_wall:.6f} seconds")
        print(f"  Shuffle Phase:         {shuffle_wall:.6f} seconds")
        print(f"  Reduce Phase:          {reduce_wall:.6f} seconds")
        print(f"  Other (overhead):      {overhead:.6f} seconds")
        print(f"{'='*60}\n")


if __name__ == '__main__':
    if not WORKER_ADDRESSES:
        print("ERROR: Please define at least one worker address in WORKER_ADDRESSES.")
    else:
        run_mapreduce()