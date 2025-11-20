import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

# Configuration
NUM_WORKERS = int(os.environ.get('NUM_WORKERS', '2'))
WORKER_ADDRESSES = [f"http://localhost:{5001 + i}" for i in range(NUM_WORKERS)]
INPUT_FILE_NAME = "testfile.txt"

print(f"\n{'='*60}")
print(f"REST MapReduce Configuration: {NUM_WORKERS} Worker(s)")
print(f"{'='*60}")

def read_input_file(filename):
    """Read input file."""
    if not os.path.exists(filename):
        raise FileNotFoundError(f"Error: The input file '{filename}' was not found.")
    print(f"Reading input data from: {filename}")
    with open(filename, 'r', encoding='utf-8') as f:
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
    """Execute Map phase - send chunks to REST workers and collect results."""
    all_intermediate_data = []
    
    print(f"\n[Map Phase] Starting on {NUM_WORKERS} worker(s)...")
    start_time = time.perf_counter()

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {}
        for i, chunk in enumerate(chunks):
            worker_index = i % NUM_WORKERS
            worker_url = WORKER_ADDRESSES[worker_index] + "/map"
            future = executor.submit(lambda url, c: requests.post(url, json={"chunk": c}).json(), worker_url, chunk)
            futures[future] = worker_index

        for future in as_completed(futures):
            worker_index = futures[future]
            try:
                response = future.result()
                if response:
                    # Each worker returns a dict of word counts
                    all_intermediate_data.append(response)
            except Exception as e:
                worker_addr = WORKER_ADDRESSES[worker_index]
                print(f"!!! Error calling MapTask on {worker_addr}: {e}")

    elapsed = time.perf_counter() - start_time
    print(f"[Map Phase] Complete - Time: {elapsed:.6f}s, Results collected: {len(all_intermediate_data)}")
    return all_intermediate_data, elapsed

def run_reduce_phase(intermediate_data):
    """Execute Reduce phase - shuffle data and send to REST workers."""
    # Shuffle: group data by word
    shuffle_start = time.perf_counter()
    grouped_data = defaultdict(list)
    for data_dict in intermediate_data:
        for word, count in data_dict.items():
            grouped_data[word].append(count)
    unique_keys = list(grouped_data.keys())
    shuffle_elapsed = time.perf_counter() - shuffle_start
    print(f"[Shuffle Phase] Complete - Time: {shuffle_elapsed:.6f}s, Unique keys: {len(unique_keys)}")

    # Reduce: send grouped data to workers
    final_results = {}
    print(f"[Reduce Phase] Starting on {NUM_WORKERS} worker(s)...")
    reduce_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {}
        for i, key in enumerate(unique_keys):
            worker_index = i % NUM_WORKERS
            worker_url = WORKER_ADDRESSES[worker_index] + "/reduce"
            future = executor.submit(lambda url, k, counts: requests.post(url, json={"counts": counts}).json(), worker_url, key, grouped_data[key])
            futures[future] = key

        for future in as_completed(futures):
            key = futures[future]
            try:
                response = future.result()
                if response is not None:
                    # Each response is a single count for that word
                    final_results[key] = sum(grouped_data[key])
            except Exception as e:
                print(f"!!! Error calling ReduceTask for key '{key}': {e}")

    reduce_elapsed = time.perf_counter() - reduce_start
    print(f"[Reduce Phase] Complete - Time: {reduce_elapsed:.6f}s, Results: {len(final_results)} keys")
    return final_results, reduce_elapsed, shuffle_elapsed

def parse_and_display_results(final_results):
    """Display final word counts."""
    sorted_words = sorted(final_results.items(), key=lambda item: item[1], reverse=True)
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
