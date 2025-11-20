import os
import time
from collections import defaultdict
from flask import Flask, request, jsonify

# Configuration
WORKER_ID = int(os.environ.get('WORKER_ID', 1))
PORT = int(os.environ.get('PORT', 5000))

app = Flask(__name__)

def _tokenize_text(text):
    """Tokenize text: split by whitespace and filter alphanumeric characters."""
    text = text.lower()
    words = [''.join(filter(str.isalnum, word)) for word in text.split()]
    return [word for word in words if word]

@app.route("/map", methods=["POST"])
def map_task():
    """Map phase: tokenize input text and emit (word: count) dictionary."""
    start_time = time.perf_counter()
    
    input_text = request.json.get("chunk", "")
    print(f"Worker {WORKER_ID} received MapTask: '{(input_text[:30])}...'")
    
    # Process: Tokenize and emit key-value pairs
    words = _tokenize_text(input_text)
    intermediate_results = {}
    for word in words:
        intermediate_results[word] = intermediate_results.get(word, 0) + 1
    
    elapsed = time.perf_counter() - start_time
    print(f"Worker {WORKER_ID} MapTask completed: {len(intermediate_results)} unique words in {elapsed:.6f}s")
    
    return jsonify(intermediate_results)

@app.route("/reduce", methods=["POST"])
def reduce_task():
    """Reduce phase: aggregate values for each key."""
    start_time = time.perf_counter()
    print(f"Worker {WORKER_ID} received ReduceTask")

    counts_list = request.json.get("counts", [])
    final_counts = defaultdict(int)
    
    for count_dict in counts_list:
        for key, count in count_dict.items():
            final_counts[key] += count

    elapsed = time.perf_counter() - start_time
    print(f"Worker {WORKER_ID} ReduceTask completed: {len(final_counts)} keys in {elapsed:.6f}s")
    
    return jsonify(final_counts)

if __name__ == "__main__":
    print(f"REST MapReduce Worker {WORKER_ID} running on port {PORT}...")
    app.run(host="0.0.0.0", port=PORT)
