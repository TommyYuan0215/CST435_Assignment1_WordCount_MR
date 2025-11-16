# Protocol Buffer Definitions

## Files Overview

- **`mapreduce.proto`** - Source Protocol Buffer definition file

  - Defines the MapReduceService with MapTask and ReduceTask RPCs
  - Defines message types: MapRequest, MapResponse, ReduceRequest, ReduceResponse

- **`mapreduce_pb2.py`** - Generated Python code for message types

  - Generated from `mapreduce.proto`
  - Contains: MapRequest, MapResponse, ReduceRequest, ReduceResponse classes
  - The `_pb2` suffix is a protobuf convention (even for proto3 syntax)

- **`mapreduce_pb2_grpc.py`** - Generated Python code for gRPC service
  - Generated from `mapreduce.proto`
  - Contains: MapReduceServiceStub, MapReduceServiceServicer classes
  - The `_pb2_grpc` suffix indicates gRPC bindings for protobuf

## Regenerating Files

If you modify `mapreduce.proto`, regenerate the Python files:

```bash
python -m grpc_tools.protoc \
  -I proto \
  --python_out=proto \
  --grpc_python_out=proto \
  proto/mapreduce.proto
```

## Naming Convention

The `_pb2` suffix is standard protobuf naming:

- `_pb2` = Protobuf Python API version 2 (used even with proto3 syntax)
- `_pb2_grpc` = gRPC bindings for protobuf

These names are generated automatically and should not be manually edited.
