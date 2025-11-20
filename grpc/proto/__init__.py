"""
MapReduce Protocol Buffer Definitions

This module provides user-friendly imports for the MapReduce gRPC service.
The actual generated files follow protobuf naming conventions (_pb2 suffix).
"""

# Import with user-friendly names
from proto.mapreduce_pb2 import (
    MapRequest,
    MapResponse,
    ReduceRequest,
    ReduceResponse,
)

from proto.mapreduce_pb2_grpc import (
    MapReduceServiceStub,
    MapReduceServiceServicer,
    add_MapReduceServiceServicer_to_server,
)

__all__ = [
    # Messages
    'MapRequest',
    'MapResponse',
    'ReduceRequest',
    'ReduceResponse',
    # Service
    'MapReduceServiceStub',
    'MapReduceServiceServicer',
    'add_MapReduceServiceServicer_to_server',
]

