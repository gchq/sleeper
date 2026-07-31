#!/usr/bin/env python3
"""Demo script showing how to call DataFusion query from Rust via ctypes and convert to PyArrow RecordBatchReader."""

import sys
from pathlib import Path
from ctypes import POINTER, c_char_p, c_bool
import ctypes

import pyarrow as pa

# Add src directory to path for development
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from sleeper.generated import sleeper_df_bindings as bindings


def create_ffi_bytes_array(strings):
    """Create an array of FFIBytes structs from a list of strings."""
    ffi_bytes_array = (bindings.FFIBytes * len(strings))()

    # Keep references to allocated buffers to prevent garbage collection
    buffers = []

    for i, s in enumerate(strings):
        if isinstance(s, str):
            s = s.encode('utf-8')
        # Create a buffer that won't be garbage collected
        buf = ctypes.create_string_buffer(s)
        buffers.append(buf)

        ffi_bytes_array[i].length = len(s)
        ffi_bytes_array[i].buffer = ctypes.cast(buf, ctypes.POINTER(ctypes.c_ubyte))

    # Store references to prevent collection
    ffi_bytes_array._buffers = buffers
    return ffi_bytes_array


def create_row_key_value_string(value):
    """Create an FFIRowKeyValue with a string value."""
    # Create buffer for the string
    if isinstance(value, str):
        value = value.encode('utf-8')

    buf = ctypes.create_string_buffer(value)

    # Create FFIBytes struct
    ffi_bytes = bindings.FFIBytes()
    ffi_bytes.length = len(value)
    ffi_bytes.buffer = ctypes.cast(buf, ctypes.POINTER(ctypes.c_ubyte))

    # Allocate and store FFIBytes pointer
    ffi_bytes_ptr = ctypes.pointer(ffi_bytes)

    # Create FFIRowKeyValue
    row_key = bindings.FFIRowKeyValue()
    row_key.contained = bindings.String  # FFIRowKeyValueType.String = 3
    row_key.item.string = ffi_bytes_ptr

    # Keep references alive
    row_key._buf = buf
    row_key._ffi_bytes = ffi_bytes
    row_key._ffi_bytes_ptr = ffi_bytes_ptr

    return row_key


def create_sleeper_region(min_val, max_val=None):
    """
    Create a Sleeper region for a single string key dimension.

    Args:
        min_val: Minimum (inclusive) string value
        max_val: Maximum (exclusive) string value, or None for unbounded max
    """
    region = bindings.FFISleeperRegion()
    region.number_of_dimensions = 1

    # Create min boundary
    min_key = create_row_key_value_string(min_val)
    min_ptr = ctypes.pointer(min_key)

    region.mins = min_ptr
    region.mins_inclusive = ctypes.pointer(c_bool(True))

    # Create max boundary if provided
    if max_val is not None:
        max_key = create_row_key_value_string(max_val)
        max_ptr = ctypes.pointer(max_key)
        region.maxs = max_ptr
        region.maxs_inclusive = ctypes.pointer(c_bool(False))  # exclusive
        region._max_key = max_key
        region._max_ptr = max_ptr
    else:
        region.maxs = None
        region.maxs_inclusive = None

    # Dimension index (size_t, not bool)
    dim_idx = ctypes.c_size_t(0)  # First dimension
    region.dimension_indexes = ctypes.pointer(dim_idx)

    # Keep references alive
    region._min_key = min_key
    region._min_ptr = min_ptr
    region._dim_idx = dim_idx

    return region


def run_query_demo():
    """Demonstrate calling native_query_stream with populated configuration."""
    print("=== Sleeper DataFusion Query Demo ===\n")

    # Step 1: Create context
    print("Creating context...")
    ctx = bindings.create_context()
    if not ctx:
        print("Failed to create context")
        return

    try:
        # Step 2: Set up query configuration for S3 Parquet files
        print("Setting up query configuration...")

        # Create common config
        common = bindings.FFICommonConfig()

        # Set job ID
        job_id = b"demo-query-001"
        job_id_buf = ctypes.create_string_buffer(job_id)
        common.job_id = ctypes.cast(job_id_buf, c_char_p)

        # Set input files (Parquet files on S3)
        input_file_paths = [
            "s3://my-bucket/data/partition1/0.parquet",
            "s3://my-bucket/data/partition1/1.parquet",
        ]
        input_files = create_ffi_bytes_array(input_file_paths)
        common.input_files_len = len(input_file_paths)
        common.input_files = input_files
        common.input_files_sorted = True

        # Set row key columns
        row_key_cols = create_ffi_bytes_array(["key"])
        common.row_key_cols_len = len(row_key_cols)
        common.row_key_cols = row_key_cols

        # Set sort key columns
        sort_key_cols = create_ffi_bytes_array(["timestamp"])
        common.sort_key_cols_len = len(sort_key_cols)
        common.sort_key_cols = sort_key_cols

        # Set partition region (data range stored in this partition)
        partition_region = create_sleeper_region("aaaaa", "b")
        common.region = ctypes.pointer(partition_region)

        # Optional: AWS config can be NULL for IAM credentials
        common.aws_config = None
        common.parquet_options = None
        # Aggregation/filtering config must be empty strings, not NULL
        agg_config = ctypes.create_string_buffer(b"")
        common.aggregation_config = ctypes.cast(agg_config, c_char_p)
        filter_config = ctypes.create_string_buffer(b"")
        common.filtering_config = ctypes.cast(filter_config, c_char_p)
        common.output_file = None
        common.write_sketch_file = False
        common.use_readahead_store = False

        # Create leaf partition query config
        query_config = bindings.FFILeafPartitionQueryConfig()
        query_config.common = ctypes.pointer(common)

        # Set up query regions (what data range we want to query)
        query_region = create_sleeper_region("aaa", "aab")
        query_regions = (bindings.FFISleeperRegion * 1)()
        query_regions[0] = query_region

        query_config.query_regions_len = 1
        query_config.query_regions = query_regions
        query_config.requested_value_fields_set = False
        query_config.requested_value_fields_len = 0
        query_config.requested_value_fields = None
        query_config.extensions_len = 0
        query_config.extensions = None
        query_config.explain_plans = False

        # Step 3: Execute query and get Arrow stream
        print("Executing query...\n")
        query_results = bindings.FFIQueryResults()

        result_code = bindings.native_query_stream(
            ctx,
            POINTER(bindings.FFILeafPartitionQueryConfig)(query_config),
            POINTER(bindings.FFIQueryResults)(query_results)
        )

        if result_code != 0:
            print(f"Query failed with error code: {result_code}")
            print("\nNote: This demo uses example S3 paths which likely don't exist.")
            print("In real usage with actual Sleeper data, the query would return results.")
            print(f"\nCheck logs above for details from the Rust library.")
            return

        # Step 4: Convert Arrow C stream to PyArrow RecordBatchReader
        print("Converting Arrow C stream to PyArrow RecordBatchReader...")

        if not query_results.arrow_array_stream:
            print("Query returned null pointer")
            return

        # Get the pointer to the Arrow C Data stream
        arrow_stream_ptr = query_results.arrow_array_stream

        # Use PyArrow's C Data import function to convert
        # The pointer is already a ctypes pointer object, get its address as integer
        ptr_addr = ctypes.cast(arrow_stream_ptr, ctypes.c_void_p).value
        reader = pa.RecordBatchReader._import_from_c(ptr_addr)

        print("Query completed successfully!\n")
        print("=== Query Results ===")
        print("-" * 50)

        # Read and display all batches
        batch_count = 0
        for i, batch in enumerate(reader):
            print(f"\nBatch {i}:")
            print(f"  Schema: {batch.schema}")
            print(f"  Rows: {batch.num_rows}")
            print(f"  Columns: {batch.num_columns}")

            # Try to display as pandas DataFrame if available
            try:
                df = batch.to_pandas()
                print(df)
            except ModuleNotFoundError:
                # Display as PyArrow Table instead
                print("  (pandas not installed, showing PyArrow output)")
                print(batch.to_pydict())

            batch_count += 1

        if batch_count == 0:
            print("(No batches returned)")

        print("\n" + "-" * 50)

    finally:
        # Step 5: Clean up context
        print("\nCleaning up context...")
        bindings.destroy_context(ctx)
        print("Done!")


if __name__ == "__main__":
    run_query_demo()
