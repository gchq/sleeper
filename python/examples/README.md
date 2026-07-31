# Sleeper Python Examples

## DataFusion Query Demo

The `demo_datafusion_query.py` script demonstrates how to call the Sleeper Rust DataFusion library from Python using ctypes bindings and return results as PyArrow RecordBatchReader.

### Setup

Before running the demo, ensure the Python package is built with native bindings:

```bash
cd /home/ubuntu/sleeper/python
pip install -e .
```

This command will:
1. Auto-detect your CPU architecture (x86_64 or aarch64)
2. Generate Python ctypes bindings from `sleeper_df.h`
3. Copy the native library (`libsleeper_df.so`) to the Python package
4. Install ctypesgen automatically if needed

**Note:** The Rust library must be built first. If you get a "native library not found" error, run:
```bash
./scripts/build/build.sh
```

### Running the Demo

```bash
python examples/demo_datafusion_query.py
```

**Expected output:** The demo creates a context, populates realistic query configuration (S3 file paths, row/sort keys, region boundaries), and attempts to execute a query. The example uses fake S3 paths that don't exist, so the Rust library returns an error code (-1) when trying to access the files. However, this demonstrates that the FFI bindings are working correctly - the configuration structures are being properly passed through to the Rust library, which validates them and logs detailed information about what it's trying to query.

In real usage with actual Sleeper data files, the query would succeed and return Arrow record batches that can be consumed from Python.

### How It Works

The demo shows the typical workflow for calling the Sleeper DataFusion API with realistic configuration:

1. **Create a context** - Initialize the FFI context with `create_context()`
2. **Configure the query** - Build `FFICommonConfig` with:
   - **Input files**: Parquet file paths (S3 URLs or local paths)
   - **Row/sort keys**: Column names for the table's key schema
   - **Partition region**: The data range stored in this partition (min/max boundaries)
   - Optional: AWS config (NULL uses IAM role credentials)
3. **Set up query regions** - Define what data ranges you want to query within the partition
4. **Execute the query** - Call `native_query_stream()` to get an Arrow C Data stream
5. **Convert to PyArrow** - Use `pa.RecordBatchReader._import_from_c()` to convert the C stream pointer to a Python RecordBatchReader
6. **Process results** - Iterate through record batches and convert to pandas DataFrames
7. **Cleanup** - Call `destroy_context()` to release resources

### Configuration Details

**FFICommonConfig fields:**
- `job_id`: Unique identifier for this query
- `input_files`: Array of Parquet file paths to query
- `row_key_cols`: Column names that form the row key
- `sort_key_cols`: Column names that form the sort key
- `region`: The partition region (min/max boundaries for row keys)
- `aws_config`: AWS credentials (NULL uses IAM role credentials)

**FFISleeperRegion** defines boundaries:
- `mins` / `maxs`: Row key boundary values (FFIRowKeyValue structs)
- `mins_inclusive` / `maxs_inclusive`: Whether boundaries are inclusive
- `dimension_indexes`: Which dimensions these boundaries apply to

### Key FFI Functions

- `create_context()` → `FFIContext*` - Create context for queries
- `clone_context(ctx)` → `FFIContext*` - Clone a context (shares runtime)
- `destroy_context(ctx)` → `void` - Release context resources
- `native_query_stream(ctx, config, results)` → `int` - Execute query, populate `FFIQueryResults` with Arrow stream
- `native_compact(ctx, config, results, callback)` → `int` - Execute compaction
- `native_query_file(ctx, config, results)` → `int` - Execute query writing to file

All functions return 0 on success, or an error code (see sleeper_df.h for details).

### Generated Bindings

After installation, Python bindings are available in:
```python
from sleeper.generated import sleeper_df_bindings as bindings
```

The bindings are auto-generated from `rust/sleeper_df/include/sleeper_df.h` using ctypesgen and support all C structs and functions defined in the header.
