Python API
==========

The Python API allows users to query Sleeper from Python, and to trigger uploads of data in Parquet files. There is
also the ability to upload rows directly from the Python API, but this is only intended to be used for very small
volumes of data.

## Requirements

* Python 3.7+

## Installation

From the `python` directory, run:
```bash
pip install .
```

## Known issues

* Python (pyarrow) uses INT64 in saved Parquet files, so the Sleeper schema must use LongType, not IntType for
integer columns.

## Usage

The main entrypoint for the Python API is the SleeperClient class. See the Docstrings for more information.

```python
from sleeper.client import SleeperClient
my_sleeper = SleeperClient('my_sleeper_instance')
```

You can find examples in the `python/examples` directory:

https://github.com/gchq/sleeper/tree/develop/python/examples
