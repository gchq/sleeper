Python API
==========

The Python API allows users to query Sleeper from Python, and to trigger uploads of data in Parquet files. There is
also the ability to upload rows directly from the Python API, but this is only intended to be used for very small
volumes of data.

## Requirements

* Python 3.10+
* Docker to run pytests

## Installation

From the `python` directory, run:
```bash
pip install .
```

## Developments

To develop the Python module from the `python` directory, run:

```bash
# Create a virtual environment for the project
python -m venv .venv

# Activate the virtual environment
source .venv/bin/activate

# Install the package in editable mode along with development dependencies.
#This includes `pytest`, `ruff` (linter/formatter), and `testcontainers` for running tests against a local AWS environment via LocalStack.
pip install -e ".[dev]"

# Run Ruff linting checks
ruff check

# Check that code formatting matches the project's Ruff formatting rules
ruff format --check

# Run the test suite
pytest
```

## Known issues

* Python (pyarrow) uses INT64 in saved Parquet files, so the Sleeper schema must use LongType, not IntType for
integer columns.

## Usage

The main entry point to the Python API is the `SleeperClient` class. API documentation is available through the package docstrings.

To browse the documentation, activate a virtual environment with Sleeper installed and start an interactive Python session:

```python
>>> from sleeper import SleeperClient
>>> help(SleeperClient)
```

Create a client for a Sleeper instance:

```python
from sleeper import SleeperClient

client = SleeperClient("my_sleeper_instance")
```

Additional examples are available in the `python/examples` directory.

https://github.com/gchq/sleeper/tree/develop/python/examples
