# Sleeper Python Client

Python client library for Sleeper services.

## Requirements

- Python 3.10, 3.11, or 3.12
- pip

## Setup

Create a virtual environment:

```python -m venv .venv source .venv/bin/activate```

Install the package:

```pip install -e .```

Install development dependencies:

```pip install -e .[dev]```


## Tooling

### Linting

Run Ruff:

```ruff check src/```


Apply automatic fixes:

```ruff check src/ --fix```


## Tests

Run the test suite:

```pytest```


## Project structure

- pyproject.toml
- README.md
- LICENSE
- examples/
- src/
  - pq/
  - sleeper/
- tests/
- test/

## Python version support

- Python 3.10
- Python 3.11
- Python 3.12

## License

See the LICENSE file for details.
