import pathlib
from pathlib import Path


def get_repository_path() -> Path:
    file_path = pathlib.Path(__file__).resolve()
    return file_path.parent.parent.parent.parent
