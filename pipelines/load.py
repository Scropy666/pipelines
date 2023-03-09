import sys
import importlib.machinery
from pathlib import Path

from .utils import print_error


def load_pipeline():
    source_dir = str(Path.cwd().parent)
    source_file_path = f'{source_dir}/example_pipeline/pipeline.py'

    sys.path.insert(0, source_dir)
    loader = importlib.machinery.SourceFileLoader('pipeline', source_file_path)

    try:
        module = loader.load_module()
    except FileNotFoundError:
        print_error(f"File not found: {source_file_path}")
        sys.exit(1)

    try:
        return module.pipeline
    except AttributeError:
        print_error(f"There is no pipeline object defined in this file: {source_file_path}")
        sys.exit(1)

