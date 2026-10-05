from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import SMALL_FILE_THRESHOLD_MB


def get_file_size_mb(file_path: str) -> float:
    """
    Return file size in megabytes.
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    if not path.is_file():
        raise ValueError(f"Path is not a file: {file_path}")

    return path.stat().st_size / (1024 * 1024)


def choose_engine(file_path: str) -> str:
    """
    Choose the processing engine based on file size.
    """
    file_size_mb = get_file_size_mb(file_path)

    if file_size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
        reason = (
            f"file size ({file_size_mb:.2f} MB) "
            f"is <= threshold ({SMALL_FILE_THRESHOLD_MB} MB)"
        )
    else:
        engine = "pyspark"
        reason = (
            f"file size ({file_size_mb:.2f} MB) "
            f"is > threshold ({SMALL_FILE_THRESHOLD_MB} MB)"
        )

    print("=" * 60)
    print("FILE ROUTER ENGINE SELECTION")
    print("=" * 60)
    print(f"File Path   : {file_path}")
    print(f"File Size   : {file_size_mb:.2f} MB")
    print(f"Engine Used : {engine}")
    print(f"Reason      : {reason}")
    print("=" * 60)

    return engine


if __name__ == "__main__":
    small_file = PROJECT_ROOT / "data" / "small_sample.csv"
    if small_file.exists():
        choose_engine(str(small_file))