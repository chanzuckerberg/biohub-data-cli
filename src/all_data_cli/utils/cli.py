import contextlib
from pathlib import Path
from typing import Iterator

from tqdm import tqdm


def safe_join(root: Path, *parts: str) -> Path:
    """Join parts under root, rejecting paths that escape via '..' or absolute components."""
    root = root.resolve()
    candidate = (root / Path(*parts)).resolve()
    if not candidate.is_relative_to(root):
        raise ValueError(f"refusing path that escapes {root}: {parts!r}")
    return candidate


@contextlib.contextmanager
def progress_bar_ctx(total: int) -> Iterator[tqdm]:
    pbar = tqdm(total=total, unit="B", unit_scale=True)
    try:
        yield pbar
    finally:
        pbar.close()
