import threading
from pathlib import Path
from types import TracebackType

from rich.console import Console, Group
from rich.live import Live
from rich.markup import escape
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TaskProgressColumn,
    TextColumn,
)
from rich.tree import Tree

from biohub_data_cli.models import DownloadFailure

console = Console()


def safe_join(root: Path, *parts: str) -> Path:
    """Join parts under root, rejecting paths that escape via '..' or absolute components."""
    root = root.resolve()
    candidate = (root / Path(*parts)).resolve()
    if not candidate.is_relative_to(root):
        raise ValueError(f"refusing path that escapes {root}: {parts!r}")
    return candidate


def make_progress() -> Progress:
    """A Progress configured with the column set we want for downloads:
    label · bar · percentage · "bytes-done / total". Single instance shared
    across all dataset tasks so rich can serialize terminal writes.
    """
    return Progress(
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        DownloadColumn(),
        console=console,
    )


class DownloadDisplay:
    """Owns the rich.Live region for the download flow.

    Initial render shows only the progress bars. The first time `record_failure`
    is called, the live region swaps to show a failures Tree below the bars.
    Failures are grouped two levels deep: collection → dataset → one leaf per
    failed URL.
    """

    def __init__(self) -> None:
        self.progress = make_progress()
        self.failures: list[DownloadFailure] = []
        # task_id -> collection_slug. Written once at task creation time
        # (single-threaded). Used after the download flow to group each task's
        # bytes (read off Progress.tasks[i].completed) by collection.
        self._task_collection: dict[TaskID, str] = {}
        self._tree = Tree("[bold red]Failures[/bold red]")
        # collection_slug -> its branch under the root tree. Memoized so multiple
        # failures in the same collection don't create duplicate branches.
        self._collection_branches: dict[str, Tree] = {}
        # (collection_slug, dataset_slug) -> its branch under the collection's
        # branch. Same memoization rationale, scoped per dataset.
        self._dataset_branches: dict[tuple[str, str], Tree] = {}
        self._live = Live(
            self.progress, console=console, refresh_per_second=4, transient=False
        )
        # Serializes read-modify-write of a task's total across HTTP workers
        # that report Content-Length concurrently for the same dataset.
        self._total_lock = threading.Lock()

    def __enter__(self) -> "DownloadDisplay":
        self._live.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        return self._live.__exit__(exc_type, exc, tb)

    def add_dataset_download_task(
        self, label: str, collection_slug: str, total: int | None
    ) -> TaskID:
        """Create a per-dataset progress task and register it under its collection
        so get_bytes_by_collection() can group the task's bytes correctly later."""
        task_id = self.progress.add_task(label, total=total)
        self._task_collection[task_id] = collection_slug
        return task_id

    def advance_task(self, task_id: TaskID, n: int) -> None:
        """Bump one Progress task by `n` bytes.

        The rare HTTP-without-Content-Length case may show >100% in the percentage / bytes columns.
        No lock needed, since rich takes care of concurrent updates.
        """
        self.progress.update(task_id, advance=n)

    def get_bytes_by_collection(self) -> dict[str, int]:
        """Sum each task's downloaded bytes (from rich's `Progress.tasks`)
        grouped by the collection_slug it was registered under in
        add_dataset_download_task. Intended to be read after the download flow finishes."""
        totals: dict[str, int] = {}
        for task in self.progress.tasks:
            coll = self._task_collection.get(task.id)
            if coll is not None:
                totals[coll] = totals.get(coll, 0) + int(task.completed)
        return totals

    def grow_task_total(self, task_id: TaskID, n: int) -> None:
        """Add `n` bytes to one task's total.

        Called from HTTP workers when they learn a file's size (e.g. via
        Content-Length on the GET response). S3 sizes are seeded at task
        creation time from list_objects_v2, so S3 workers don't call this.

        TODO: use a concurrent dict with TTL to avoid the linear scan.
        """
        with self._total_lock:
            task = next((t for t in self.progress.tasks if t.id == task_id), None)
            if task is None:
                return
            self.progress.update(task_id, total=(task.total or 0) + n)

    def record_failure(self, f: DownloadFailure) -> None:
        """Append a failure and mutate the failures tree. Not thread-safe — call from the main thread only."""
        assert threading.current_thread() is threading.main_thread(), (
            "record_failure must be called from the main thread"
        )
        if not self.failures:
            self._live.update(Group(self.progress, self._tree))
        self.failures.append(f)

        coll_branch = self._collection_branches.get(f.collection_slug)
        if coll_branch is None:
            coll_branch = self._tree.add(f"[red]{escape(f.collection_slug)}[/red]")
            self._collection_branches[f.collection_slug] = coll_branch

        ds_key = (f.collection_slug, f.dataset_slug)
        ds_branch = self._dataset_branches.get(ds_key)
        if ds_branch is None:
            ds_branch = coll_branch.add(f"[red]{escape(f.dataset_slug)}[/red]")
            self._dataset_branches[ds_key] = ds_branch

        ds_branch.add(f"{escape(f.url)} — {escape(f.reason)}")
