from rich.markup import escape
from rich.tree import Tree

from biohub_data_cli.models import Collection, DatasetStats
from biohub_data_cli.utils.cli import console, format_bytes
from biohub_data_cli.utils.s3 import resolve_s3_uris


def get_collections_stats(
    collections: list[Collection],
) -> dict[str, list[DatasetStats]]:
    """Resolve every dataset's S3 URIs and return per-dataset aggregates
    grouped by collection slug. Insertion order follows the input.

    Dry-run stats do not support HTTP URLs at the moment. They are silently skipped,
    since we don't expect HTTP URLs in OPS data.
    """
    stats_by_collection: dict[str, list[DatasetStats]] = {}
    for collection in collections:
        bucket = stats_by_collection.setdefault(collection.slug, [])
        for dataset in collection.datasets:
            s3_uris = [u for u in dataset.urls if u.startswith("s3://")]
            objects, failures = resolve_s3_uris(
                collection.slug, dataset.slug, s3_uris
            )
            bucket.append(
                DatasetStats(
                    collection_slug=collection.slug,
                    dataset_slug=dataset.slug,
                    total_bytes=sum(size for _, size in objects),
                    n_failed_uris=len(failures),
                )
            )
    return stats_by_collection


def estimate_size_summary(collections: list[Collection]) -> str:
    """Format a size estimate from curator-provided `Dataset.file_size_bytes`.

    Three cases: all sized → `~X.Y MB estimated`; some `None` → also flags
    how many; all `None` → `size unknown`. The estimate is dataset-level
    (per the model), so this is faster than dry-run but coarser.
    """
    sizes = [
        d.file_size_bytes
        for c in collections
        for d in c.datasets
    ]
    sized = [s for s in sizes if s is not None]
    if not sized:
        return "size unknown"
    total = format_bytes(sum(sized))
    n_unsized = len(sizes) - len(sized)
    if n_unsized:
        return f"~{total} estimated, {n_unsized} dataset(s) unsized"
    return f"~{total} estimated"


def print_dry_run_summary(
    stats_by_collection: dict[str, list[DatasetStats]],
) -> int:
    """Print one tree per collection + grand total. Partial rows append an
    inline note; the grand total flags overall partiality. Returns the
    total count of S3 URIs that failed to list so the caller can branch on
    it without re-walking the stats.
    """
    total_bytes = 0
    total_failed = 0
    total_datasets = 0
    for coll_slug, stats in stats_by_collection.items():
        tree = Tree(f"[bold]{escape(coll_slug)}[/bold]")
        for s in stats:
            total_bytes += s.total_bytes
            total_failed += s.n_failed_uris
            total_datasets += 1
            note = (
                f" [yellow]· partial ({s.n_failed_uris} size lookup(s) failed)[/yellow]"
                if s.n_failed_uris
                else ""
            )
            tree.add(
                f"{escape(s.dataset_slug)} · {format_bytes(s.total_bytes)}{note}"
            )
        console.print(tree)

    console.print(
        f"Total: {len(stats_by_collection)} collection(s), "
        f"{total_datasets} dataset(s), "
        f"{format_bytes(total_bytes)}"
    )
    if total_failed:
        console.print(
            f"[yellow]Note: {total_failed} size lookup(s) failed; "
            f"total is an underestimate.[/yellow]"
        )
    return total_failed
