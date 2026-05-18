from rich.filesize import decimal as format_bytes
from rich.markup import escape
from rich.tree import Tree

from biohub_data_cli.models import Collection, DatasetStats, DryRunAggregate
from biohub_data_cli.utils.cli import console
from biohub_data_cli.utils.s3 import resolve_s3_uris


def get_collections_stats(
    collections: list[Collection],
) -> list[tuple[Collection, list[DatasetStats]]]:
    """Resolve every dataset's S3 URIs and return per-dataset aggregates
    paired with their collection. Order follows the input; duplicate
    collections appear as separate entries.

    Dry-run stats do not support HTTP URLs at the moment. They are counted per
    dataset and surfaced as a warning in the summary (see `print_dry_run_summary`),
    but not sized, since we don't expect HTTP URLs in OPS data.
    """
    result: list[tuple[Collection, list[DatasetStats]]] = []
    total_datasets = sum(len(c.datasets) for c in collections)
    done = 0
    with console.status("Resolving collections stats…") as status:
        for collection in collections:
            rows: list[DatasetStats] = []
            for dataset in collection.datasets:
                done += 1
                status.update(
                    f"Resolving collections stats… ({done}/{total_datasets}) "
                    f"{collection.slug}/{dataset.slug}"
                )
                s3_uris = [u for u in dataset.urls if u.startswith("s3://")]
                n_http = sum(
                    1 for u in dataset.urls if u.startswith(("http://", "https://"))
                )
                objects, failures = resolve_s3_uris(
                    collection.slug, dataset.slug, s3_uris
                )
                rows.append(
                    DatasetStats(
                        collection_slug=collection.slug,
                        dataset_slug=dataset.slug,
                        total_bytes=sum(size for _, size in objects),
                        n_failed_uris=len(failures),
                        n_http_urls_skipped=n_http,
                    )
                )
            result.append((collection, rows))
    return result


def estimate_size_summary(collections: list[Collection]) -> str:
    """Format a size estimate from curator-provided `Dataset.file_size_bytes`.

    Three cases: all sized → `~X.Y MB estimated`; some `None` → also flags
    how many; all `None` → `size unknown`. The estimate is dataset-level
    (per the model), so this is faster than dry-run but coarser.
    """
    sizes = [d.file_size_bytes for c in collections for d in c.datasets]
    sized = [s for s in sizes if s is not None]
    if not sized:
        return "size unknown"
    total = format_bytes(sum(sized))
    n_unsized = len(sizes) - len(sized)
    if n_unsized:
        return (
            f"~{total} estimated (size unknown for {n_unsized}/{len(sizes)} dataset(s))"
        )
    return f"~{total} estimated"


def aggregate_dry_run_stats(
    stats_by_collection: list[tuple[Collection, list[DatasetStats]]],
) -> DryRunAggregate:
    """Roll per-dataset stats into a single grand-total summary."""
    all_rows = [s for _, rows in stats_by_collection for s in rows]
    return DryRunAggregate(
        n_collections=len(stats_by_collection),
        n_datasets=len(all_rows),
        total_bytes=sum(s.total_bytes for s in all_rows),
        n_failed_uris=sum(s.n_failed_uris for s in all_rows),
        n_http_urls_skipped=sum(s.n_http_urls_skipped for s in all_rows),
    )


def print_dry_run_summary(
    stats_by_collection: list[tuple[Collection, list[DatasetStats]]],
    aggregate: DryRunAggregate,
) -> None:
    """Print one tree per collection followed by the grand-total line.
    Partial rows append an inline note; the grand total flags overall
    partiality.
    """
    for collection, stats in stats_by_collection:
        tree = Tree(f"[bold]{escape(collection.slug)}[/bold]")
        for s in stats:
            dataset_warnings: list[str] = []
            if s.n_failed_uris:
                dataset_warnings.append(
                    f"partial ({s.n_failed_uris} size lookup(s) failed)"
                )
            if s.n_http_urls_skipped:
                dataset_warnings.append("contains HTTP URLs that are not sized")
            warning_str = "".join(f" [yellow]· {w}[/yellow]" for w in dataset_warnings)
            tree.add(
                f"{escape(s.dataset_slug)} · {format_bytes(s.total_bytes)}{warning_str}"
            )
        console.print(tree)

    console.print(
        f"Total: {aggregate.n_collections} collection(s), "
        f"{aggregate.n_datasets} dataset(s), "
        f"{format_bytes(aggregate.total_bytes)}"
    )

    total_warnings: list[str] = []
    if aggregate.n_failed_uris:
        total_warnings.append(f"{aggregate.n_failed_uris} lookup(s) failed")
    if aggregate.n_http_urls_skipped:
        total_warnings.append("HTTP URLs are not sized")
    if total_warnings:
        console.print(
            f"[yellow]Warning: total is an underestimate, since "
            f"{' and '.join(total_warnings)}.[/yellow]"
        )
