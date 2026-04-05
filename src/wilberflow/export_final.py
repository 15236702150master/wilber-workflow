from __future__ import annotations

import csv
import shutil
from pathlib import Path
from typing import Callable

from .common import ensure_dir, write_csv, write_json, write_key_value_csv


SUMMARY_FILES = [
    "processing_summary.csv",
    "processing_failures.csv",
    "skipped_extra_channels.csv",
    "summary.csv",
    "summary.json",
]
FINAL_ROOT_NAME = "07_final"
DELIVERY_EVENTS_DIR_NAME = "events"
DELIVERY_METADATA_DIR_NAME = "metadata"
StageProgressCallback = Callable[[str, int | None, int | None, str | None, str | None], None]


def _copy_event_tree(
    source_events_root: Path,
    event_root: Path,
    progress_callback: StageProgressCallback | None = None,
) -> tuple[int, int]:
    ensure_dir(event_root)
    event_dirs = 0
    sac_files = 0
    source_event_dirs = sorted(path for path in source_events_root.iterdir() if path.is_dir())
    total_event_dirs = len(source_event_dirs)
    if progress_callback is not None:
        progress_callback("deliver", 0, total_event_dirs, "等待整理交付目录", "running")
    for index, event_dir in enumerate(source_event_dirs, start=1):
        if progress_callback is not None:
            progress_callback("deliver", index - 1, total_event_dirs, f"正在整理 {index}/{total_event_dirs}: {event_dir.name}", "running")
        target_dir = event_root / event_dir.name
        if target_dir.exists():
            shutil.rmtree(target_dir)
        shutil.copytree(event_dir, target_dir)
        event_dirs += 1
        sac_files += sum(1 for path in target_dir.iterdir() if path.is_file())
        if progress_callback is not None:
            progress_callback("deliver", index, total_event_dirs, f"已整理 {index}/{total_event_dirs}: {event_dir.name}", "running")
    return event_dirs, sac_files


def _rewrite_output_paths(source_csv: Path, target_csv: Path, source_events_root: Path, event_root: Path) -> None:
    if not source_csv.exists():
        return
    rows: list[dict[str, str]] = []
    with source_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        for row in reader:
            output_path = row.get("OutputPath", "").strip()
            if output_path:
                path = Path(output_path)
                try:
                    relative = path.relative_to(source_events_root)
                    row["OutputPath"] = str(event_root / relative)
                except ValueError:
                    pass
            rows.append(dict(row))
    if fieldnames:
        write_csv(target_csv, fieldnames, rows)


def default_export_roots(workspace_root: Path) -> tuple[Path, Path]:
    final_root = workspace_root / FINAL_ROOT_NAME
    return final_root / DELIVERY_EVENTS_DIR_NAME, final_root / DELIVERY_METADATA_DIR_NAME


def export_final_layout(
    workspace_root: Path,
    event_root: Path,
    metadata_root: Path,
    logger,
    progress_callback: StageProgressCallback | None = None,
) -> dict[str, object]:
    source_final_root = workspace_root / "07_final"
    source_events_root = source_final_root / "events"
    if not source_events_root.exists():
        raise FileNotFoundError(f"missing source final events directory: {source_events_root}")

    ensure_dir(event_root)
    ensure_dir(metadata_root)

    source_events_resolved = source_events_root.resolve()
    event_root_resolved = event_root.resolve()
    if source_events_resolved == event_root_resolved:
        source_event_dirs = sorted(path for path in source_events_root.iterdir() if path.is_dir())
        event_dirs = len(source_event_dirs)
        sac_files = sum(1 for event_dir in source_event_dirs for path in event_dir.iterdir() if path.is_file())
        if progress_callback is not None:
            progress_callback("deliver", 0, len(SUMMARY_FILES), "事件目录已在最终位置，正在整理 metadata", "running")
        logger.info("reusing final event directory at %s", event_root)
    else:
        event_dirs, sac_files = _copy_event_tree(source_events_root, event_root, progress_callback=progress_callback)
        logger.info("exported %s event directories and %s SAC files", event_dirs, sac_files)

    total_steps = len(SUMMARY_FILES)
    for index, name in enumerate(SUMMARY_FILES, start=1):
        source_path = source_final_root / name
        target_path = metadata_root / name
        if name in {"processing_summary.csv", "processing_failures.csv"}:
            _rewrite_output_paths(source_path, target_path, source_events_root, event_root)
        elif source_path.exists():
            shutil.copy2(source_path, target_path)
        if progress_callback is not None:
            progress_callback("deliver", index, total_steps, f"已整理 {index}/{total_steps}: {name}", "running")

    payload = {
        "workspace_root": str(workspace_root),
        "event_root": str(event_root),
        "metadata_root": str(metadata_root),
        "event_dir_count": event_dirs,
        "sac_file_count": sac_files,
    }
    write_json(metadata_root / "export_summary.json", payload)
    write_key_value_csv(metadata_root / "export_summary.csv", payload)
    if progress_callback is not None:
        progress_callback("deliver", total_steps, total_steps, f"整理交付完成：metadata {total_steps}/{total_steps}", "completed")
    return payload
