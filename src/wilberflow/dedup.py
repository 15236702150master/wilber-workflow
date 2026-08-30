from __future__ import annotations

import shutil
from collections import defaultdict
from pathlib import Path
from typing import Callable

from obspy import read

from .common import (
    DEDUP_DROPPED_DIR_NAME,
    ensure_dir,
    load_csv_rows,
    parse_location_priority,
    write_csv,
)
from .normalize import normalize_loc_code, preferred_location_code
from .wilber import CHANNEL_PREFERENCE_RANK

StageProgressCallback = Callable[[str, int | None, int | None, str | None, str | None, dict | None], None]

# processing_summary.csv field list, kept in sync with normalize.py write_csv call so the
# rewritten file preserves the exact column order consumers (export_final._rewrite_output_paths) expect.
PROCESSING_SUMMARY_FIELDS = [
    "EventID", "EventLabel", "InputSAC", "InputSACCount", "MatchedSACPZ", "OutputPath",
    "Network", "Station", "LocationCode", "Channel", "SampleRateHz", "Npts", "DistanceDeg",
    "InventoryFile", "PreprocessStatus", "DespikeReplaced", "PeakBeforeClean",
    "PeakAfterClean", "Method", "Reason",
]
PROCESSING_SUMMARY_BACKUP = "processing_summary.before_dedup.csv"
DEDUP_SUMMARY_FIELDS = [
    "EventID", "Network", "Station", "LocationCode", "Channel",
    "DroppedFile", "KeptFile", "KeptChannel", "KeptLocation", "Reason",
]


def _iter_delivery_event_dirs(events_root: Path) -> list[Path]:
    """Event subdirectories under 07_final/events, excluding the dedup overflow folder."""
    return sorted(p for p in events_root.iterdir() if p.is_dir() and p.name != DEDUP_DROPPED_DIR_NAME)


def _iter_output_sac_files(event_dir: Path) -> list[Path]:
    """Lowercase .sac outputs of write_final_sac (final_filename uses .sac)."""
    return sorted(p for p in event_dir.iterdir() if p.is_file() and p.suffix.lower() == ".sac")


def _read_sac_header_identity(sac_path: Path) -> tuple[str, str, str, str]:
    """Return (network, station, normalized_location, channel) from SAC header (headonly)."""
    trace = read(str(sac_path), headonly=True)[0]
    location = normalize_loc_code(trace.stats.location or "")
    return (
        str(trace.stats.network).strip(),
        str(trace.stats.station).strip(),
        location,
        str(trace.stats.channel).strip(),
    )


def _channel_rank(channel: str) -> int:
    # Lower rank = higher priority. Unknown channels rank last (stable).
    return CHANNEL_PREFERENCE_RANK.get(channel, len(CHANNEL_PREFERENCE_RANK))


def _select_kept(files: list[dict], location_priority: list[str]) -> tuple[dict, list[dict]]:
    """Pick one file to keep for a single (network, station) group.

    files: each dict has keys path/network/station/location/channel.
    Returns (kept, dropped_list) where each dropped dict gains a "Reason" key.
    """
    best_rank = min(_channel_rank(f["channel"]) for f in files)
    best_channel_files = [f for f in files if _channel_rank(f["channel"]) == best_rank]

    if len(best_channel_files) == 1:
        kept = best_channel_files[0]
    else:
        # Same best channel across multiple locations: resolve via location priority.
        locs = [f["location"] for f in best_channel_files]
        preferred = preferred_location_code(locs, location_priority)
        kept = next(f for f in best_channel_files if f["location"] == preferred)

    dropped: list[dict] = []
    kept_channel = kept["channel"]
    kept_location = kept["location"]
    for f in files:
        if f is kept:
            continue
        if _channel_rank(f["channel"]) == best_rank:
            # same channel, lost on location priority
            reason = f"lower_priority_location:{kept_location}>{f['location']}"
        else:
            reason = f"lower_priority_channel:{kept_channel}>{f['channel']}"
        dropped.append({**f, "Reason": reason})
    return kept, dropped


def _dedup_event(
    event_dir: Path,
    event_id: str,
    location_priority: list[str],
    dropped_root: Path,
) -> list[dict]:
    """Dedup one event dir; move dropped files into dropped_root/<event_id>/. Return summary rows."""
    sac_files = _iter_output_sac_files(event_dir)
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for sac_path in sac_files:
        network, station, location, channel = _read_sac_header_identity(sac_path)
        groups[(network, station)].append({
            "path": sac_path,
            "network": network,
            "station": station,
            "location": location,
            "channel": channel,
        })

    event_dropped_dir = ensure_dir(dropped_root / event_id)
    rows: list[dict] = []
    for files in groups.values():
        kept, dropped = _select_kept(files, location_priority)
        for d in dropped:
            shutil.move(str(d["path"]), str(event_dropped_dir / d["path"].name))
            rows.append({
                "EventID": event_id,
                "Network": d["network"],
                "Station": d["station"],
                "LocationCode": d["location"],
                "Channel": d["channel"],
                "DroppedFile": d["path"].name,
                "KeptFile": kept["path"].name,
                "KeptChannel": kept["channel"],
                "KeptLocation": kept["location"],
                "Reason": d["Reason"],
            })
    return rows


def _update_processing_summary(stage_dir: Path, dropped_basenames: set[str]) -> None:
    """Remove rows whose OutputPath basename is in dropped_basenames; back up the original once."""
    csv_path = stage_dir / "processing_summary.csv"
    if not csv_path.exists():
        return
    backup_path = stage_dir / PROCESSING_SUMMARY_BACKUP
    if not backup_path.exists():
        shutil.copy2(csv_path, backup_path)
    rows = load_csv_rows(csv_path)
    kept_rows = [r for r in rows if Path(r.get("OutputPath", "")).name not in dropped_basenames]
    write_csv(csv_path, PROCESSING_SUMMARY_FIELDS, kept_rows)


def _accumulate_dedup_summary(stage_dir: Path, new_rows: list[dict]) -> None:
    """Append new dedup rows to dedup_summary.csv (cumulative across reruns)."""
    summary_path = stage_dir / "dedup_summary.csv"
    existing = load_csv_rows(summary_path)
    combined = existing + new_rows
    write_csv(summary_path, DEDUP_SUMMARY_FIELDS, combined)


def dedup_workspace(
    workspace_root: Path,
    pipeline_config,
    logger,
    progress_callback: StageProgressCallback | None = None,
) -> dict[str, int]:
    """Cross-band dedup stage: keep one Z component per station under 07_final/events.

    Channel priority is fixed (BHZ>HHZ>SHZ>EHZ>DHZ>MHZ>LHZ>VHZ>UHZ); same-channel
    multiple locations resolve via request.location_priority. Dropped files move to
    07_final/events/_dedup_dropped/<event_id>/ for rollback.
    """
    if not pipeline_config.dedup.enabled:
        logger.info("dedup disabled, skipping")
        if progress_callback is not None:
            progress_callback("dedup", 0, 0, "跨频带去重已禁用", "completed", {"dropped": 0, "kept": 0, "events": 0})
        return {"events": 0, "dropped": 0, "kept": 0, "enabled": False}

    stage_dir = workspace_root / "07_final"
    events_root = stage_dir / "events"
    if not events_root.exists():
        logger.info("no 07_final/events directory, dedup nothing to do")
        return {"events": 0, "dropped": 0, "kept": 0, "enabled": True}

    location_priority = parse_location_priority(pipeline_config.request.location_priority)
    dropped_root = ensure_dir(events_root / DEDUP_DROPPED_DIR_NAME)
    event_dirs = _iter_delivery_event_dirs(events_root)
    total = len(event_dirs)

    if progress_callback is not None:
        progress_callback("dedup", 0, total, "正在跨频带去重", "running", None)

    all_rows: list[dict] = []
    dropped_basenames: set[str] = set()
    total_kept = 0
    for index, event_dir in enumerate(event_dirs, start=1):
        event_id = event_dir.name
        before = len(_iter_output_sac_files(event_dir))
        rows = _dedup_event(event_dir, event_id, location_priority, dropped_root)
        all_rows.extend(rows)
        for r in rows:
            dropped_basenames.add(r["DroppedFile"])
        after = before - len(rows)
        total_kept += after
        if progress_callback is not None:
            progress_callback(
                "dedup", index, total,
                f"正在去重 {index}/{total}: {event_id}（移除 {len(rows)} 个）",
                "running", None,
            )

    _update_processing_summary(stage_dir, dropped_basenames)
    _accumulate_dedup_summary(stage_dir, all_rows)

    stats = {"events": total, "dropped": len(all_rows), "kept": total_kept, "enabled": True}
    logger.info(
        "dedup complete: events=%s kept=%s dropped=%s",
        stats["events"], stats["kept"], stats["dropped"],
    )
    if progress_callback is not None:
        progress_callback(
            "dedup", total, total,
            f"跨频带去重完成：保留 {total_kept} / 移除 {len(all_rows)} 个文件",
            "completed", stats,
        )
    return stats
