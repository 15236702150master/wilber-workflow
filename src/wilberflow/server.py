from __future__ import annotations

import json
import mimetypes
import os
import threading
from collections import Counter
from datetime import datetime, timedelta, timezone
from functools import partial
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from obspy import UTCDateTime

from .common import (
    DEFAULT_EVENT_SERVICE_URL,
    DEFAULT_WILBER_BASE_URL,
    build_http_opener,
    format_ws_datetime,
    http_get_text,
    resolve_user_path,
    setup_logger,
    write_json,
)
from .config import load_config
from .pipeline import prepare_workspace, run_all, run_resume_from_mail, workflow_stage_sequence
from .wilber import (
    attach_virtual_networks,
    fetch_station_records_for_time,
    fetch_virtual_networks_for_time,
    parse_event_service_text,
    parse_station_tree_text,
)


DEFAULT_NETWORKS = ""
DEFAULT_CHANNELS = "BH?"
OUTPUT_FORMAT_OPTIONS = [
    {"value": "sacbl", "label": "SAC binary (little-endian)"},
    {"value": "sacbb", "label": "SAC binary (big-endian)"},
    {"value": "saca", "label": "SAC ASCII"},
    {"value": "miniseed", "label": "miniSEED"},
    {"value": "ascii1", "label": "ASCII: 1 column format"},
    {"value": "ascii2", "label": "ASCII: 2 column format"},
    {"value": "geocsv.slist", "label": "GeoCSV: 1 column format"},
    {"value": "geocsv.tspair", "label": "GeoCSV: 2 column format"},
    {"value": "requestbody", "label": "Request Body Only (no data)"},
]
BUNDLE_OPTIONS = [
    {"value": "", "label": "individual files"},
    {"value": "tar", "label": "tar archive"},
]
NETWORK_CATALOG_LIMIT = 500
NETWORK_CATALOG_CACHE_VERSION = 1
NETWORK_CATALOG_SAMPLE_TIMES_UTC = [
    "1996-01-01T00:00:00",
    "2001-02-16T05:59:09",
    "2010-02-27T06:34:14",
    "2023-02-06T01:17:34",
    "2024-01-01T00:00:00",
]
TIMEWINDOW_BEFORE_OPTIONS = [0, 1, 2, 5, 10, 30, 60, 90, 120]
TIMEWINDOW_AFTER_OPTIONS = [0, 1, 2, 5, 10, 20, 30, 60, 90, 120, 240, 360, 480]
CHANNEL_OPTIONS = [
    {
        "label": "High Gain Seismometer",
        "options": [
            {"value": "?H?", "label": "All channels (?H?)", "group": "High Gain Seismometer"},
            {"value": "BH?", "label": "All channels (BH?)", "group": "High Gain Seismometer / Mid Rate"},
            {"value": "BHZ", "label": "Vertical only (BHZ)", "group": "High Gain Seismometer / Mid Rate"},
            {"value": "LH?", "label": "All channels (LH?)", "group": "High Gain Seismometer / Low Rate"},
            {"value": "LHZ", "label": "Vertical only (LHZ)", "group": "High Gain Seismometer / Low Rate"},
        ],
    },
    {
        "label": "Synthetics",
        "options": [
            {"value": "MX?", "label": "ShakeMovie seismograms (MX?)", "group": "Synthetics"},
            {"value": "LX?", "label": "ShakeMovie seismograms (LX?)", "group": "Synthetics"},
        ],
    },
    {
        "label": "Common Atmospheric Pressure",
        "options": [
            {"value": "BD?", "label": "Mid rate (BD?)", "group": "Common Atmospheric Pressure"},
            {"value": "LD?", "label": "Low rate (LD?)", "group": "Common Atmospheric Pressure"},
        ],
    },
]

WORKFLOW_STATE_LOCK = threading.Lock()
WORKFLOW_STATE: dict[str, object] = {
    "status": "idle",
    "message": "等待运行",
    "mode": "run_all",
    "stage_sequence": [],
    "stage_progress": {},
    "current_stage_key": "",
    "batch_id": "",
    "workspace_base_root": "",
    "mail_expected_count": 0,
    "mail_received_count": 0,
    "mail_pending_count": 0,
    "mail_progress_note": "",
    "workspace_root": "",
    "started_at_utc": None,
    "finished_at_utc": None,
    "log_path": "",
}
WORKFLOW_THREAD: threading.Thread | None = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_workspace_root() -> Path:
    return _project_root() / "output"


def _batch_timezone() -> timezone:
    return timezone(timedelta(hours=8))


def _generate_batch_id(now: datetime | None = None) -> str:
    current = now.astimezone(_batch_timezone()) if now is not None else datetime.now(_batch_timezone())
    return f"wf_{current.strftime('%Y%m%d_%H%M%S')}"


def _batch_dir_path(base_root: Path, batch_id: str) -> Path:
    return base_root / batch_id


def _batch_manifest_for_path(path: Path) -> dict[str, object]:
    runtime_config = path / ".wilberflow-studio" / "runtime_config.toml"
    request_plan = path / "03_requests" / "request_plan.csv"
    mail_summary = path / "04_mail" / "summary.json"
    status = "unknown"
    if mail_summary.exists():
        try:
            payload = json.loads(mail_summary.read_text(encoding="utf-8"))
            status = str(payload.get("status", "unknown"))
        except Exception:
            status = "unknown"
    modified_at_utc = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "batch_id": path.name,
        "workspace_root": str(path),
        "has_runtime_config": runtime_config.exists(),
        "has_request_plan": request_plan.exists(),
        "status": status,
        "modified_at_utc": modified_at_utc,
    }


def _list_batch_directories(base_root: Path) -> list[dict[str, object]]:
    if not base_root.exists() or not base_root.is_dir():
        return []
    batches: list[dict[str, object]] = []
    for path in sorted((item for item in base_root.iterdir() if item.is_dir()), key=lambda item: item.stat().st_mtime, reverse=True):
        if (path / ".wilberflow-studio" / "runtime_config.toml").exists() or (path / "03_requests" / "request_plan.csv").exists():
            batches.append(_batch_manifest_for_path(path))
    return batches


def _toml_unquote(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        inner = text[1:-1]
        return inner.replace('\\"', '"').replace("\\\\", "\\")
    return text


def _toml_quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _with_batched_request_label_prefix(config_toml: str, batch_id: str) -> str:
    lines = config_toml.splitlines()
    in_request_section = False
    replaced = False
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            if in_request_section and not replaced:
                lines.insert(index, f"request_label_prefix = {_toml_quote(f'wilberflow_{batch_id}')}")
                replaced = True
                break
            in_request_section = stripped == "[request]"
            continue
        if not in_request_section or not stripped.startswith("request_label_prefix"):
            continue
        _, _, raw_value = line.partition("=")
        prefix = _toml_unquote(raw_value).strip() or "wilberflow"
        if not prefix.endswith(f"_{batch_id}"):
            prefix = f"{prefix}_{batch_id}"
        lines[index] = f"request_label_prefix = {_toml_quote(prefix)}"
        replaced = True
        break
    if in_request_section and not replaced:
        lines.append(f"request_label_prefix = {_toml_quote(f'wilberflow_{batch_id}')}")
    return "\n".join(lines) + ("\n" if config_toml.endswith("\n") else "")


def _resolve_batch_workspace(
    workspace_root_text: str,
    batch_mode: str,
    batch_id_text: str,
    *,
    create_new_if_missing: bool,
) -> tuple[Path, Path, str]:
    base_root = resolve_user_path(workspace_root_text)
    normalized_mode = batch_mode.strip().lower()
    requested_batch_id = batch_id_text.strip()
    if normalized_mode not in {"new", "existing"}:
        return base_root, base_root, requested_batch_id
    resolved_batch_id = requested_batch_id or (_generate_batch_id() if create_new_if_missing else "")
    if not resolved_batch_id:
        raise ValueError("missing batch_id for existing batch")
    return base_root, _batch_dir_path(base_root, resolved_batch_id), resolved_batch_id


def _network_catalog_cache_path() -> Path:
    return _project_root() / ".wilberflow-studio" / f"network_catalog_v{NETWORK_CATALOG_CACHE_VERSION}.json"


def workflow_state() -> dict[str, object]:
    with WORKFLOW_STATE_LOCK:
        return dict(WORKFLOW_STATE)


def update_workflow_state(**updates: object) -> dict[str, object]:
    with WORKFLOW_STATE_LOCK:
        WORKFLOW_STATE.update(updates)
        return dict(WORKFLOW_STATE)


def _blank_stage_progress(stage_sequence: list[dict[str, str]]) -> dict[str, dict[str, object]]:
    return {
        stage["key"]: {
            "status": "pending",
            "current": 0,
            "total": None,
            "note": "",
            "stats": {},
        }
        for stage in stage_sequence
    }


def _merged_stage_progress(
    stage_sequence: list[dict[str, str]],
    stage_progress: dict[str, object] | None,
) -> dict[str, dict[str, object]]:
    merged = _blank_stage_progress(stage_sequence)
    if not isinstance(stage_progress, dict):
        return merged
    for key, value in stage_progress.items():
        if key not in merged or not isinstance(value, dict):
            continue
        merged[key].update(value)
    return merged


def _stage_progress_on_enter(
    stage_sequence: list[dict[str, str]],
    current_progress: dict[str, object] | None,
    stage_key: str,
    note: str,
) -> dict[str, dict[str, object]]:
    merged = _merged_stage_progress(stage_sequence, current_progress)
    for index, stage in enumerate(stage_sequence):
        key = stage["key"]
        if key == stage_key:
            merged[key]["status"] = "running"
            merged[key]["note"] = note
            merged[key]["stats"] = {}
            break
        if merged[key]["status"] == "running":
            merged[key]["status"] = "completed"
            total = merged[key].get("total")
            if isinstance(total, int):
                merged[key]["current"] = total
    return merged


def _stage_progress_update(
    stage_sequence: list[dict[str, str]],
    current_progress: dict[str, object] | None,
    stage_key: str,
    *,
    current: int | None = None,
    total: int | None = None,
    note: str | None = None,
    status: str | None = None,
    stats: dict[str, object] | None = None,
) -> dict[str, dict[str, object]]:
    merged = _merged_stage_progress(stage_sequence, current_progress)
    if stage_key not in merged:
        merged[stage_key] = {"status": "pending", "current": 0, "total": None, "note": "", "stats": {}}
    if current is not None:
        merged[stage_key]["current"] = current
    if total is not None:
        merged[stage_key]["total"] = total
    if note is not None:
        merged[stage_key]["note"] = note
    if status is not None:
        merged[stage_key]["status"] = status
    if stats is not None:
        merged[stage_key]["stats"] = dict(stats)
    return merged


def _complete_stage_progress(
    stage_sequence: list[dict[str, str]],
    current_progress: dict[str, object] | None,
    stage_key: str,
) -> dict[str, dict[str, object]]:
    merged = _merged_stage_progress(stage_sequence, current_progress)
    if stage_key in merged:
        merged[stage_key]["status"] = "completed"
        total = merged[stage_key].get("total")
        if isinstance(total, int):
            merged[stage_key]["current"] = total
    return merged


def _fail_stage_progress(
    stage_sequence: list[dict[str, str]],
    current_progress: dict[str, object] | None,
    stage_key: str,
    note: str,
) -> dict[str, dict[str, object]]:
    merged = _merged_stage_progress(stage_sequence, current_progress)
    if stage_key in merged:
        merged[stage_key]["status"] = "failed"
        merged[stage_key]["note"] = note
    return merged


def workflow_running() -> bool:
    global WORKFLOW_THREAD
    return WORKFLOW_THREAD is not None and WORKFLOW_THREAD.is_alive()


def _dataset_label(dataset: str) -> str:
    if dataset == "custom":
        return "Custom Query"
    if dataset == "month0":
        return "Past 30 days, all magnitudes"
    if dataset == "month":
        return "Past 30 days, M3.0+"
    if dataset == "year":
        return "Past 12 months, M5.0+"
    if dataset == "all":
        return "Since 1990, M6.0+"
    return f"{dataset}, M5.0+"


def build_dataset_options(now: datetime | None = None) -> list[dict[str, str]]:
    current = now or _utc_now()
    options = [
        {"value": "custom", "label": _dataset_label("custom")},
        {"value": "month0", "label": _dataset_label("month0")},
        {"value": "month", "label": _dataset_label("month")},
        {"value": "year", "label": _dataset_label("year")},
        {"value": "all", "label": _dataset_label("all")},
    ]
    for year in range(current.year, 1989, -1):
        options.append({"value": str(year), "label": _dataset_label(str(year))})
    return options


def resolve_dataset_query(dataset: str, now: datetime | None = None) -> dict[str, object] | None:
    current = now or _utc_now()
    today = current.replace(microsecond=0)
    if dataset == "custom":
        return None
    if dataset == "month0":
        return {
            "starttime": (today - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S"),
            "endtime": today.strftime("%Y-%m-%dT%H:%M:%S"),
            "minmagnitude": 0.0,
        }
    if dataset == "month":
        return {
            "starttime": (today - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S"),
            "endtime": today.strftime("%Y-%m-%dT%H:%M:%S"),
            "minmagnitude": 3.0,
        }
    if dataset == "year":
        return {
            "starttime": (today - timedelta(days=366)).strftime("%Y-%m-%dT%H:%M:%S"),
            "endtime": today.strftime("%Y-%m-%dT%H:%M:%S"),
            "minmagnitude": 5.0,
        }
    if dataset == "all":
        return {
            "starttime": "1990-01-01T00:00:00",
            "endtime": today.strftime("%Y-%m-%dT%H:%M:%S"),
            "minmagnitude": 6.0,
        }
    year = int(dataset)
    return {
        "starttime": f"{year:04d}-01-01T00:00:00",
        "endtime": f"{year + 1:04d}-01-01T00:00:00",
        "minmagnitude": 5.0,
    }


def _single_param(params: dict[str, list[str]], key: str, default: str = "") -> str:
    values = params.get(key)
    if not values:
        return default
    return values[0].strip()


def _maybe_float(value: str) -> float | None:
    if not value:
        return None
    numeric = float(value)
    return numeric


def _maybe_int(value: str) -> int | None:
    if not value:
        return None
    numeric = int(value)
    return numeric


def build_search_query(params: dict[str, list[str]]) -> tuple[dict[str, object], str]:
    dataset = _single_param(params, "dataset", "custom") or "custom"
    query = resolve_dataset_query(dataset) or {}

    custom_fields = [
        "starttime",
        "endtime",
        "minmagnitude",
        "maxmagnitude",
        "mindepth",
        "maxdepth",
        "minlatitude",
        "maxlatitude",
        "minlongitude",
        "maxlongitude",
    ]
    for field in custom_fields:
        value = _single_param(params, field)
        if not value:
            continue
        query[field] = value if field.endswith("time") else float(value)

    query.setdefault("orderby", "time-asc")
    query["output"] = "text"
    query["limit"] = min(max(_maybe_int(_single_param(params, "limit", "200")) or 200, 1), 2000)
    return query, dataset


def search_events(params: dict[str, list[str]]) -> dict[str, object]:
    query, dataset = build_search_query(params)
    opener = build_http_opener()
    text = http_get_text(opener, DEFAULT_EVENT_SERVICE_URL, params=query, timeout=30)
    events = parse_event_service_text(text)
    return {
        "dataset": dataset,
        "query": query,
        "count": len(events),
        "events": [
            {
                "event_id": event.event_id,
                "event_key": event.event_key,
                "output_event_id": event.output_event_id,
                "event_time_utc": str(event.event_time),
                "latitude": event.latitude,
                "longitude": event.longitude,
                "depth_km": event.depth_km,
                "magnitude": event.magnitude,
                "magnitude_type": event.magnitude_type,
                "description": event.description,
                "catalog": event.catalog,
                "author": event.author,
            }
            for event in events
        ],
    }


def station_options(event_time_text: str) -> dict[str, object]:
    event_time = UTCDateTime(event_time_text)
    opener = build_http_opener()
    stations = fetch_station_records_for_time(opener, DEFAULT_WILBER_BASE_URL, event_time, timeout=30)
    virtual_networks = fetch_virtual_networks_for_time(opener, DEFAULT_WILBER_BASE_URL, event_time, timeout=30)

    physical_networks: dict[str, dict[str, object]] = {}
    virtual_counts: dict[str, int] = {}
    available_channels: set[str] = set()
    station_values: set[str] = set()

    for station in stations:
        station_values.add(station.station)
        station_values.add(station.station_id)
        network = physical_networks.setdefault(
            station.network,
            {"value": station.network, "name": station.network_name, "count": 0},
        )
        network["count"] = int(network["count"]) + 1
        for virtual_code in station.virtual_networks:
            virtual_counts[virtual_code] = virtual_counts.get(virtual_code, 0) + 1
        for location in station.locations:
            available_channels.update(location.channels)

    virtual_options = []
    for code, network in sorted(virtual_networks.items()):
        count = virtual_counts.get(code, 0)
        if count <= 0:
            continue
        virtual_options.append(
            {
                "value": code,
                "label": f"{code}: {network['name']} ({count})",
                "title": network["name"],
                "count": count,
            }
        )

    physical_options = [
        {
            "value": code,
            "label": f"{code}: {network['name']} ({network['count']})",
            "title": network["name"],
            "count": network["count"],
        }
        for code, network in sorted(physical_networks.items())
        if int(network["count"]) > 0
    ]

    return {
        "event_time_utc": str(event_time),
        "station_count": len(stations),
        "network_groups": [
            {"label": "Virtual Networks", "options": virtual_options},
            {"label": "Networks", "options": physical_options},
        ],
        "channel_groups": CHANNEL_OPTIONS,
        "available_channels": sorted(available_channels),
        "station_suggestions": sorted(station_values)[:1500],
    }


def _network_catalog_from_cache(limit: int) -> dict[str, object] | None:
    cache_path = _network_catalog_cache_path()
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    catalog = payload.get("catalog")
    if payload.get("version") != NETWORK_CATALOG_CACHE_VERSION or not isinstance(catalog, list):
        return None
    limited_catalog = catalog[:limit]
    return {
        "generated_at_utc": payload.get("generated_at_utc"),
        "sample_times_utc": payload.get("sample_times_utc", NETWORK_CATALOG_SAMPLE_TIMES_UTC),
        "warnings": payload.get("warnings", []),
        "catalog": limited_catalog,
        "entry_count": len(limited_catalog),
        "catalog_size_total": len(catalog),
        "limit": limit,
        "cache_path": str(cache_path),
        "cached": True,
    }


def network_catalog(limit: int = NETWORK_CATALOG_LIMIT, refresh: bool = False) -> dict[str, object]:
    safe_limit = min(max(int(limit), 1), 1000)
    if not refresh:
        cached = _network_catalog_from_cache(safe_limit)
        if cached is not None:
            return cached

    opener = build_http_opener()
    appearances: Counter[tuple[str, str]] = Counter()
    metadata: dict[tuple[str, str], dict[str, object]] = {}
    warnings: list[str] = []

    for event_time_text in NETWORK_CATALOG_SAMPLE_TIMES_UTC:
        event_time = UTCDateTime(event_time_text)
        try:
            station_tree_text = http_get_text(
                opener,
                f"{DEFAULT_WILBER_BASE_URL}/services/stations_for_time/{format_ws_datetime(event_time)}",
                timeout=30,
            )
            raw_stations = parse_station_tree_text(station_tree_text)
            virtual_networks = fetch_virtual_networks_for_time(opener, DEFAULT_WILBER_BASE_URL, event_time, timeout=30)
            stations = attach_virtual_networks(raw_stations, virtual_networks)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"{event_time_text}: {exc}")
            continue

        sample_hits: set[tuple[str, str]] = set()
        for station in stations:
            if station.network:
                key = ("physical", station.network)
                sample_hits.add(key)
                metadata.setdefault(
                    key,
                    {
                        "value": station.network,
                        "name": station.network_name or station.network,
                        "kind": "physical",
                    },
                )
            for code in station.virtual_networks:
                if not code:
                    continue
                key = ("virtual", code)
                sample_hits.add(key)
                network_name = str(virtual_networks.get(code, {}).get("name", code))
                metadata.setdefault(
                    key,
                    {
                        "value": code,
                        "name": network_name or code,
                        "kind": "virtual",
                    },
                )
        appearances.update(sample_hits)

    catalog = [
        {
            **metadata[key],
            "label": f"{metadata[key]['value']}: {metadata[key]['name']}",
            "sample_hits": appearances[key],
        }
        for key in metadata
    ]
    catalog.sort(key=lambda item: (-int(item["sample_hits"]), 0 if item["kind"] == "physical" else 1, str(item["value"])))

    if not catalog:
        raise RuntimeError("no network catalog entries could be collected from Wilber")

    payload = {
        "version": NETWORK_CATALOG_CACHE_VERSION,
        "generated_at_utc": _utc_now().isoformat().replace("+00:00", "Z"),
        "sample_times_utc": NETWORK_CATALOG_SAMPLE_TIMES_UTC,
        "warnings": warnings,
        "catalog": catalog,
    }
    write_json(_network_catalog_cache_path(), payload)

    return {
        "generated_at_utc": payload["generated_at_utc"],
        "sample_times_utc": payload["sample_times_utc"],
        "warnings": warnings,
        "catalog": catalog[:safe_limit],
        "entry_count": min(safe_limit, len(catalog)),
        "catalog_size_total": len(catalog),
        "limit": safe_limit,
        "cache_path": str(_network_catalog_cache_path()),
        "cached": False,
    }


def ui_metadata() -> dict[str, object]:
    return {
        "defaults": {
            "networks": DEFAULT_NETWORKS,
            "channels": DEFAULT_CHANNELS,
            "workspace_root": str(_default_workspace_root()),
            "batch_id": _generate_batch_id(),
        },
        "event_datasets": build_dataset_options(),
        "timewindow_before_options_min": TIMEWINDOW_BEFORE_OPTIONS,
        "timewindow_after_options_min": TIMEWINDOW_AFTER_OPTIONS,
        "channel_groups": CHANNEL_OPTIONS,
        "output_format_options": OUTPUT_FORMAT_OPTIONS,
        "bundle_options": BUNDLE_OPTIONS,
        "network_catalog_limit": NETWORK_CATALOG_LIMIT,
        "platform_support": {
            "service_runtime": "WSL/Linux",
            "windows_native_supported": False,
            "windows_mount_example": "/mnt/d/Data/Wilber/workflow_run",
        },
    }


def _write_runtime_config(workspace_root: Path, config_toml: str) -> Path:
    config_path = workspace_root / ".wilberflow-studio" / "runtime_config.toml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(config_toml, encoding="utf-8")
    return config_path


def _run_workflow_in_background(
    workspace_base_root: Path,
    workspace_root: Path,
    batch_id: str,
    config_toml: str,
    request_email: str,
    qq_imap_auth_code: str,
    mode: str = "run_all",
) -> None:
    global WORKFLOW_THREAD

    def worker() -> None:
        try:
            if mode == "resume_from_mail":
                config_path = workspace_root / ".wilberflow-studio" / "runtime_config.toml"
                if not config_path.exists():
                    raise FileNotFoundError(f"missing runtime config for resume: {config_path}")
            else:
                config_path = _write_runtime_config(workspace_root, _with_batched_request_label_prefix(config_toml, batch_id))
            logger = setup_logger(workspace_root / "logs" / "pipeline.log", logger_name=f"wilberflow-runner-{workspace_root}")
            update_workflow_state(
                status="running",
                message="本地流程运行中" if mode == "run_all" else "正在补跑收信与下载后半程",
                batch_id=batch_id,
                workspace_base_root=str(workspace_base_root),
                workspace_root=str(workspace_root),
                mail_expected_count=0,
                mail_received_count=0,
                mail_pending_count=0,
                mail_progress_note="",
                started_at_utc=_utc_now().isoformat().replace("+00:00", "Z"),
                finished_at_utc=None,
                log_path=str(workspace_root / "logs" / "pipeline.log"),
            )
            if request_email:
                os.environ["QQ_IMAP_USER"] = request_email
            if qq_imap_auth_code:
                os.environ["QQ_IMAP_AUTH_CODE"] = qq_imap_auth_code
            pipeline_config = load_config(config_path)
            stage_sequence = workflow_stage_sequence(pipeline_config, mode=mode)
            stage_progress = _blank_stage_progress(stage_sequence)

            def stage_callback(stage_key: str, stage_message: str) -> None:
                current_state = workflow_state()
                update_workflow_state(
                    status="running",
                    message=stage_message,
                    mode=mode,
                    stage_sequence=stage_sequence,
                    stage_progress=_stage_progress_on_enter(
                        stage_sequence,
                        current_state.get("stage_progress") if isinstance(current_state.get("stage_progress"), dict) else stage_progress,
                        stage_key,
                        stage_message,
                    ),
                    current_stage_key=stage_key,
                    batch_id=batch_id,
                    workspace_base_root=str(workspace_base_root),
                )

            def mail_progress_callback(expected_count: int, received_count: int, pending_count: int, note: str) -> None:
                current_state = workflow_state()
                update_workflow_state(
                    status="running",
                    mode=mode,
                    stage_sequence=stage_sequence,
                    stage_progress=_stage_progress_update(
                        stage_sequence,
                        current_state.get("stage_progress") if isinstance(current_state.get("stage_progress"), dict) else stage_progress,
                        "mail",
                        current=received_count,
                        total=expected_count,
                        note=note,
                        status="running",
                    ),
                    current_stage_key="mail",
                    batch_id=batch_id,
                    workspace_base_root=str(workspace_base_root),
                    mail_expected_count=expected_count,
                    mail_received_count=received_count,
                    mail_pending_count=pending_count,
                    mail_progress_note=note,
                )

            def stage_progress_callback(
                stage_key: str,
                current: int | None,
                total: int | None,
                note: str | None,
                status: str | None,
                stats: dict[str, object] | None = None,
            ) -> None:
                current_state = workflow_state()
                update_workflow_state(
                    status="running",
                    mode=mode,
                    stage_sequence=stage_sequence,
                    stage_progress=_stage_progress_update(
                        stage_sequence,
                        current_state.get("stage_progress") if isinstance(current_state.get("stage_progress"), dict) else stage_progress,
                        stage_key,
                        current=current,
                        total=total,
                        note=note,
                        status=status,
                        stats=stats,
                    ),
                    current_stage_key=current_state.get("current_stage_key", "") if isinstance(current_state.get("current_stage_key", ""), str) else "",
                    batch_id=batch_id,
                    workspace_base_root=str(workspace_base_root),
                )

            prepare_workspace(workspace_root, config_path, logger)
            update_workflow_state(
                status="running",
                message="工作目录已准备，正在启动流程",
                mode=mode,
                stage_sequence=stage_sequence,
                stage_progress=stage_progress,
                current_stage_key="",
                batch_id=batch_id,
                workspace_base_root=str(workspace_base_root),
                mail_expected_count=0,
                mail_received_count=0,
                mail_pending_count=0,
                mail_progress_note="",
            )
            if mode == "resume_from_mail":
                run_resume_from_mail(
                    workspace_root,
                    pipeline_config,
                    logger,
                    stage_callback=stage_callback,
                    mail_progress_callback=mail_progress_callback,
                    stage_progress_callback=stage_progress_callback,
                )
            else:
                run_all(
                    workspace_root,
                    pipeline_config,
                    logger,
                    stage_callback=stage_callback,
                    mail_progress_callback=mail_progress_callback,
                    stage_progress_callback=stage_progress_callback,
                )
            final_progress = _complete_stage_progress(
                stage_sequence,
                workflow_state().get("stage_progress") if isinstance(workflow_state().get("stage_progress"), dict) else stage_progress,
                stage_sequence[-1]["key"] if stage_sequence else "",
            )
            update_workflow_state(
                status="completed",
                message="流程已完成" if mode == "run_all" else "补跑已完成",
                mode=mode,
                stage_sequence=stage_sequence,
                stage_progress=final_progress,
                current_stage_key=stage_sequence[-1]["key"] if stage_sequence else "",
                batch_id=batch_id,
                workspace_base_root=str(workspace_base_root),
                finished_at_utc=_utc_now().isoformat().replace("+00:00", "Z"),
                log_path=str(workspace_root / "logs" / "pipeline.log"),
            )
        except Exception as exc:  # noqa: BLE001
            current_state = workflow_state()
            current_stage_key = str(current_state.get("current_stage_key", "") or "")
            update_workflow_state(
                status="failed",
                message=f"流程失败: {exc}",
                mode=mode,
                stage_progress=_fail_stage_progress(
                    current_state.get("stage_sequence") if isinstance(current_state.get("stage_sequence"), list) else [],
                    current_state.get("stage_progress") if isinstance(current_state.get("stage_progress"), dict) else {},
                    current_stage_key,
                    str(exc),
                ),
                batch_id=batch_id,
                workspace_base_root=str(workspace_base_root),
                finished_at_utc=_utc_now().isoformat().replace("+00:00", "Z"),
                log_path=str(workspace_root / "logs" / "pipeline.log"),
            )

    WORKFLOW_THREAD = threading.Thread(target=worker, name="wilberflow-studio-runner", daemon=True)
    WORKFLOW_THREAD.start()


class WilberStudioHandler(SimpleHTTPRequestHandler):
    server_version = "WilberFlowStudio/0.1"

    def __init__(self, *args, directory: str | None = None, logger=None, **kwargs):
        self._logger = logger
        super().__init__(*args, directory=directory, **kwargs)

    def log_message(self, format: str, *args) -> None:
        if self._logger is not None:
            self._logger.info("studio %s", format % args)

    def _send_json(self, payload: dict[str, object], status: int = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_error_json(self, status: int, message: str) -> None:
        self._send_json({"error": message, "status": status}, status=status)

    def _read_json_body(self) -> dict[str, object]:
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON body: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError("JSON body must be an object")
        return payload

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/health":
            self._send_json({"ok": True})
            return
        if parsed.path == "/api/workflow/status":
            self._send_json({"state": workflow_state()})
            return
        if parsed.path == "/api/workflow/batches":
            params = parse_qs(parsed.query)
            workspace_root_text = _single_param(params, "workspace_root", str(_default_workspace_root()))
            try:
                base_root = resolve_user_path(workspace_root_text)
                self._send_json(
                    {
                        "workspace_base_root": str(base_root),
                        "batches": _list_batch_directories(base_root),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                self._send_error_json(HTTPStatus.BAD_GATEWAY, f"batch listing failed: {exc}")
            return
        if parsed.path == "/api/wilber/ui-metadata":
            self._send_json(ui_metadata())
            return
        if parsed.path == "/api/wilber/network-catalog":
            params = parse_qs(parsed.query)
            refresh = _single_param(params, "refresh").lower() in {"1", "true", "yes"}
            limit = _maybe_int(_single_param(params, "limit", str(NETWORK_CATALOG_LIMIT))) or NETWORK_CATALOG_LIMIT
            try:
                self._send_json(network_catalog(limit=limit, refresh=refresh))
            except Exception as exc:  # noqa: BLE001
                self._send_error_json(HTTPStatus.BAD_GATEWAY, f"network catalog failed: {exc}")
            return
        if parsed.path == "/api/wilber/search-events":
            try:
                self._send_json(search_events(parse_qs(parsed.query)))
            except Exception as exc:  # noqa: BLE001
                self._send_error_json(HTTPStatus.BAD_GATEWAY, f"event search failed: {exc}")
            return
        if parsed.path == "/api/wilber/station-options":
            params = parse_qs(parsed.query)
            event_time = _single_param(params, "event_time")
            if not event_time:
                self._send_error_json(HTTPStatus.BAD_REQUEST, "missing event_time query parameter")
                return
            try:
                self._send_json(station_options(event_time))
            except Exception as exc:  # noqa: BLE001
                self._send_error_json(HTTPStatus.BAD_GATEWAY, f"station options failed: {exc}")
            return
        if parsed.path == "/":
            self.path = "/index.html"
        return super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/workflow/run":
            try:
                payload = self._read_json_body()
                workspace_root_text = str(payload.get("workspace_root", "")).strip() or str(_default_workspace_root())
                batch_mode = str(payload.get("batch_mode", "new")).strip() or "new"
                batch_id_text = str(payload.get("batch_id", "")).strip()
                config_toml = str(payload.get("config_toml", ""))
                request_email = str(payload.get("request_email", "")).strip()
                qq_imap_auth_code = str(payload.get("qq_imap_auth_code", ""))
                if not config_toml.strip():
                    self._send_error_json(HTTPStatus.BAD_REQUEST, "missing config_toml")
                    return
                if workflow_running():
                    self._send_error_json(HTTPStatus.CONFLICT, "another workflow is already running")
                    return
                workspace_base_root, workspace_root, resolved_batch_id = _resolve_batch_workspace(
                    workspace_root_text,
                    batch_mode,
                    batch_id_text,
                    create_new_if_missing=True,
                )
                update_workflow_state(
                    status="queued",
                    message="流程已提交，等待启动",
                    mode="run_all",
                    stage_sequence=[],
                    stage_progress={},
                    current_stage_key="",
                    batch_id=resolved_batch_id,
                    workspace_base_root=str(workspace_base_root),
                    mail_expected_count=0,
                    mail_received_count=0,
                    mail_pending_count=0,
                    mail_progress_note="",
                    workspace_root=str(workspace_root),
                    started_at_utc=None,
                    finished_at_utc=None,
                    log_path=str(workspace_root / "logs" / "pipeline.log"),
                )
                _run_workflow_in_background(
                    workspace_base_root,
                    workspace_root,
                    resolved_batch_id,
                    config_toml,
                    request_email,
                    qq_imap_auth_code,
                )
                self._send_json(
                    {
                        "ok": True,
                        "message": "流程已启动",
                        "batch_id": resolved_batch_id,
                        "workspace_base_root": str(workspace_base_root),
                        "workspace_root": str(workspace_root),
                        "log_path": str(workspace_root / "logs" / "pipeline.log"),
                    }
                )
            except ValueError as exc:
                self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            except Exception as exc:  # noqa: BLE001
                self._send_error_json(HTTPStatus.BAD_GATEWAY, f"workflow start failed: {exc}")
            return
        if parsed.path == "/api/workflow/resume-mail":
            try:
                payload = self._read_json_body()
                workspace_root_text = str(payload.get("workspace_root", "")).strip() or str(_default_workspace_root())
                batch_mode = str(payload.get("batch_mode", "existing")).strip() or "existing"
                batch_id_text = str(payload.get("batch_id", "")).strip()
                request_email = str(payload.get("request_email", "")).strip()
                qq_imap_auth_code = str(payload.get("qq_imap_auth_code", ""))
                if workflow_running():
                    self._send_error_json(HTTPStatus.CONFLICT, "another workflow is already running")
                    return
                workspace_base_root, workspace_root, resolved_batch_id = _resolve_batch_workspace(
                    workspace_root_text,
                    batch_mode,
                    batch_id_text,
                    create_new_if_missing=False,
                )
                runtime_config_path = workspace_root / ".wilberflow-studio" / "runtime_config.toml"
                if not runtime_config_path.exists():
                    self._send_error_json(HTTPStatus.BAD_REQUEST, f"missing runtime config: {runtime_config_path}")
                    return
                update_workflow_state(
                    status="queued",
                    message="补跑流程已提交，等待启动",
                    mode="resume_from_mail",
                    stage_sequence=[],
                    stage_progress={},
                    current_stage_key="",
                    batch_id=resolved_batch_id,
                    workspace_base_root=str(workspace_base_root),
                    mail_expected_count=0,
                    mail_received_count=0,
                    mail_pending_count=0,
                    mail_progress_note="",
                    workspace_root=str(workspace_root),
                    started_at_utc=None,
                    finished_at_utc=None,
                    log_path=str(workspace_root / "logs" / "pipeline.log"),
                )
                _run_workflow_in_background(
                    workspace_base_root,
                    workspace_root,
                    resolved_batch_id,
                    "",
                    request_email,
                    qq_imap_auth_code,
                    mode="resume_from_mail",
                )
                self._send_json(
                    {
                        "ok": True,
                        "message": "补跑流程已启动",
                        "batch_id": resolved_batch_id,
                        "workspace_base_root": str(workspace_base_root),
                        "workspace_root": str(workspace_root),
                        "log_path": str(workspace_root / "logs" / "pipeline.log"),
                    }
                )
            except ValueError as exc:
                self._send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            except Exception as exc:  # noqa: BLE001
                self._send_error_json(HTTPStatus.BAD_GATEWAY, f"resume workflow failed: {exc}")
            return
        self._send_error_json(HTTPStatus.NOT_FOUND, "unknown endpoint")

    def guess_type(self, path: str) -> str:
        if path.endswith(".js"):
            return "application/javascript; charset=utf-8"
        if path.endswith(".css"):
            return "text/css; charset=utf-8"
        guessed, _ = mimetypes.guess_type(path)
        return guessed or "application/octet-stream"


def serve(host: str, port: int, logger) -> None:
    project_root = Path(__file__).resolve().parents[2]
    site_dir = project_root / "site"
    handler = partial(WilberStudioHandler, directory=str(site_dir), logger=logger)
    server = ThreadingHTTPServer((host, port), handler)
    logger.info("wilberflow studio serving at http://%s:%s", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("wilberflow studio stopped")
    finally:
        server.server_close()
