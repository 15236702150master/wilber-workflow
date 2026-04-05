from __future__ import annotations

import csv
import fnmatch
import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from pathlib import Path
from typing import Callable
from urllib.error import URLError

from obspy import UTCDateTime
from obspy.geodetics.base import gps2dist_azimuth, locations2degrees
from obspy.taup import TauPyModel

from .common import (
    build_http_opener,
    format_ws_datetime,
    http_get_text,
    http_post_form,
    load_csv_rows,
    match_any,
    parse_filter_tokens,
    parse_location_priority,
    sanitize_text,
    write_csv,
    write_json,
    write_stage_summary,
)
from .config import EventSearchConfig, PipelineConfig
from .models import CandidateEvent, SelectedStation, StationLocation, StationRecord


EVENT_FIELDNAMES = [
    "EventKey",
    "OutputEventID",
    "WilberEventID",
    "WilberEventTimeUTC",
    "Magnitude",
    "MagnitudeType",
    "Latitude",
    "Longitude",
    "DepthKm",
    "Description",
    "Author",
    "Catalog",
    "Contributor",
    "EventDataJSON",
]

STATION_SELECTION_FIELDNAMES = [
    "EventKey",
    "OutputEventID",
    "WilberEventID",
    "WilberEventTimeUTC",
    "Network",
    "Station",
    "StationID",
    "SelectedLocationCode",
    "SelectedChannels",
    "SelectedChannelCount",
    "MatchingLocationCodes",
    "MatchingChannels",
    "MatchingChannelCount",
    "DataCenter",
    "VirtualNetworks",
    "StaLat",
    "StaLon",
    "ElevationM",
    "DistanceDeg",
    "DistanceKm",
    "Azimuth",
    "BackAzimuth",
]

REQUEST_PLAN_FIELDNAMES = [
    "EventKey",
    "OutputEventID",
    "WilberEventID",
    "WilberEventTimeUTC",
    "RequestLabel",
    "Networks",
    "Stations",
    "Channels",
    "DistanceMinDeg",
    "DistanceMaxDeg",
    "AzimuthMinDeg",
    "AzimuthMaxDeg",
    "WindowStartBeforeMin",
    "WindowStartPhase",
    "WindowEndAfterMin",
    "WindowEndPhase",
    "OutputFormat",
    "Bundle",
    "User",
    "Email",
    "SelectedStationCount",
    "SelectedChannelCount",
    "StationCsvPath",
    "RequestBodyPath",
    "SubmitStatus",
    "TrackURL",
    "SubmitMessage",
    "RequestedAtUTC",
]

StageProgressCallback = Callable[[str, int | None, int | None, str | None, str | None, dict[str, object] | None], None]
STATION_STAGE_MAX_WORKERS = 4
REQUEST_STAGE_MAX_WORKERS = 3
STATION_RAW_CACHE_TTL_SECONDS = 6 * 60 * 60


def _stable_payload_hash(payload: object) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


def _read_stage_summary(stage_dir: Path) -> dict[str, object]:
    summary_path = stage_dir / "summary.json"
    if not summary_path.exists():
        return {}
    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_json_dict(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _utc_now_text() -> str:
    return UTCDateTime().strftime("%Y-%m-%dT%H:%M:%SZ")


def _station_meta_path(stage_dir: Path, event_key: str) -> Path:
    return stage_dir / "per_event_meta" / f"{event_key}.json"


def _request_meta_path(stage_dir: Path, event_key: str) -> Path:
    return stage_dir / "per_event_meta" / f"{event_key}.json"


def _station_raw_cache_path(stage_dir: Path, event_key: str) -> Path:
    return stage_dir / "raw_station_cache" / f"{event_key}.json"


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _http_get_text_with_retry_count(
    opener,
    url: str,
    *,
    timeout: int,
    retry_attempts: int,
    retry_sleep_seconds: float,
) -> tuple[str, int]:
    last_error: Exception | None = None
    retries = 0
    for attempt in range(1, max(1, retry_attempts) + 1):
        try:
            text = http_get_text(
                opener,
                url,
                timeout=timeout,
                retry_attempts=1,
                retry_sleep_seconds=0.0,
            )
            return text, retries
        except (URLError, TimeoutError, OSError) as exc:
            last_error = exc
            retries += 1
            if attempt >= max(1, retry_attempts):
                break
            if retry_sleep_seconds > 0:
                time.sleep(retry_sleep_seconds)
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"unexpected request state for {url}")


def _station_payload_to_records(station_tree_text: str, virtual_networks_text: str) -> list[StationRecord]:
    stations = parse_station_tree_text(station_tree_text)
    virtual_networks = parse_virtual_network_text(virtual_networks_text)
    return attach_virtual_networks(stations, virtual_networks)


def _fetch_station_payload_texts(
    opener,
    base_url: str,
    event_time: UTCDateTime,
    timeout: int,
    *,
    retry_attempts: int = 3,
    retry_sleep_seconds: float = 1.0,
) -> dict[str, object]:
    station_tree_text, station_retries = _http_get_text_with_retry_count(
        opener,
        f"{base_url}/services/stations_for_time/{format_ws_datetime(event_time)}",
        timeout=timeout,
        retry_attempts=retry_attempts,
        retry_sleep_seconds=retry_sleep_seconds,
    )
    virtual_networks_text, vnet_retries = _http_get_text_with_retry_count(
        opener,
        f"{base_url}/services/vnets/{format_ws_datetime(event_time)}",
        timeout=timeout,
        retry_attempts=retry_attempts,
        retry_sleep_seconds=retry_sleep_seconds,
    )
    return {
        "station_tree_text": station_tree_text,
        "virtual_networks_text": virtual_networks_text,
        "retry_count": station_retries + vnet_retries,
    }


def _station_cache_payload_reusable(payload: dict[str, object], event: CandidateEvent, base_url: str) -> bool:
    if not payload:
        return False
    if str(payload.get("event_key", "")) != event.event_key:
        return False
    if _safe_int(payload.get("wilber_event_id")) != event.event_id:
        return False
    if str(payload.get("wilber_event_time_utc", "")) != str(event.event_time):
        return False
    if str(payload.get("wilber_base_url", "")) != base_url:
        return False
    try:
        fetched_at_epoch = float(payload.get("fetched_at_epoch") or 0.0)
    except (TypeError, ValueError):
        return False
    if fetched_at_epoch <= 0:
        return False
    if time.time() - fetched_at_epoch > STATION_RAW_CACHE_TTL_SECONDS:
        return False
    return isinstance(payload.get("station_tree_text"), str) and isinstance(payload.get("virtual_networks_text"), str)


def _station_reuse_result_from_meta(
    event: CandidateEvent,
    meta: dict[str, object],
    per_event_path: Path,
) -> dict[str, object]:
    return {
        "event_key": event.event_key,
        "event": event,
        "selected": [],
        "per_event_path": per_event_path,
        "selected_station_count": _safe_int(meta.get("selected_station_count")),
        "selected_channel_count": _safe_int(meta.get("selected_channel_count")),
        "status": "completed",
        "reused": True,
        "fetch_source": str(meta.get("fetch_source", "reused") or "reused"),
        "retry_count": _safe_int(meta.get("retry_count")),
        "error": "",
    }


def _station_summary_row(event: CandidateEvent, result: dict[str, object]) -> dict[str, object]:
    return {
        "EventKey": event.event_key,
        "OutputEventID": event.output_event_id,
        "WilberEventID": event.event_id,
        "SelectedStationCount": result["selected_station_count"],
        "SelectedChannelCount": result["selected_channel_count"],
        "PerEventCsvPath": str(result["per_event_path"]),
    }


def _request_summary_row_from_meta(meta: dict[str, object]) -> dict[str, object]:
    plan_row = meta.get("plan_row")
    return dict(plan_row) if isinstance(plan_row, dict) else {}


def _request_reuse_result_from_meta(event_key: str, meta: dict[str, object]) -> dict[str, object]:
    plan_row = _request_summary_row_from_meta(meta)
    submit_status = str(plan_row.get("SubmitStatus", "")).strip().lower()
    return {
        "event_key": event_key,
        "plan_row": plan_row,
        "prepared": 1 if submit_status not in {"", "build_failed", "no_station_selected"} else 0,
        "submitted": 1 if submit_status == "submitted" else 0,
        "failed": 0,
        "reused": True,
        "retry_count": _safe_int(meta.get("retry_count")),
        "status": "completed",
        "error": "",
    }


def _station_progress_note(completed: int, total: int, stats: dict[str, int], last_event_key: str = "") -> str:
    parts = [f"已处理 {completed}/{total}"]
    if last_event_key:
        parts.append(last_event_key)
    if stats.get("failed", 0):
        parts.append(f"失败 {stats['failed']}")
    return " · ".join(parts)


def _request_progress_note(completed: int, total: int, stats: dict[str, int], last_event_key: str = "") -> str:
    parts = [f"已处理 {completed}/{total}"]
    if last_event_key:
        parts.append(last_event_key)
    if stats.get("failed", 0):
        parts.append(f"失败 {stats['failed']}")
    return " · ".join(parts)


def parse_event_service_text(text: str) -> list[CandidateEvent]:
    candidates: list[CandidateEvent] = []
    reader = csv.reader(text.splitlines(), delimiter="|")
    for fields in reader:
        if not fields or fields[0].startswith("#") or len(fields) < 13:
            continue
        cleaned = [field.strip() for field in fields]
        magnitude_text = cleaned[10]
        depth_text = cleaned[4]
        candidates.append(
            CandidateEvent(
                event_id=int(cleaned[0]),
                event_time=UTCDateTime(cleaned[1]),
                latitude=float(cleaned[2]),
                longitude=float(cleaned[3]),
                depth_km=float(depth_text) if depth_text else None,
                author=cleaned[5],
                catalog=cleaned[6],
                contributor=cleaned[7],
                contributor_id=cleaned[8],
                magnitude_type=cleaned[9],
                magnitude=float(magnitude_text) if magnitude_text else None,
                magnitude_author=cleaned[11],
                description=cleaned[12],
            )
        )
    return candidates


def parse_station_tree_text(text: str) -> list[StationRecord]:
    stations: list[StationRecord] = []
    current_network_code = ""
    current_network_name = ""
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        fields = line.split("|")
        record_type = fields[0]
        if record_type == "N":
            if len(fields) >= 3:
                current_network_code = fields[1].strip()
                current_network_name = fields[2].strip()
            continue
        if record_type != "S" or len(fields) < 8:
            continue
        station_code = fields[1].strip()
        virtual_networks = tuple(item for item in fields[6].split(",") if item.strip())
        locations: list[StationLocation] = []
        for location_value in fields[8:]:
            if not location_value:
                continue
            loc_parts = location_value.split(":")
            if len(loc_parts) < 4:
                continue
            location_code = loc_parts[0].strip() or "--"
            channels = tuple(item.strip() for item in loc_parts[3].split(",") if item.strip())
            locations.append(
                StationLocation(
                    code=location_code,
                    instrument=loc_parts[1].strip(),
                    depth=loc_parts[2].strip(),
                    channels=channels,
                )
            )
        elevation_text = fields[4].strip()
        stations.append(
            StationRecord(
                network=current_network_code,
                network_name=current_network_name,
                station=station_code,
                station_id=f"{current_network_code}.{station_code}",
                latitude=float(fields[2]),
                longitude=float(fields[3]),
                elevation_m=float(elevation_text) if elevation_text else None,
                name=fields[5].strip(),
                virtual_networks=virtual_networks,
                data_center=fields[7].strip(),
                locations=tuple(locations),
            )
        )
    return stations


def parse_virtual_network_text(text: str) -> dict[str, dict[str, object]]:
    virtual_networks: dict[str, dict[str, object]] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        fields = [field.strip() for field in line.split("|")]
        if len(fields) < 2:
            continue
        code = fields[0]
        if not code:
            continue
        virtual_networks[code] = {
            "code": code,
            "name": fields[1],
            "station_ids": tuple(field for field in fields[2:] if field),
        }
    return virtual_networks


def attach_virtual_networks(
    stations: list[StationRecord],
    virtual_networks: dict[str, dict[str, object]],
) -> list[StationRecord]:
    station_memberships: dict[str, set[str]] = {
        station.station_id: set(station.virtual_networks) for station in stations
    }
    for network_code, network in virtual_networks.items():
        for station_id in network.get("station_ids", ()):
            memberships = station_memberships.get(str(station_id))
            if memberships is not None:
                memberships.add(network_code)
    return [
        replace(station, virtual_networks=tuple(sorted(station_memberships[station.station_id])))
        for station in stations
    ]


def fetch_virtual_networks_for_time(opener, base_url: str, event_time: UTCDateTime, timeout: int) -> dict[str, dict[str, object]]:
    text = http_get_text(
        opener,
        f"{base_url}/services/vnets/{format_ws_datetime(event_time)}",
        timeout=timeout,
        retry_attempts=3,
        retry_sleep_seconds=1.0,
    )
    return parse_virtual_network_text(text)


def fetch_station_records_for_time(
    opener,
    base_url: str,
    event_time: UTCDateTime,
    timeout: int,
    *,
    retry_attempts: int = 3,
    retry_sleep_seconds: float = 1.0,
) -> list[StationRecord]:
    payload = _fetch_station_payload_texts(
        opener,
        base_url,
        event_time,
        timeout,
        retry_attempts=retry_attempts,
        retry_sleep_seconds=retry_sleep_seconds,
    )
    return _station_payload_to_records(
        str(payload["station_tree_text"]),
        str(payload["virtual_networks_text"]),
    )


def station_matches_networks(station: StationRecord, tokens: list[str]) -> bool:
    if not tokens:
        return True
    candidates = [station.network, *station.virtual_networks]
    return any(match_any(candidate, tokens) for candidate in candidates)


def station_matches_patterns(station: StationRecord, tokens: list[str]) -> bool:
    if not tokens:
        return True
    candidates = [station.station, station.station_id]
    return any(match_any(candidate, tokens) for candidate in candidates)


def normalize_event_token(text: str) -> str:
    return "".join(char.lower() for char in text.strip() if char.isalnum())


def signed_angle_deg(value: float) -> float:
    return value if value <= 180.0 else value - 360.0


def event_lookup_tokens(event: CandidateEvent) -> set[str]:
    raw_tokens = {
        str(event.event_id),
        event.output_event_id,
        event.event_key,
        str(event.event_time),
        event.event_time.strftime("%Y-%m-%dT%H:%M:%S"),
        event.event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        event.event_time.strftime("%Y-%m-%d %H:%M:%S"),
        event.event_time.strftime("%Y_%m_%d_%H_%M_%S"),
        event.event_time.strftime("%Y%m%d%H%M%S"),
    }
    return {token for token in (normalize_event_token(item) for item in raw_tokens) if token}


def matching_channel_details(station: StationRecord, channel_patterns: list[str]) -> tuple[tuple[str, ...], tuple[str, ...], int]:
    location_codes: list[str] = []
    channels: list[str] = []
    count = 0
    for location in station.locations:
        matched_here = False
        for channel in location.channels:
            if not channel_patterns or any(fnmatch.fnmatchcase(channel, pattern) for pattern in channel_patterns):
                count += 1
                channels.append(channel)
                matched_here = True
        if matched_here:
            code = "" if location.code == "--" else location.code
            if code not in location_codes:
                location_codes.append(code)
    deduped_channels: list[str] = []
    for channel in channels:
        if channel not in deduped_channels:
            deduped_channels.append(channel)
    return tuple(location_codes), tuple(deduped_channels), count


def select_preferred_location(
    station: StationRecord,
    channel_patterns: list[str],
    location_priority: list[str],
) -> tuple[str, tuple[str, ...], int] | None:
    candidates: list[tuple[int, str, tuple[str, ...]]] = []
    for location in station.locations:
        selected_channels = tuple(
            channel
            for channel in location.channels
            if not channel_patterns or any(fnmatch.fnmatchcase(channel, pattern) for pattern in channel_patterns)
        )
        if not selected_channels:
            continue
        code = "" if location.code == "--" else location.code
        priority_rank = location_priority.index(code) if code in location_priority else len(location_priority) + 1
        candidates.append((priority_rank, code, selected_channels))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]))
    _, code, channels = candidates[0]
    return code, channels, len(channels)


def select_stations_for_event(event: CandidateEvent, stations: list[StationRecord], config) -> list[SelectedStation]:
    network_tokens = parse_filter_tokens(config.networks)
    station_tokens = parse_filter_tokens(config.stations)
    channel_patterns = parse_filter_tokens(config.channels)
    location_priority = parse_location_priority(config.location_priority)

    selected: list[SelectedStation] = []
    for station in stations:
        if not station_matches_networks(station, network_tokens):
            continue
        if not station_matches_patterns(station, station_tokens):
            continue
        location_codes, matching_channels, matching_channel_count = matching_channel_details(station, channel_patterns)
        if matching_channel_count == 0:
            continue
        preferred = select_preferred_location(station, channel_patterns, location_priority)
        if preferred is None:
            continue
        selected_location_code, selected_channels, selected_channel_count = preferred
        distance_deg = float(locations2degrees(event.latitude, event.longitude, station.latitude, station.longitude))
        if not (config.min_distance_deg <= distance_deg <= config.max_distance_deg):
            continue
        distance_m, azimuth, back_azimuth = gps2dist_azimuth(
            event.latitude,
            event.longitude,
            station.latitude,
            station.longitude,
        )
        signed_azimuth = signed_angle_deg(float(azimuth))
        signed_back_azimuth = signed_angle_deg(float(back_azimuth))
        if not (config.min_azimuth_deg <= signed_azimuth <= config.max_azimuth_deg):
            continue
        selected.append(
            SelectedStation(
                event_key=event.event_key,
                output_event_id=event.output_event_id,
                wilber_event_id=event.event_id,
                wilber_event_time=event.event_time,
                station=station,
                distance_deg=distance_deg,
                distance_km=distance_m / 1000.0,
                azimuth=signed_azimuth,
                back_azimuth=signed_back_azimuth,
                selected_location_code=selected_location_code,
                selected_channels=selected_channels,
                selected_channel_count=selected_channel_count,
                matching_location_codes=location_codes,
                matching_channels=matching_channels,
                matching_channel_count=matching_channel_count,
            )
        )
    selected.sort(key=lambda item: (item.distance_deg, item.station.network, item.station.station))
    return selected


def phase_arrival_time(
    model: TauPyModel,
    event: CandidateEvent,
    distance_deg: float,
    phase_name: str,
) -> UTCDateTime:
    if phase_name == "":
        return event.event_time
    phase_map = {
        "P": ["P", "Pdiff", "PKP", "PKIKP"],
        "S": ["S", "Sdiff", "SKS", "SKIKS"],
    }
    depth = 0.0 if event.depth_km is None else event.depth_km
    arrivals = model.get_travel_times(
        source_depth_in_km=depth,
        distance_in_degree=distance_deg,
        phase_list=phase_map[phase_name],
    )
    if not arrivals:
        raise ValueError(f"no {phase_name} arrival available at distance {distance_deg:.3f}")
    return event.event_time + float(arrivals[0].time)


def build_selection_lines(
    event: CandidateEvent,
    selected_stations: list[SelectedStation],
    request_config,
    model: TauPyModel,
) -> list[str]:
    selection_lines: list[str] = []
    for selected in selected_stations:
        start_ref = phase_arrival_time(model, event, selected.distance_deg, request_config.window_start_phase)
        end_ref = phase_arrival_time(model, event, selected.distance_deg, request_config.window_end_phase)
        start_time = start_ref - request_config.window_start_before_min * 60.0
        end_time = end_ref + request_config.window_end_after_min * 60.0
        location_field = selected.selected_location_code or "*"
        selection_lines.append(
            f"{selected.station.network} {selected.station.station} {location_field} {request_config.channels} "
            f"{format_ws_datetime(start_time)} {format_ws_datetime(end_time)}"
        )
    return selection_lines


def build_selection_lines_from_rows(
    event: CandidateEvent,
    selected_rows: list[dict[str, str]],
    request_config,
    model: TauPyModel,
) -> list[str]:
    selection_lines: list[str] = []
    for row in selected_rows:
        distance_deg = float(row["DistanceDeg"])
        start_ref = phase_arrival_time(model, event, distance_deg, request_config.window_start_phase)
        end_ref = phase_arrival_time(model, event, distance_deg, request_config.window_end_phase)
        start_time = start_ref - request_config.window_start_before_min * 60.0
        end_time = end_ref + request_config.window_end_after_min * 60.0
        location_code = row.get("SelectedLocationCode", "--").strip()
        location_field = "*" if location_code in {"", "--"} else location_code
        selection_lines.append(
            f"{row['Network']} {row['Station']} {location_field} {request_config.channels} "
            f"{format_ws_datetime(start_time)} {format_ws_datetime(end_time)}"
        )
    return selection_lines


def build_request_label(prefix: str, event: CandidateEvent, channel_text: str) -> str:
    base = f"{prefix}_{event.output_event_id}_{event.event_id}_{channel_text}"
    return sanitize_text(base, fallback=f"{prefix}_{event.output_event_id}_{event.event_id}")


def query_events(
    config: EventSearchConfig,
    stage_dir: Path,
    logger,
    progress_callback: StageProgressCallback | None = None,
) -> list[CandidateEvent]:
    opener = build_http_opener()
    query_params = dict(config.query)
    query_params.setdefault("output", "text")
    text = ""
    last_error: Exception | None = None
    if progress_callback is not None:
        progress_callback("events", 0, None, "正在请求事件服务", "running")
    for attempt in range(1, config.max_request_attempts + 1):
        try:
            text = http_get_text(
                opener,
                config.event_service_url,
                params=query_params,
                timeout=config.timeout,
                retry_attempts=config.max_request_attempts,
                retry_sleep_seconds=config.sleep_seconds,
            )
            last_error = None
            break
        except (URLError, TimeoutError, OSError) as exc:
            last_error = exc
            logger.warning(
                "event search request failed on attempt %s/%s: %s",
                attempt,
                config.max_request_attempts,
                exc,
            )
            if progress_callback is not None:
                progress_callback(
                    "events",
                    0,
                    None,
                    f"事件服务请求失败，准备重试 {attempt}/{config.max_request_attempts}: {exc}",
                    "running",
                )
            if attempt < config.max_request_attempts:
                time.sleep(config.sleep_seconds)
    if last_error is not None:
        raise last_error
    queried_candidates = parse_event_service_text(text)
    selected_token_map = {
        normalized: token
        for token in config.selected_event_tokens
        if (normalized := normalize_event_token(token))
    }
    matched_selected_tokens: set[str] = set()

    candidates = queried_candidates
    if selected_token_map:
        filtered_candidates: list[CandidateEvent] = []
        requested_tokens = set(selected_token_map)
        for event in queried_candidates:
            event_tokens = event_lookup_tokens(event)
            if event_tokens & requested_tokens:
                filtered_candidates.append(event)
                matched_selected_tokens.update(event_tokens & requested_tokens)
        candidates = filtered_candidates

    if config.limit is not None:
        candidates = candidates[: config.limit]

    logger.info(
        "event search fetched=%s after_selected_filter=%s",
        len(queried_candidates),
        len(candidates),
    )
    rows: list[dict[str, object]] = []
    total_candidates = len(candidates)
    if progress_callback is not None:
        if total_candidates == 0:
            progress_callback("events", 0, 0, "未找到匹配事件", "completed")
        else:
            progress_callback("events", 0, total_candidates, f"已获取 {total_candidates} 个候选事件", "running")

    for index, event in enumerate(candidates, start=1):
        if progress_callback is not None:
            progress_callback("events", index - 1, total_candidates, f"正在整理事件 {index}/{total_candidates}: {event.event_key}", "running")
        rows.append(
            {
                "EventKey": event.event_key,
                "OutputEventID": event.output_event_id,
                "WilberEventID": event.event_id,
                "WilberEventTimeUTC": str(event.event_time),
                "Magnitude": event.magnitude,
                "MagnitudeType": event.magnitude_type,
                "Latitude": event.latitude,
                "Longitude": event.longitude,
                "DepthKm": event.depth_km,
                "Description": event.description,
                "Author": event.author,
                "Catalog": event.catalog,
                "Contributor": event.contributor,
                "EventDataJSON": json.dumps(event.event_data_payload(), ensure_ascii=False),
            }
        )
        if progress_callback is not None:
            progress_callback("events", index, total_candidates, f"已整理 {index}/{total_candidates}: {event.event_key}", "running")

    write_csv(stage_dir / "events.csv", EVENT_FIELDNAMES, rows)
    write_stage_summary(
        stage_dir,
        {
            "queried_event_count": len(queried_candidates),
            "selected_event_token_count": len(selected_token_map),
            "matched_selected_event_token_count": len(matched_selected_tokens),
            "candidate_after_selected_filter_count": len(candidates),
            "written_event_count": len(rows),
        },
    )
    logger.info(
        "events queried=%s after_selected_filter=%s written=%s",
        len(queried_candidates),
        len(candidates),
        len(rows),
    )
    if progress_callback is not None:
        progress_callback("events", len(rows), total_candidates, f"事件搜索完成：{len(rows)}/{total_candidates}", "completed")
    return candidates


def load_events_from_csv(path: Path) -> list[CandidateEvent]:
    events: list[CandidateEvent] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            events.append(
                CandidateEvent(
                    event_id=int(row["WilberEventID"]),
                    event_time=UTCDateTime(row["WilberEventTimeUTC"]),
                    latitude=float(row["Latitude"]),
                    longitude=float(row["Longitude"]),
                    depth_km=float(row["DepthKm"]) if row.get("DepthKm") not in ("", None) else None,
                    author=row.get("Author", ""),
                    catalog=row.get("Catalog", ""),
                    contributor=row.get("Contributor", ""),
                    contributor_id="",
                    magnitude_type=row.get("MagnitudeType", ""),
                    magnitude=float(row["Magnitude"]) if row.get("Magnitude") not in ("", None) else None,
                    magnitude_author="",
                    description=row.get("Description", ""),
                )
            )
    return events


def _worker_count(total_items: int, max_workers: int) -> int:
    if total_items <= 0:
        return 1
    return max(1, min(max_workers, total_items))


def _station_rows_for_selected(selected: list[SelectedStation]) -> list[dict[str, object]]:
    return [
        {
            "EventKey": item.event_key,
            "OutputEventID": item.output_event_id,
            "WilberEventID": item.wilber_event_id,
            "WilberEventTimeUTC": str(item.wilber_event_time),
            "Network": item.station.network,
            "Station": item.station.station,
            "StationID": item.station.station_id,
            "SelectedLocationCode": item.selected_location_code or "--",
            "SelectedChannels": ",".join(item.selected_channels),
            "SelectedChannelCount": item.selected_channel_count,
            "MatchingLocationCodes": ",".join(code or "--" for code in item.matching_location_codes),
            "MatchingChannels": ",".join(item.matching_channels),
            "MatchingChannelCount": item.matching_channel_count,
            "DataCenter": item.station.data_center,
            "VirtualNetworks": ",".join(item.station.virtual_networks),
            "StaLat": item.station.latitude,
            "StaLon": item.station.longitude,
            "ElevationM": item.station.elevation_m,
            "DistanceDeg": round(item.distance_deg, 6),
            "DistanceKm": round(item.distance_km, 3),
            "Azimuth": round(item.azimuth, 3),
            "BackAzimuth": round(item.back_azimuth, 3),
        }
        for item in selected
    ]


def _select_stations_for_single_event(
    event: CandidateEvent,
    pipeline_config: PipelineConfig,
    stage_fingerprint: str,
    stage_dir: Path,
    per_event_dir: Path,
) -> dict[str, object]:
    per_event_path = per_event_dir / f"{event.event_key}.csv"
    meta_path = _station_meta_path(stage_dir, event.event_key)
    cache_path = _station_raw_cache_path(stage_dir, event.event_key)
    retry_count = 0
    fetch_source = "network"
    try:
        cached_payload = _load_json_dict(cache_path)
        if _station_cache_payload_reusable(
            cached_payload,
            event,
            pipeline_config.event_search.wilber_base_url,
        ):
            fetch_source = "cache"
            stations = _station_payload_to_records(
                str(cached_payload["station_tree_text"]),
                str(cached_payload["virtual_networks_text"]),
            )
        else:
            opener = build_http_opener()
            fetched_payload = _fetch_station_payload_texts(
                opener,
                pipeline_config.event_search.wilber_base_url,
                event.event_time,
                pipeline_config.request.timeout,
                retry_attempts=pipeline_config.request.max_request_attempts,
                retry_sleep_seconds=pipeline_config.request.sleep_seconds,
            )
            retry_count = _safe_int(fetched_payload.get("retry_count"))
            cache_payload = {
                "event_key": event.event_key,
                "wilber_event_id": event.event_id,
                "wilber_event_time_utc": str(event.event_time),
                "wilber_base_url": pipeline_config.event_search.wilber_base_url,
                "fetched_at_utc": _utc_now_text(),
                "fetched_at_epoch": time.time(),
                "station_tree_text": str(fetched_payload["station_tree_text"]),
                "virtual_networks_text": str(fetched_payload["virtual_networks_text"]),
            }
            write_json(cache_path, cache_payload)
            stations = _station_payload_to_records(
                cache_payload["station_tree_text"],
                cache_payload["virtual_networks_text"],
            )
        selected = select_stations_for_event(event, stations, pipeline_config.request)
        per_event_rows = _station_rows_for_selected(selected)
        write_csv(per_event_path, STATION_SELECTION_FIELDNAMES, per_event_rows)
        result = {
            "event_key": event.event_key,
            "event": event,
            "selected": selected,
            "per_event_path": per_event_path,
            "selected_station_count": len(selected),
            "selected_channel_count": sum(item.selected_channel_count for item in selected),
            "status": "completed",
            "reused": False,
            "fetch_source": fetch_source,
            "retry_count": retry_count,
            "error": "",
        }
        write_json(
            meta_path,
            {
                "event_key": event.event_key,
                "output_event_id": event.output_event_id,
                "wilber_event_id": event.event_id,
                "wilber_event_time_utc": str(event.event_time),
                "config_fingerprint": stage_fingerprint,
                "status": "completed",
                "updated_at_utc": _utc_now_text(),
                "per_event_csv_path": str(per_event_path),
                "selected_station_count": result["selected_station_count"],
                "selected_channel_count": result["selected_channel_count"],
                "fetch_source": fetch_source,
                "retry_count": retry_count,
            },
        )
        return result
    except Exception as exc:
        write_json(
            meta_path,
            {
                "event_key": event.event_key,
                "output_event_id": event.output_event_id,
                "wilber_event_id": event.event_id,
                "wilber_event_time_utc": str(event.event_time),
                "config_fingerprint": stage_fingerprint,
                "status": "failed",
                "updated_at_utc": _utc_now_text(),
                "per_event_csv_path": str(per_event_path),
                "selected_station_count": 0,
                "selected_channel_count": 0,
                "fetch_source": fetch_source,
                "retry_count": retry_count,
                "error": str(exc),
            },
        )
        return {
            "event_key": event.event_key,
            "event": event,
            "selected": [],
            "per_event_path": per_event_path,
            "selected_station_count": 0,
            "selected_channel_count": 0,
            "status": "failed",
            "reused": False,
            "fetch_source": fetch_source,
            "retry_count": retry_count,
            "error": str(exc),
        }


def _stations_stage_fingerprint(events: list[CandidateEvent], pipeline_config: PipelineConfig) -> str:
    return _stable_payload_hash(
        {
            "events": [
                {
                    "event_key": event.event_key,
                    "event_id": event.event_id,
                    "event_time": str(event.event_time),
                }
                for event in events
            ],
            "request_filter": {
                "networks": pipeline_config.request.networks,
                "stations": pipeline_config.request.stations,
                "channels": pipeline_config.request.channels,
                "location_priority": pipeline_config.request.location_priority,
                "min_distance_deg": pipeline_config.request.min_distance_deg,
                "max_distance_deg": pipeline_config.request.max_distance_deg,
                "min_azimuth_deg": pipeline_config.request.min_azimuth_deg,
                "max_azimuth_deg": pipeline_config.request.max_azimuth_deg,
            },
            "wilber_base_url": pipeline_config.event_search.wilber_base_url,
        }
    )


def _requests_stage_fingerprint(selection_summary_rows: list[dict[str, str]], pipeline_config: PipelineConfig) -> str:
    return _stable_payload_hash(
        {
            "selection_summary": [
                {
                    "EventKey": row.get("EventKey", ""),
                    "SelectedStationCount": row.get("SelectedStationCount", ""),
                    "SelectedChannelCount": row.get("SelectedChannelCount", ""),
                }
                for row in selection_summary_rows
            ],
            "request": {
                "channels": pipeline_config.request.channels,
                "networks": pipeline_config.request.networks,
                "stations": pipeline_config.request.stations,
                "location_priority": pipeline_config.request.location_priority,
                "min_distance_deg": pipeline_config.request.min_distance_deg,
                "max_distance_deg": pipeline_config.request.max_distance_deg,
                "min_azimuth_deg": pipeline_config.request.min_azimuth_deg,
                "max_azimuth_deg": pipeline_config.request.max_azimuth_deg,
                "window_start_before_min": pipeline_config.request.window_start_before_min,
                "window_start_phase": pipeline_config.request.window_start_phase,
                "window_end_after_min": pipeline_config.request.window_end_after_min,
                "window_end_phase": pipeline_config.request.window_end_phase,
                "output_format": pipeline_config.request.output_format,
                "bundle": pipeline_config.request.bundle,
                "user": pipeline_config.request.user,
                "email": pipeline_config.request.email,
                "request_label_prefix": pipeline_config.request.request_label_prefix,
                "submit": pipeline_config.request.submit,
                "sleep_seconds": pipeline_config.request.sleep_seconds,
                "max_request_attempts": pipeline_config.request.max_request_attempts,
                "skip_find_stations_prefetch": pipeline_config.request.skip_find_stations_prefetch,
            },
            "wilber_base_url": pipeline_config.event_search.wilber_base_url,
        }
    )


def _request_row_reusable(plan_row: dict[str, str]) -> bool:
    status = str(plan_row.get("SubmitStatus", "")).strip().lower()
    if status in {"submit_error", "submit_request_failed", "http_error"}:
        return False
    request_body_path = Path(str(plan_row.get("RequestBodyPath", "")).strip())
    return bool(request_body_path) and request_body_path.exists()


def fetch_and_select_stations(
    workspace_root: Path,
    pipeline_config: PipelineConfig,
    logger,
    progress_callback: StageProgressCallback | None = None,
) -> dict[str, list[SelectedStation]]:
    stage_dir = workspace_root / "02_stations"
    per_event_dir = stage_dir / "per_event_selected"
    meta_dir = stage_dir / "per_event_meta"
    cache_dir = stage_dir / "raw_station_cache"
    per_event_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    events = load_events_from_csv(workspace_root / "01_events" / "events.csv")
    selections: dict[str, list[SelectedStation]] = {}
    event_rows: list[dict[str, object]] = []
    selected_event_count = 0
    total_selected_stations = 0
    total_selected_channels = 0
    reused_event_count = 0
    failed_event_count = 0
    cache_hit_count = 0
    fetched_event_count = 0
    retry_count_total = 0
    if progress_callback is not None:
        progress_callback("stations", 0, len(events), "等待筛选台站", "running", {})

    total_events = len(events)
    if total_events == 0:
        write_csv(
            stage_dir / "event_station_summary.csv",
            ["EventKey", "OutputEventID", "WilberEventID", "SelectedStationCount", "SelectedChannelCount", "PerEventCsvPath"],
            [],
        )
        write_stage_summary(
            stage_dir,
            {
                "event_count": 0,
                "events_with_selected_stations": 0,
                "selected_station_total": 0,
                "selected_channel_total": 0,
            },
        )
        if progress_callback is not None:
            progress_callback("stations", 0, 0, "没有可筛选的事件", "completed", {})
        return selections

    stage_fingerprint = _stations_stage_fingerprint(events, pipeline_config)
    existing_summary = _read_stage_summary(stage_dir)
    reuse_allowed = str(existing_summary.get("config_fingerprint", "")) == stage_fingerprint
    existing_rows_by_event = {
        row.get("EventKey", ""): row
        for row in load_csv_rows(stage_dir / "event_station_summary.csv")
        if row.get("EventKey")
    }
    reusable_event_keys: set[str] = set()
    reusable_results: dict[str, dict[str, object]] = {}
    for event in events:
        event_key = event.event_key
        per_event_path = per_event_dir / f"{event_key}.csv"
        meta = _load_json_dict(_station_meta_path(stage_dir, event_key))
        if (
            str(meta.get("status", "")) == "completed"
            and str(meta.get("config_fingerprint", "")) == stage_fingerprint
            and per_event_path.exists()
        ):
            reusable_event_keys.add(event_key)
            reusable_results[event_key] = _station_reuse_result_from_meta(event, meta, per_event_path)
            continue
        if reuse_allowed and event_key in existing_rows_by_event and per_event_path.exists():
            row = existing_rows_by_event[event_key]
            fallback_meta = {
                "selected_station_count": row.get("SelectedStationCount", 0),
                "selected_channel_count": row.get("SelectedChannelCount", 0),
                "fetch_source": "reused",
                "retry_count": 0,
            }
            reusable_event_keys.add(event_key)
            reusable_results[event_key] = _station_reuse_result_from_meta(event, fallback_meta, per_event_path)

    pending_events = [(index, event) for index, event in enumerate(events, start=1) if event.event_key not in reusable_event_keys]
    max_workers = _worker_count(len(pending_events), STATION_STAGE_MAX_WORKERS)
    logger.info(
        "station selection using %s workers for %s pending events (%s reused)",
        max_workers,
        len(pending_events),
        len(reusable_event_keys),
    )
    if progress_callback is not None:
        initial_stats = {
            "succeeded": 0,
            "failed": 0,
            "reused": len(reusable_event_keys),
            "cache_hits": 0,
            "fetched": 0,
            "retry": 0,
        }
        if reusable_event_keys:
            progress_callback(
                "stations",
                len(reusable_event_keys),
                total_events,
                f"复用 {len(reusable_event_keys)} 个已有事件，剩余 {len(pending_events)} 个待处理",
                "running",
                initial_stats,
            )
        else:
            progress_callback("stations", 0, total_events, f"并行筛选台站，worker={max_workers}", "running", initial_stats)

    ordered_results: list[tuple[int, dict[str, object]]] = [
        (
            index,
            reusable_results[event.event_key],
        )
        for index, event in enumerate(events, start=1)
        if event.event_key in reusable_event_keys
    ]
    reused_event_count = len(reusable_event_keys)

    if pending_events:
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="wilber-stations") as executor:
            future_map = {
                executor.submit(_select_stations_for_single_event, event, pipeline_config, stage_fingerprint, stage_dir, per_event_dir): (index, event)
                for index, event in pending_events
            }
            completed = reused_event_count
            progress_stats = {
                "succeeded": 0,
                "failed": 0,
                "reused": reused_event_count,
                "cache_hits": 0,
                "fetched": 0,
                "retry": 0,
            }
            for future in as_completed(future_map):
                index, event = future_map[future]
                logger.info("selecting stations for %s (%s/%s)", event.event_key, index, total_events)
                result = future.result()
                ordered_results.append((index, result))
                completed += 1
                if result["status"] == "failed":
                    progress_stats["failed"] += 1
                else:
                    progress_stats["succeeded"] += 1
                if result["fetch_source"] == "cache":
                    progress_stats["cache_hits"] += 1
                elif result["fetch_source"] == "network":
                    progress_stats["fetched"] += 1
                progress_stats["retry"] += _safe_int(result.get("retry_count"))
                if progress_callback is not None:
                    progress_callback(
                        "stations",
                        completed,
                        total_events,
                        _station_progress_note(completed, total_events, progress_stats, event.event_key),
                        "running",
                        progress_stats,
                    )
    else:
        progress_stats = {
            "succeeded": 0,
            "failed": 0,
            "reused": reused_event_count,
            "cache_hits": 0,
            "fetched": 0,
            "retry": 0,
        }

    ordered_results.sort(key=lambda item: item[0])
    failed_messages: list[str] = []
    for _index, result in ordered_results:
        event = result["event"]
        selected = result["selected"]
        selections[event.event_key] = selected
        if result["status"] == "failed":
            failed_event_count += 1
            failed_messages.append(f"{event.event_key}: {result['error']}")
        if int(result["selected_station_count"]) > 0:
            selected_event_count += 1
        total_selected_stations += int(result["selected_station_count"])
        total_selected_channels += int(result["selected_channel_count"])
        if not result.get("reused") and result["fetch_source"] == "cache":
            cache_hit_count += 1
        elif not result.get("reused") and result["fetch_source"] == "network":
            fetched_event_count += 1
        retry_count_total += _safe_int(result.get("retry_count"))
        event_rows.append(_station_summary_row(event, result))

    write_csv(
        stage_dir / "event_station_summary.csv",
        ["EventKey", "OutputEventID", "WilberEventID", "SelectedStationCount", "SelectedChannelCount", "PerEventCsvPath"],
        event_rows,
    )
    write_stage_summary(
        stage_dir,
        {
            "event_count": len(events),
            "events_with_selected_stations": selected_event_count,
            "selected_station_total": total_selected_stations,
            "selected_channel_total": total_selected_channels,
            "reused_event_count": reused_event_count,
            "failed_event_count": failed_event_count,
            "cache_hit_event_count": cache_hit_count,
            "fetched_event_count": fetched_event_count,
            "retry_count_total": retry_count_total,
            "config_fingerprint": stage_fingerprint,
        },
    )
    if progress_callback is not None:
        progress_callback(
            "stations",
            total_events,
            total_events,
            f"台站筛选完成：{total_events}/{total_events}",
            "completed" if failed_event_count == 0 else "failed",
            {
                "succeeded": total_events - failed_event_count - reused_event_count,
                "failed": failed_event_count,
                "reused": reused_event_count,
                "cache_hits": cache_hit_count,
                "fetched": fetched_event_count,
                "retry": retry_count_total,
            },
        )
    if failed_messages:
        preview = " | ".join(failed_messages[:3])
        if len(failed_messages) > 3:
            preview += f" | 其余 {len(failed_messages) - 3} 个事件见 per_event_meta"
        raise RuntimeError(f"station selection failed for {len(failed_messages)} event(s): {preview}")
    return selections


def submit_request(opener, base_url: str, event: CandidateEvent, request_label: str, selection_lines: list[str], num_channels: int, request_config):
    station_page_url = f"{base_url}/find_stations/{event.event_id}"
    if not request_config.skip_find_stations_prefetch:
        http_get_text(
            opener,
            station_page_url,
            timeout=request_config.timeout,
            retry_attempts=request_config.max_request_attempts,
            retry_sleep_seconds=request_config.sleep_seconds,
        )
    payload = {
        "windowStartBefore": request_config.window_start_before_min,
        "windowStartPhase": request_config.window_start_phase,
        "windowEndAfter": request_config.window_end_after_min,
        "windowEndPhase": request_config.window_end_phase,
        "output": request_config.output_format,
        "bundle": request_config.bundle,
        "user": request_config.user,
        "label": request_label,
        "email": request_config.email,
        "channels": "\n".join(selection_lines),
        "num_channels": num_channels,
        "event_data": json.dumps(event.event_data_payload(), ensure_ascii=False),
    }
    status_code, response_text = http_post_form(
        opener,
        f"{base_url}/submit_data_request",
        payload,
        timeout=request_config.timeout,
        retry_attempts=request_config.max_request_attempts,
        retry_sleep_seconds=request_config.sleep_seconds,
        headers={
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": station_page_url,
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    try:
        response_json = json.loads(response_text)
    except json.JSONDecodeError:
        return "http_error", "", f"non-json response ({status_code}): {response_text[:500]}"
    if response_json.get("status") == "success":
        return "submitted", str(response_json.get("url", "")), ""
    errors = response_json.get("errors", response_json)
    message = json.dumps(errors, ensure_ascii=False) if isinstance(errors, dict) else str(errors)
    return "submit_error", "", message


def _build_request_plan_row(
    row: dict[str, str],
    events: dict[str, CandidateEvent],
    workspace_root: Path,
    pipeline_config: PipelineConfig,
    request_body_dir: Path,
) -> dict[str, object]:
    event_key = row["EventKey"]
    event = events[event_key]
    selected_rows = load_csv_rows(workspace_root / "02_stations" / "per_event_selected" / f"{event_key}.csv")
    station_count = len(selected_rows)
    selected_channel_count = sum(int(item.get("SelectedChannelCount") or "0") for item in selected_rows)
    request_label = build_request_label(pipeline_config.request.request_label_prefix, event, pipeline_config.request.channels)
    request_body_path = request_body_dir / f"{event_key}.txt"

    if station_count == 0:
        request_body_path.write_text("", encoding="utf-8")
        return {
            "event_key": event_key,
            "plan_row": {
                "EventKey": event_key,
                "OutputEventID": event.output_event_id,
                "WilberEventID": event.event_id,
                "WilberEventTimeUTC": str(event.event_time),
                "RequestLabel": request_label,
                "Networks": pipeline_config.request.networks,
                "Stations": pipeline_config.request.stations,
                "Channels": pipeline_config.request.channels,
                "DistanceMinDeg": pipeline_config.request.min_distance_deg,
                "DistanceMaxDeg": pipeline_config.request.max_distance_deg,
                "AzimuthMinDeg": pipeline_config.request.min_azimuth_deg,
                "AzimuthMaxDeg": pipeline_config.request.max_azimuth_deg,
                "WindowStartBeforeMin": pipeline_config.request.window_start_before_min,
                "WindowStartPhase": pipeline_config.request.window_start_phase,
                "WindowEndAfterMin": pipeline_config.request.window_end_after_min,
                "WindowEndPhase": pipeline_config.request.window_end_phase,
                "OutputFormat": pipeline_config.request.output_format,
                "Bundle": pipeline_config.request.bundle,
                "User": pipeline_config.request.user,
                "Email": pipeline_config.request.email,
                "SelectedStationCount": 0,
                "SelectedChannelCount": 0,
                "StationCsvPath": str(workspace_root / "02_stations" / "per_event_selected" / f"{event_key}.csv"),
                "RequestBodyPath": str(request_body_path),
                "SubmitStatus": "no_station_selected",
                "TrackURL": "",
                "SubmitMessage": "no station matched the configured filters",
                "RequestedAtUTC": "",
            },
            "prepared": 0,
            "submitted": 0,
            "failed": 0,
            "retry_count": 0,
            "status": "completed",
            "error": "",
        }

    model = TauPyModel(model="iasp91")
    selection_lines = build_selection_lines_from_rows(event, selected_rows, pipeline_config.request, model)
    request_body_path.write_text("\n".join(selection_lines) + "\n", encoding="utf-8")

    submit_status = "not_submitted"
    track_url = ""
    submit_message = ""
    requested_at = ""
    prepared = 1
    submitted = 0
    failed = 0
    retry_count = 0

    if pipeline_config.request.submit:
        opener = build_http_opener()
        for attempt in range(1, pipeline_config.request.max_request_attempts + 1):
            try:
                submit_status, track_url, submit_message = submit_request(
                    opener=opener,
                    base_url=pipeline_config.event_search.wilber_base_url,
                    event=event,
                    request_label=request_label,
                    selection_lines=selection_lines,
                    num_channels=selected_channel_count,
                    request_config=pipeline_config.request,
                )
                requested_at = UTCDateTime().strftime("%Y-%m-%dT%H:%M:%SZ")
                break
            except (URLError, TimeoutError, OSError) as exc:
                retry_count += 1
                submit_status = "submit_request_failed"
                submit_message = f"attempt_{attempt}: {exc}"
                if attempt < pipeline_config.request.max_request_attempts:
                    time.sleep(max(pipeline_config.request.sleep_seconds, 1.0))
        if submit_status == "submitted":
            submitted = 1
        elif submit_status not in {"not_submitted", "no_station_selected"}:
            failed = 1

    return {
        "event_key": event_key,
        "plan_row": {
            "EventKey": event_key,
            "OutputEventID": event.output_event_id,
            "WilberEventID": event.event_id,
            "WilberEventTimeUTC": str(event.event_time),
            "RequestLabel": request_label,
            "Networks": pipeline_config.request.networks,
            "Stations": pipeline_config.request.stations,
            "Channels": pipeline_config.request.channels,
            "DistanceMinDeg": pipeline_config.request.min_distance_deg,
            "DistanceMaxDeg": pipeline_config.request.max_distance_deg,
            "AzimuthMinDeg": pipeline_config.request.min_azimuth_deg,
            "AzimuthMaxDeg": pipeline_config.request.max_azimuth_deg,
            "WindowStartBeforeMin": pipeline_config.request.window_start_before_min,
            "WindowStartPhase": pipeline_config.request.window_start_phase,
            "WindowEndAfterMin": pipeline_config.request.window_end_after_min,
            "WindowEndPhase": pipeline_config.request.window_end_phase,
            "OutputFormat": pipeline_config.request.output_format,
            "Bundle": pipeline_config.request.bundle,
            "User": pipeline_config.request.user,
            "Email": pipeline_config.request.email,
            "SelectedStationCount": station_count,
            "SelectedChannelCount": selected_channel_count,
            "StationCsvPath": str(workspace_root / "02_stations" / "per_event_selected" / f"{event_key}.csv"),
            "RequestBodyPath": str(request_body_path),
            "SubmitStatus": submit_status,
            "TrackURL": track_url,
            "SubmitMessage": submit_message,
            "RequestedAtUTC": requested_at,
        },
        "prepared": prepared,
        "submitted": submitted,
        "failed": failed,
        "retry_count": retry_count,
        "status": "completed",
        "error": "",
    }


def _build_failed_request_plan_row(
    event_key: str,
    event: CandidateEvent,
    workspace_root: Path,
    pipeline_config: PipelineConfig,
    request_body_dir: Path,
    error: str,
) -> dict[str, object]:
    request_body_path = request_body_dir / f"{event_key}.txt"
    return {
        "event_key": event_key,
        "plan_row": {
            "EventKey": event_key,
            "OutputEventID": event.output_event_id,
            "WilberEventID": event.event_id,
            "WilberEventTimeUTC": str(event.event_time),
            "RequestLabel": build_request_label(pipeline_config.request.request_label_prefix, event, pipeline_config.request.channels),
            "Networks": pipeline_config.request.networks,
            "Stations": pipeline_config.request.stations,
            "Channels": pipeline_config.request.channels,
            "DistanceMinDeg": pipeline_config.request.min_distance_deg,
            "DistanceMaxDeg": pipeline_config.request.max_distance_deg,
            "AzimuthMinDeg": pipeline_config.request.min_azimuth_deg,
            "AzimuthMaxDeg": pipeline_config.request.max_azimuth_deg,
            "WindowStartBeforeMin": pipeline_config.request.window_start_before_min,
            "WindowStartPhase": pipeline_config.request.window_start_phase,
            "WindowEndAfterMin": pipeline_config.request.window_end_after_min,
            "WindowEndPhase": pipeline_config.request.window_end_phase,
            "OutputFormat": pipeline_config.request.output_format,
            "Bundle": pipeline_config.request.bundle,
            "User": pipeline_config.request.user,
            "Email": pipeline_config.request.email,
            "SelectedStationCount": 0,
            "SelectedChannelCount": 0,
            "StationCsvPath": str(workspace_root / "02_stations" / "per_event_selected" / f"{event_key}.csv"),
            "RequestBodyPath": str(request_body_path),
            "SubmitStatus": "build_failed",
            "TrackURL": "",
            "SubmitMessage": error,
            "RequestedAtUTC": "",
        },
        "prepared": 0,
        "submitted": 0,
        "failed": 1,
        "retry_count": 0,
        "status": "failed",
        "error": error,
    }


def _build_request_plan_for_single_event(
    row: dict[str, str],
    events: dict[str, CandidateEvent],
    workspace_root: Path,
    pipeline_config: PipelineConfig,
    request_body_dir: Path,
    stage_dir: Path,
    stage_fingerprint: str,
) -> dict[str, object]:
    event_key = row["EventKey"]
    event = events[event_key]
    meta_path = _request_meta_path(stage_dir, event_key)
    try:
        result = _build_request_plan_row(row, events, workspace_root, pipeline_config, request_body_dir)
    except Exception as exc:
        result = _build_failed_request_plan_row(
            event_key,
            event,
            workspace_root,
            pipeline_config,
            request_body_dir,
            str(exc),
        )
    write_json(
        meta_path,
        {
            "event_key": event_key,
            "output_event_id": event.output_event_id,
            "wilber_event_id": event.event_id,
            "wilber_event_time_utc": str(event.event_time),
            "config_fingerprint": stage_fingerprint,
            "status": result["status"],
            "updated_at_utc": _utc_now_text(),
            "retry_count": _safe_int(result.get("retry_count")),
            "plan_row": result["plan_row"],
            "error": result.get("error", ""),
        },
    )
    return result


def build_requests(
    workspace_root: Path,
    pipeline_config: PipelineConfig,
    logger,
    progress_callback: StageProgressCallback | None = None,
) -> list[dict[str, object]]:
    stage_dir = workspace_root / "03_requests"
    request_body_dir = stage_dir / "request_bodies"
    meta_dir = stage_dir / "per_event_meta"
    request_body_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    events = {event.event_key: event for event in load_events_from_csv(workspace_root / "01_events" / "events.csv")}
    selection_summary_rows = load_csv_rows(workspace_root / "02_stations" / "event_station_summary.csv")
    plan_rows: list[dict[str, object]] = []
    submitted_count = 0
    prepared_count = 0
    failed_count = 0
    reused_request_count = 0
    retry_count_total = 0
    total_rows = len(selection_summary_rows)
    if progress_callback is not None:
        progress_callback("requests", 0, total_rows, "等待生成请求计划", "running", {})
    if total_rows == 0:
        write_csv(stage_dir / "request_plan.csv", REQUEST_PLAN_FIELDNAMES, [])
        write_stage_summary(
            stage_dir,
            {
                "plan_row_count": 0,
                "prepared_request_count": 0,
                "submitted_request_count": 0,
                "request_failure_count": 0,
                "submit_enabled": str(pipeline_config.request.submit).lower(),
            },
        )
        if progress_callback is not None:
            progress_callback("requests", 0, 0, "没有可处理的请求", "completed", {})
        return plan_rows

    stage_fingerprint = _requests_stage_fingerprint(selection_summary_rows, pipeline_config)
    existing_summary = _read_stage_summary(stage_dir)
    reuse_allowed = str(existing_summary.get("config_fingerprint", "")) == stage_fingerprint
    existing_rows_by_event = {
        row.get("EventKey", ""): row
        for row in load_csv_rows(stage_dir / "request_plan.csv")
        if row.get("EventKey")
    }
    reusable_event_keys: set[str] = set()
    reusable_results: dict[str, dict[str, object]] = {}
    for row in selection_summary_rows:
        event_key = row.get("EventKey", "")
        if not event_key:
            continue
        meta = _load_json_dict(_request_meta_path(stage_dir, event_key))
        plan_row = _request_summary_row_from_meta(meta)
        if (
            str(meta.get("status", "")) == "completed"
            and str(meta.get("config_fingerprint", "")) == stage_fingerprint
            and plan_row
            and _request_row_reusable(plan_row)
        ):
            reusable_event_keys.add(event_key)
            reusable_results[event_key] = _request_reuse_result_from_meta(event_key, meta)
            continue
        existing_row = existing_rows_by_event.get(event_key)
        if reuse_allowed and existing_row and _request_row_reusable(existing_row):
            reusable_event_keys.add(event_key)
            fallback_meta = {"plan_row": existing_row, "retry_count": 0}
            reusable_results[event_key] = _request_reuse_result_from_meta(event_key, fallback_meta)

    pending_rows = [(index, row) for index, row in enumerate(selection_summary_rows, start=1) if row.get("EventKey", "") not in reusable_event_keys]
    max_workers = _worker_count(
        len(pending_rows),
        REQUEST_STAGE_MAX_WORKERS if pipeline_config.request.submit else STATION_STAGE_MAX_WORKERS,
    )
    logger.info(
        "request planning using %s workers for %s pending events (%s reused)",
        _worker_count(len(pending_rows), REQUEST_STAGE_MAX_WORKERS if pipeline_config.request.submit else STATION_STAGE_MAX_WORKERS),
        len(pending_rows),
        len(reusable_event_keys),
    )
    if progress_callback is not None:
        initial_stats = {
            "prepared": 0,
            "submitted": 0,
            "failed": 0,
            "reused": len(reusable_event_keys),
            "retry": 0,
        }
        if reusable_event_keys:
            progress_callback(
                "requests",
                len(reusable_event_keys),
                total_rows,
                f"复用 {len(reusable_event_keys)} 个已有请求，剩余 {len(pending_rows)} 个待处理",
                "running",
                initial_stats,
            )
        else:
            progress_callback("requests", 0, total_rows, f"并行生成请求，worker={max_workers}", "running", initial_stats)

    ordered_results: list[tuple[int, dict[str, object]]] = [
        (
            index,
            reusable_results[event_key],
        )
        for index, row in enumerate(selection_summary_rows, start=1)
        for event_key in [row.get("EventKey", "")]
        if event_key in reusable_event_keys
    ]
    reused_request_count = len(reusable_event_keys)

    if pending_rows:
        effective_workers = _worker_count(
            len(pending_rows),
            REQUEST_STAGE_MAX_WORKERS if pipeline_config.request.submit else STATION_STAGE_MAX_WORKERS,
        )
        with ThreadPoolExecutor(max_workers=effective_workers, thread_name_prefix="wilber-requests") as executor:
            future_map = {
                executor.submit(
                    _build_request_plan_for_single_event,
                    row,
                    events,
                    workspace_root,
                    pipeline_config,
                    request_body_dir,
                    stage_dir,
                    stage_fingerprint,
                ): (index, row["EventKey"])
                for index, row in pending_rows
            }
            completed = reused_request_count
            progress_stats = {
                "prepared": 0,
                "submitted": 0,
                "failed": 0,
                "reused": reused_request_count,
                "retry": 0,
            }
            for future in as_completed(future_map):
                index, event_key = future_map[future]
                result = future.result()
                ordered_results.append((index, result))
                completed += 1
                progress_stats["prepared"] += int(result["prepared"])
                progress_stats["submitted"] += int(result["submitted"])
                progress_stats["failed"] += int(result["failed"])
                progress_stats["retry"] += _safe_int(result.get("retry_count"))
                if progress_callback is not None:
                    progress_callback(
                        "requests",
                        completed,
                        total_rows,
                        _request_progress_note(completed, total_rows, progress_stats, event_key),
                        "running",
                        progress_stats,
                    )
    else:
        progress_stats = {
            "prepared": 0,
            "submitted": 0,
            "failed": 0,
            "reused": reused_request_count,
            "retry": 0,
        }

    ordered_results.sort(key=lambda item: item[0])
    failed_messages: list[str] = []
    for _index, result in ordered_results:
        plan_rows.append(result["plan_row"])
        prepared_count += int(result["prepared"])
        submitted_count += int(result["submitted"])
        failed_count += int(result["failed"])
        retry_count_total += _safe_int(result.get("retry_count"))
        if result["status"] == "failed":
            failed_messages.append(f"{result['event_key']}: {result['error']}")

    write_csv(stage_dir / "request_plan.csv", REQUEST_PLAN_FIELDNAMES, plan_rows)
    write_stage_summary(
        stage_dir,
        {
            "plan_row_count": len(plan_rows),
            "prepared_request_count": prepared_count,
            "submitted_request_count": submitted_count,
            "request_failure_count": failed_count,
            "reused_request_count": reused_request_count,
            "retry_count_total": retry_count_total,
            "config_fingerprint": stage_fingerprint,
            "submit_enabled": str(pipeline_config.request.submit).lower(),
        },
    )
    if progress_callback is not None:
        progress_callback(
            "requests",
            len(plan_rows),
            total_rows,
            f"请求阶段完成：{len(plan_rows)}/{total_rows}",
            "completed" if failed_count == 0 else "failed",
            {
                "prepared": prepared_count,
                "submitted": submitted_count,
                "failed": failed_count,
                "reused": reused_request_count,
                "retry": retry_count_total,
            },
        )
    if failed_messages:
        preview = " | ".join(failed_messages[:3])
        if len(failed_messages) > 3:
            preview += f" | 其余 {len(failed_messages) - 3} 个事件见 per_event_meta"
        raise RuntimeError(f"request planning failed for {len(failed_messages)} event(s): {preview}")
    return plan_rows
