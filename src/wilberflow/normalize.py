from __future__ import annotations

import fnmatch
import math
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Callable

import numpy as np
from obspy import Stream, UTCDateTime, read, read_inventory
from obspy.clients.fdsn import Client, RoutingClient
from obspy.geodetics.base import gps2dist_azimuth, locations2degrees
from obspy.io.sac import SACTrace
from obspy.io.sac.util import SacError
from obspy.taup import TauPyModel

from .common import ensure_dir, parse_filter_tokens, parse_pre_filt, write_csv, write_stage_summary
from .models import EventInfo, TraceJob


OUTPUT_UNIT_TO_IDEP = {
    "DISP": "idisp",
    "VEL": "ivel",
    "ACC": "iacc",
}

FALLBACK_FDSN_CLIENT_NAMES = ("EARTHSCOPE",)
StageProgressCallback = Callable[[str, int | None, int | None, str | None, str | None], None]


def discover_events(input_root: Path, selected_ids: set[str] | None = None, limit_events: int | None = None) -> list[EventInfo]:
    events: list[EventInfo] = []
    for event_dir in sorted(path for path in input_root.iterdir() if path.is_dir()):
        parts = event_dir.name.split("_")
        date_parts: list[str] = []
        for part in parts:
            if part.isdigit() and len(part) == 4 and not date_parts:
                date_parts.append(part)
                continue
            if date_parts and part.isdigit() and len(part) == 2 and len(date_parts) < 6:
                date_parts.append(part)
                if len(date_parts) == 6:
                    break
        if len(date_parts) != 6:
            continue
        event_id = "_".join(date_parts)
        if selected_ids and event_id not in selected_ids:
            continue
        event_time = UTCDateTime(
            f"{date_parts[0]}-{date_parts[1]}-{date_parts[2]}T{date_parts[3]}:{date_parts[4]}:{date_parts[5]}"
        )
        events.append(
            EventInfo(
                event_dir=event_dir,
                event_id=event_id,
                event_time=event_time,
                event_label=event_dir.name,
            )
        )
        if limit_events is not None and len(events) >= limit_events:
            break
    return events


def normalize_loc_code(loc: str) -> str:
    return "--" if loc in {"", "--"} else loc


def final_filename(event_time: UTCDateTime, network: str, station: str, channel: str, location_code: str, include_location: bool) -> str:
    location_part = f".{location_code}" if include_location else ""
    return (
        f"{network}.{station}{location_part}."
        f"{event_time.year:04d}.{event_time.julday:03d}.{event_time.hour:02d}.{event_time.minute:02d}.{event_time.second:02d}."
        f"{channel}.sac"
    )


def find_matching_pz(event_dir: Path, sac_path: Path) -> Path | None:
    parts = sac_path.name.split(".")
    if len(parts) < 4:
        raise ValueError(f"unexpected SAC filename format: {sac_path.name}")
    network, station, loc, channel = parts[0], parts[1], normalize_loc_code(parts[2]), parts[3]
    candidates = sorted(event_dir.rglob(f"SACPZ.{network}.{station}.{loc}.{channel}"))
    if not candidates:
        return None

    # Some Wilber bundles ship duplicate pole-zero files in parallel data-center
    # folders. When the exact response basename is duplicated, using the first
    # deterministic match is safer than aborting the whole batch.
    return candidates[0]


def build_jobs(events: list[EventInfo], output_root: Path, overwrite: bool, channel_patterns: list[str]):
    jobs: list[TraceJob] = []
    skipped_channels: list[dict[str, object]] = []
    for event in events:
        event_output_dir = output_root / event.event_id
        grouped_paths: dict[tuple[str, str, str, str], list[Path]] = {}
        for sac_path in sorted(event.event_dir.glob("*.SAC")):
            trace = read(str(sac_path), headonly=True)[0]
            channel = trace.stats.channel
            if channel_patterns and not any(fnmatch.fnmatchcase(channel, pattern) for pattern in channel_patterns):
                skipped_channels.append(
                    {
                        "EventID": event.event_id,
                        "EventLabel": event.event_label,
                        "InputPath": str(sac_path),
                        "Channel": channel,
                        "Reason": "channel_not_selected",
                    }
                )
                continue
            key = (
                trace.stats.network,
                trace.stats.station,
                normalize_loc_code(trace.stats.location or ""),
                channel,
            )
            grouped_paths.setdefault(key, []).append(sac_path)

        base_counts: dict[tuple[str, str, str], int] = {}
        for network, station, _location, channel in grouped_paths:
            base_key = (network, station, channel)
            base_counts[base_key] = base_counts.get(base_key, 0) + 1

        for (network, station, location_code, channel), sac_paths in sorted(grouped_paths.items()):
            include_location = base_counts[(network, station, channel)] > 1
            output_path = event_output_dir / final_filename(
                event_time=event.event_time,
                network=network,
                station=station,
                channel=channel,
                location_code=location_code,
                include_location=include_location,
            )
            if output_path.exists() and not overwrite:
                continue
            jobs.append(
                TraceJob(
                    event=event,
                    sac_paths=tuple(sac_paths),
                    pz_path=find_matching_pz(event.event_dir, sac_paths[0]),
                    output_path=output_path,
                    network=network,
                    station=station,
                    location_code=location_code,
                    channel=channel,
                )
            )
    return jobs, skipped_channels


def read_and_merge_raw_traces(sac_paths: tuple[Path, ...]):
    stream = Stream()
    for sac_path in sac_paths:
        stream += read(str(sac_path))
    if len(stream) == 1:
        return stream[0]
    try:
        stream.merge(method=1, fill_value="interpolate")
    except Exception:
        stream.sort(keys=["npts"])
        return stream[-1]
    if len(stream) == 1:
        return stream[0]
    stream.sort(keys=["npts"])
    return stream[-1]


def get_phase_time(model: TauPyModel, depth_km: float, distance_deg: float, phase_names: list[str]) -> float | None:
    arrivals = model.get_travel_times(
        source_depth_in_km=depth_km,
        distance_in_degree=distance_deg,
        phase_list=phase_names,
    )
    if not arrivals:
        return None
    return float(arrivals[0].time)


def build_geometry(trace, event_time: UTCDateTime) -> dict[str, float | None]:
    sac_header = getattr(trace.stats, "sac", None)
    event_lat = float(getattr(sac_header, "evla", math.nan))
    event_lon = float(getattr(sac_header, "evlo", math.nan))
    event_depth_km = float(getattr(sac_header, "evdp", math.nan))
    station_lat = float(getattr(sac_header, "stla", math.nan))
    station_lon = float(getattr(sac_header, "stlo", math.nan))
    if not any(math.isnan(value) for value in [event_lat, event_lon, station_lat, station_lon]):
        distance_m, azimuth, back_azimuth = gps2dist_azimuth(event_lat, event_lon, station_lat, station_lon)
        distance_deg = locations2degrees(event_lat, event_lon, station_lat, station_lon)
        distance_km = distance_m / 1000.0
    else:
        azimuth = float(getattr(sac_header, "az", math.nan))
        back_azimuth = float(getattr(sac_header, "baz", math.nan))
        distance_deg = float(getattr(sac_header, "gcarc", math.nan))
        distance_km = float(getattr(sac_header, "dist", math.nan))
    return {
        "event_lat": None if math.isnan(event_lat) else event_lat,
        "event_lon": None if math.isnan(event_lon) else event_lon,
        "event_depth_km": None if math.isnan(event_depth_km) else event_depth_km,
        "station_lat": None if math.isnan(station_lat) else station_lat,
        "station_lon": None if math.isnan(station_lon) else station_lon,
        "distance_deg": None if math.isnan(distance_deg) else distance_deg,
        "distance_km": None if math.isnan(distance_km) else distance_km,
        "azimuth": None if math.isnan(azimuth) else azimuth,
        "back_azimuth": None if math.isnan(back_azimuth) else back_azimuth,
        "start_offset_sec": float(trace.stats.starttime - event_time),
    }


def run_sac_transfer(raw_trace, pz_path: Path, pre_filt: tuple[float, float, float, float], output_unit: str):
    if shutil.which("sac") is None:
        return False, "sac command not found", None
    with tempfile.TemporaryDirectory(prefix="wilber_norm_") as temp_dir_text:
        temp_dir = Path(temp_dir_text)
        temp_sac = temp_dir / "work.sac"
        raw_trace.write(str(temp_sac), format="SAC")
        macro = (
            f"r {temp_sac}\n"
            "rtr\n"
            "taper\n"
            f"trans from polezero s {pz_path} to {output_unit.lower()} freq {pre_filt[0]} {pre_filt[1]} {pre_filt[2]} {pre_filt[3]}\n"
            "wh\n"
            "q\n"
        )
        proc = subprocess.run(["sac"], input=macro, text=True, capture_output=True, cwd=temp_dir, check=False)
        if proc.returncode != 0:
            return False, f"sac transfer failed: {proc.stderr.strip() or proc.stdout.strip()}", None
        try:
            trace = read(str(temp_sac))[0]
        except Exception as exc:
            return False, f"failed to read SAC output: {exc}", None
        return True, proc.stdout.strip(), trace


def remove_response_with_inventory(raw_trace, inventory, pre_filt: tuple[float, float, float, float], output_unit: str):
    processed = raw_trace.copy()
    processed.detrend("demean")
    processed.detrend("linear")
    processed.taper(max_percentage=0.05, type="hann")
    processed.remove_response(
        inventory=inventory,
        output=output_unit,
        water_level=None,
        pre_filt=pre_filt,
        zero_mean=False,
        taper=False,
    )
    processed.data = np.asarray(processed.data, dtype=np.float32)
    return processed


def inventory_channel_count(inventory) -> int:
    return sum(len(station.channels) for network in inventory for station in network)


def inventory_cache_filename(raw_trace) -> str:
    loc = normalize_loc_code(raw_trace.stats.location or "")
    start_tag = raw_trace.stats.starttime.strftime("%Y%m%dT%H%M%S")
    return f"{raw_trace.stats.network}.{raw_trace.stats.station}.{loc}.{raw_trace.stats.channel}.{start_tag}.stationxml"


def inventory_query_locations(raw_trace) -> list[str]:
    location = raw_trace.stats.location or ""
    loc = normalize_loc_code(location)
    candidates = [location]
    if loc == "--":
        candidates.extend(["--", ""])
    else:
        candidates.extend([loc, ""])
    candidates.append("*")

    unique: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        if item in seen:
            continue
        seen.add(item)
        unique.append(item)
    return unique


def load_cached_inventory(cache_path: Path):
    if not cache_path.exists():
        return None
    try:
        inventory = read_inventory(str(cache_path))
    except Exception:
        return None
    if inventory_channel_count(inventory) <= 0:
        return None
    return inventory


def fetch_inventory_from_clients(raw_trace, routing_client: RoutingClient, fdsn_clients: tuple[Client, ...]):
    query_attempts: list[str] = []
    clients: list[tuple[str, object]] = [("routing", routing_client)]
    clients.extend((client.base_url, client) for client in fdsn_clients)

    for location in inventory_query_locations(raw_trace):
        params = {
            "network": raw_trace.stats.network,
            "station": raw_trace.stats.station,
            "location": location,
            "channel": raw_trace.stats.channel,
            "starttime": raw_trace.stats.starttime,
            "endtime": raw_trace.stats.endtime,
            "level": "response",
        }
        for client_name, client in clients:
            try:
                inventory = client.get_stations(**params)
            except Exception as exc:
                query_attempts.append(f"{client_name}[loc={location!r}]={exc}")
                continue
            channel_count = inventory_channel_count(inventory)
            if channel_count <= 0:
                query_attempts.append(f"{client_name}[loc={location!r}]=empty")
                continue
            return True, f"{client_name}[loc={location!r}]", inventory, query_attempts

    detail = "; ".join(query_attempts[-8:]) if query_attempts else "no inventory attempts were made"
    return False, detail, None, query_attempts


def fetch_or_load_inventory(raw_trace, cache_dir: Path, routing_client: RoutingClient, fdsn_clients: tuple[Client, ...]):
    cache_path = cache_dir / inventory_cache_filename(raw_trace)
    cached_inventory = load_cached_inventory(cache_path)
    if cached_inventory is not None:
        return True, f"cached inventory: {cache_path.name}", cached_inventory, cache_path

    ok, source_detail, inventory, _attempts = fetch_inventory_from_clients(raw_trace, routing_client, fdsn_clients)
    if not ok or inventory is None:
        return False, source_detail, None, cache_path

    ensure_dir(cache_dir)
    try:
        inventory.write(str(cache_path), format="STATIONXML")
    except Exception as exc:
        return True, f"{source_detail}; inventory cache write failed: {exc}", inventory, cache_path
    return True, f"{source_detail}; cached as {cache_path.name}", inventory, cache_path


def run_iris_fallback(
    raw_trace,
    pre_filt: tuple[float, float, float, float],
    output_unit: str,
    routing_client: RoutingClient,
    fdsn_clients: tuple[Client, ...],
    cache_dir: Path,
):
    ok, detail, inventory, cache_path = fetch_or_load_inventory(raw_trace, cache_dir, routing_client, fdsn_clients)
    if not ok or inventory is None:
        cache_text = f" ({cache_path.name})" if cache_path is not None else ""
        return False, f"IRIS inventory fetch failed{cache_text}: {detail}", None, cache_path
    try:
        processed = remove_response_with_inventory(raw_trace, inventory, pre_filt, output_unit)
    except Exception as exc:
        return False, f"IRIS remove_response failed: {exc}", None, cache_path
    return True, f"fallback via IRIS inventory ({detail})", processed, cache_path


def write_final_sac(trace, output_path: Path, event: EventInfo, geometry: dict[str, float | None], output_unit: str, taup_model: TauPyModel) -> None:
    sac = SACTrace.from_obspy_trace(trace, keep_sac_header=False)
    sac.reftime = event.event_time
    sac.b = geometry["start_offset_sec"]
    sac.o = 0.0
    try:
        sac.iztype = "io"
    except SacError:
        pass
    if geometry["event_lat"] is not None:
        sac.evla = geometry["event_lat"]
    if geometry["event_lon"] is not None:
        sac.evlo = geometry["event_lon"]
    if geometry["event_depth_km"] is not None:
        sac.evdp = geometry["event_depth_km"]
    sac.knetwk = trace.stats.network
    sac.kstnm = trace.stats.station
    sac.kcmpnm = trace.stats.channel
    sac.khole = trace.stats.location or "--"
    if geometry["station_lat"] is not None:
        sac.stla = geometry["station_lat"]
    if geometry["station_lon"] is not None:
        sac.stlo = geometry["station_lon"]
    if geometry["distance_km"] is not None:
        sac.dist = geometry["distance_km"]
    if geometry["azimuth"] is not None:
        sac.az = geometry["azimuth"]
    if geometry["back_azimuth"] is not None:
        sac.baz = geometry["back_azimuth"]
    if geometry["distance_deg"] is not None:
        sac.gcarc = geometry["distance_deg"]
    sac.lcalda = True
    sac.kevnm = event.event_id[:16]
    sac.idep = OUTPUT_UNIT_TO_IDEP[output_unit]
    if geometry["event_depth_km"] is not None and geometry["distance_deg"] is not None:
        depth = geometry["event_depth_km"]
        distance = geometry["distance_deg"]
        t0 = get_phase_time(taup_model, depth, distance, ["P", "Pdiff", "PKP", "PKIKP"])
        if t0 is not None:
            sac.t0 = t0
            sac.kt0 = "P"
        t2 = get_phase_time(taup_model, depth, distance, ["pP"])
        if t2 is not None:
            sac.t2 = t2
            sac.kt2 = "pP"
        t3 = get_phase_time(taup_model, depth, distance, ["sP"])
        if t3 is not None:
            sac.t3 = t3
            sac.kt3 = "sP"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    sac.write(str(output_path))


def process_job(
    job: TraceJob,
    pre_filt: tuple[float, float, float, float],
    output_unit: str,
    taup_model: TauPyModel,
    routing_client: RoutingClient,
    fdsn_clients: tuple[Client, ...],
    inventory_cache_dir: Path,
    response_backend: str,
):
    try:
        raw_trace = read_and_merge_raw_traces(job.sac_paths)
    except Exception as exc:
        return False, {"Reason": f"failed to read raw SAC: {exc}", "Method": "none"}
    geometry = build_geometry(raw_trace, job.event.event_time)
    base_info = {
        "EventID": job.event.event_id,
        "EventLabel": job.event.event_label,
        "InputSAC": ";".join(str(path) for path in job.sac_paths),
        "InputSACCount": len(job.sac_paths),
        "MatchedSACPZ": "" if job.pz_path is None else str(job.pz_path),
        "OutputPath": str(job.output_path),
        "Network": raw_trace.stats.network,
        "Station": raw_trace.stats.station,
        "LocationCode": raw_trace.stats.location or "--",
        "Channel": raw_trace.stats.channel,
        "SampleRateHz": f"{raw_trace.stats.sampling_rate:.6f}",
        "Npts": raw_trace.stats.npts,
        "DistanceDeg": "" if geometry["distance_deg"] is None else f"{geometry['distance_deg']:.6f}",
        "InventoryFile": "",
    }
    method = "none"
    reason = ""
    processed_trace = None
    inventory_path: Path | None = None

    if response_backend == "obspy_only":
        ok, detail, processed_trace, inventory_path = run_iris_fallback(
            raw_trace,
            pre_filt,
            output_unit,
            routing_client,
            fdsn_clients,
            inventory_cache_dir,
        )
        method = "iris_fallback"
        if not ok:
            return False, {**base_info, "Method": method, "Reason": detail, "InventoryFile": "" if inventory_path is None else str(inventory_path)}
    else:
        if job.pz_path is not None:
            ok, detail, processed_trace = run_sac_transfer(raw_trace, job.pz_path, pre_filt, output_unit)
            method = "local_sac"
            if not ok:
                reason = detail
                ok, detail, processed_trace, inventory_path = run_iris_fallback(
                    raw_trace,
                    pre_filt,
                    output_unit,
                    routing_client,
                    fdsn_clients,
                    inventory_cache_dir,
                )
                method = "iris_fallback"
                if not ok:
                    combined_reason = f"{reason}; {detail}" if reason else detail
                    return False, {**base_info, "Method": method, "Reason": combined_reason, "InventoryFile": "" if inventory_path is None else str(inventory_path)}
        else:
            reason = "SACPZ not found; fallback to IRIS inventory"
            ok, detail, processed_trace, inventory_path = run_iris_fallback(
                raw_trace,
                pre_filt,
                output_unit,
                routing_client,
                fdsn_clients,
                inventory_cache_dir,
            )
            method = "iris_fallback"
            if not ok:
                combined_reason = f"{reason}; {detail}" if reason else detail
                return False, {**base_info, "Method": method, "Reason": combined_reason, "InventoryFile": "" if inventory_path is None else str(inventory_path)}

    processed_trace.data = np.asarray(processed_trace.data, dtype=np.float32)
    write_final_sac(processed_trace, job.output_path, job.event, geometry, output_unit, taup_model)
    return True, {**base_info, "Method": method, "Reason": reason, "InventoryFile": "" if inventory_path is None else str(inventory_path)}


def normalize_workspace(
    workspace_root: Path,
    pipeline_config,
    logger,
    progress_callback: StageProgressCallback | None = None,
) -> None:
    stage_dir = workspace_root / "07_final"
    output_root = stage_dir / "events"
    inventory_cache_dir = stage_dir / "response_inventory"
    output_root.mkdir(parents=True, exist_ok=True)

    pre_filt = parse_pre_filt(pipeline_config.normalize.pre_filt)
    response_backend = (pipeline_config.normalize.response_backend or "local_sac_first").strip().lower()
    if response_backend not in {"local_sac_first", "obspy_only"}:
        raise ValueError(f"unsupported normalize.response_backend: {pipeline_config.normalize.response_backend}")
    selected_ids = {item.strip() for item in pipeline_config.normalize.selected_event_ids if item.strip()}
    events = discover_events(
        workspace_root / "06_extract" / "raw",
        selected_ids=selected_ids,
        limit_events=pipeline_config.normalize.limit_events,
    )
    channel_patterns = parse_filter_tokens(pipeline_config.request.channels)
    jobs, skipped_channels = build_jobs(events, output_root, pipeline_config.normalize.overwrite, channel_patterns)

    taup_model = TauPyModel(model="iasp91")
    routing_client = RoutingClient(pipeline_config.normalize.routing_type)
    fdsn_clients = tuple(Client(name) for name in FALLBACK_FDSN_CLIENT_NAMES)

    success_rows: list[dict[str, object]] = []
    failure_rows: list[dict[str, object]] = []
    iris_fallback_success = 0
    if progress_callback is not None:
        progress_callback("response", 0, len(jobs), "等待去仪器响应", "running")
    for index, job in enumerate(jobs, start=1):
        if progress_callback is not None:
            progress_callback("response", index - 1, len(jobs), f"正在处理 {index}/{len(jobs)}: {job.network}.{job.station}.{job.channel}", "running")
        logger.info("[%s/%s] normalizing %s.%s.%s", index, len(jobs), job.network, job.station, job.channel)
        ok, info = process_job(
            job,
            pre_filt,
            pipeline_config.normalize.output_unit,
            taup_model,
            routing_client,
            fdsn_clients,
            inventory_cache_dir,
            response_backend,
        )
        if ok:
            success_rows.append(info)
            if info["Method"] == "iris_fallback":
                iris_fallback_success += 1
        else:
            failure_rows.append(info)
        if progress_callback is not None:
            progress_callback("response", index, len(jobs), f"已处理 {index}/{len(jobs)}: {job.network}.{job.station}.{job.channel}", "running")

    write_csv(
        stage_dir / "processing_summary.csv",
        [
            "EventID",
            "EventLabel",
            "InputSAC",
            "InputSACCount",
            "MatchedSACPZ",
            "OutputPath",
            "Network",
            "Station",
            "LocationCode",
            "Channel",
            "SampleRateHz",
            "Npts",
            "DistanceDeg",
            "InventoryFile",
            "Method",
            "Reason",
        ],
        success_rows,
    )
    write_csv(
        stage_dir / "processing_failures.csv",
        [
            "EventID",
            "EventLabel",
            "InputSAC",
            "InputSACCount",
            "MatchedSACPZ",
            "OutputPath",
            "Network",
            "Station",
            "LocationCode",
            "Channel",
            "SampleRateHz",
            "Npts",
            "DistanceDeg",
            "InventoryFile",
            "Method",
            "Reason",
        ],
        failure_rows,
    )
    write_csv(
        stage_dir / "skipped_extra_channels.csv",
        ["EventID", "EventLabel", "InputPath", "Channel", "Reason"],
        skipped_channels,
    )
    write_stage_summary(
        stage_dir,
        {
            "event_count": len(events),
            "selected_event_ids_count": len(selected_ids),
            "limit_events": "" if pipeline_config.normalize.limit_events is None else pipeline_config.normalize.limit_events,
            "response_backend": response_backend,
            "trace_job_count": len(jobs),
            "success_count": len(success_rows),
            "failure_count": len(failure_rows),
            "iris_fallback_success": iris_fallback_success,
            "inventory_cache_file_count": len(list(inventory_cache_dir.glob("*.stationxml"))) if inventory_cache_dir.exists() else 0,
            "skipped_extra_channel_files": len(skipped_channels),
        },
    )
    if progress_callback is not None:
        progress_callback("response", len(jobs), len(jobs), f"去仪器响应完成：{len(jobs)}/{len(jobs)}", "completed")
