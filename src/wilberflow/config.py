from __future__ import annotations

import re
import shutil
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from .common import DEFAULT_EVENT_SERVICE_URL, DEFAULT_WILBER_BASE_URL


@dataclass(frozen=True)
class EventSearchConfig:
    event_service_url: str = DEFAULT_EVENT_SERVICE_URL
    wilber_base_url: str = DEFAULT_WILBER_BASE_URL
    query: dict[str, object] = field(default_factory=dict)
    limit: int | None = None
    selected_event_tokens: tuple[str, ...] = ()
    timeout: int = 30
    max_request_attempts: int = 3
    sleep_seconds: float = 1.0


@dataclass(frozen=True)
class RequestConfig:
    channels: str = "BHZ"
    networks: str = ""
    stations: str = ""
    location_priority: str = "00,--,10"
    min_distance_deg: float = 35.0
    max_distance_deg: float = 95.0
    min_azimuth_deg: float = -180.0
    max_azimuth_deg: float = 180.0
    window_start_before_min: int = 2
    window_start_phase: str = "P"
    window_end_after_min: int = 5
    window_end_phase: str = "P"
    output_format: str = "sacbl"
    bundle: str = "tar"
    user: str = "Your Name"
    email: str = ""
    request_label_prefix: str = "wilberflow"
    submit: bool = False
    metadata_only: bool = False
    skip_find_stations_prefetch: bool = False
    timeout: int = 30
    sleep_seconds: float = 0.3
    max_request_attempts: int = 5


@dataclass(frozen=True)
class MailConfig:
    imap_host: str = "imap.qq.com"
    imap_port: int = 993
    imap_timeout_seconds: int = 30
    imap_user_env: str = "QQ_IMAP_USER"
    imap_password_env: str = "QQ_IMAP_AUTH_CODE"
    mailbox: str = "INBOX"
    subject_substring: str = "[Success]"
    from_substring: str = "wilber"
    poll_interval_seconds: int = 30
    max_wait_minutes: int = 90
    message_lookback_hours: int = 24
    max_messages: int = 1500
    prefer_https: bool = True


@dataclass(frozen=True)
class DownloadConfig:
    overwrite: bool = False
    chunk_size_bytes: int = 1024 * 1024
    timeout: int = 120


@dataclass(frozen=True)
class NormalizeConfig:
    pre_filt: str = "0.002,0.005,2.0,4.0"
    output_unit: str = "VEL"
    routing_type: str = "earthscope-federator"
    response_backend: str = "local_sac_first"
    overwrite: bool = False
    selected_event_ids: tuple[str, ...] = ()
    limit_events: int | None = None


@dataclass(frozen=True)
class PipelineConfig:
    event_search: EventSearchConfig
    request: RequestConfig
    mail: MailConfig
    download: DownloadConfig
    normalize: NormalizeConfig


def _section(data: dict[str, object], key: str) -> dict[str, object]:
    value = data.get(key, {})
    if not isinstance(value, dict):
        raise ValueError(f"section [{key}] must be a table")
    return value


def _optional_positive_int(value: object) -> int | None:
    if value is None:
        return None
    numeric = int(value)
    return None if numeric <= 0 else numeric


def _string_items(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        raw_items = re.split(r"[\n,;]+", value)
    elif isinstance(value, (list, tuple)):
        raw_items = [str(item) for item in value]
    else:
        raise ValueError(f"expected string or list of strings, got: {type(value)!r}")
    return tuple(item.strip() for item in raw_items if str(item).strip())


def load_config(path: Path) -> PipelineConfig:
    with path.open("rb") as handle:
        data = tomllib.load(handle)

    event_search_data = _section(data, "event_search")
    request_data = _section(data, "request")
    mail_data = _section(data, "mail")
    download_data = _section(data, "download")
    normalize_data = _section(data, "normalize")

    query = event_search_data.get("query", {})
    if not isinstance(query, dict):
        raise ValueError("[event_search.query] must be a table")

    return PipelineConfig(
        event_search=EventSearchConfig(
            event_service_url=str(event_search_data.get("event_service_url", DEFAULT_EVENT_SERVICE_URL)),
            wilber_base_url=str(event_search_data.get("wilber_base_url", DEFAULT_WILBER_BASE_URL)),
            query=dict(query),
            limit=_optional_positive_int(event_search_data.get("limit")),
            selected_event_tokens=_string_items(event_search_data.get("selected_event_tokens")),
            timeout=int(event_search_data.get("timeout", 30)),
            max_request_attempts=int(event_search_data.get("max_request_attempts", 3)),
            sleep_seconds=float(event_search_data.get("sleep_seconds", 1.0)),
        ),
        request=RequestConfig(
            channels=str(request_data.get("channels", "BHZ")),
            networks=str(request_data.get("networks", "")),
            stations=str(request_data.get("stations", "")),
            location_priority=str(request_data.get("location_priority", "00,--,10")),
            min_distance_deg=float(request_data.get("min_distance_deg", 35.0)),
            max_distance_deg=float(request_data.get("max_distance_deg", 95.0)),
            min_azimuth_deg=float(request_data.get("min_azimuth_deg", -180.0)),
            max_azimuth_deg=float(request_data.get("max_azimuth_deg", 180.0)),
            window_start_before_min=int(request_data.get("window_start_before_min", 2)),
            window_start_phase=str(request_data.get("window_start_phase", "P")),
            window_end_after_min=int(request_data.get("window_end_after_min", 5)),
            window_end_phase=str(request_data.get("window_end_phase", "P")),
            output_format=str(request_data.get("output_format", "sacbl")),
            bundle=str(request_data.get("bundle", "tar")),
            user=str(request_data.get("user", "Your Name")),
            email=str(request_data.get("email", "")),
            request_label_prefix=str(request_data.get("request_label_prefix", "wilberflow")),
            submit=bool(request_data.get("submit", False)),
            metadata_only=bool(request_data.get("metadata_only", False)),
            skip_find_stations_prefetch=bool(request_data.get("skip_find_stations_prefetch", False)),
            timeout=int(request_data.get("timeout", 30)),
            sleep_seconds=float(request_data.get("sleep_seconds", 0.3)),
            max_request_attempts=int(request_data.get("max_request_attempts", 5)),
        ),
        mail=MailConfig(
            imap_host=str(mail_data.get("imap_host", "imap.qq.com")),
            imap_port=int(mail_data.get("imap_port", 993)),
            imap_timeout_seconds=int(mail_data.get("imap_timeout_seconds", 30)),
            imap_user_env=str(mail_data.get("imap_user_env", "QQ_IMAP_USER")),
            imap_password_env=str(mail_data.get("imap_password_env", "QQ_IMAP_AUTH_CODE")),
            mailbox=str(mail_data.get("mailbox", "INBOX")),
            subject_substring=str(mail_data.get("subject_substring", "[Success]")),
            from_substring=str(mail_data.get("from_substring", "wilber")),
            poll_interval_seconds=int(mail_data.get("poll_interval_seconds", 30)),
            max_wait_minutes=int(mail_data.get("max_wait_minutes", 90)),
            message_lookback_hours=int(mail_data.get("message_lookback_hours", 24)),
            max_messages=int(mail_data.get("max_messages", 1500)),
            prefer_https=bool(mail_data.get("prefer_https", True)),
        ),
        download=DownloadConfig(
            overwrite=bool(download_data.get("overwrite", False)),
            chunk_size_bytes=int(download_data.get("chunk_size_bytes", 1024 * 1024)),
            timeout=int(download_data.get("timeout", 120)),
        ),
        normalize=NormalizeConfig(
            pre_filt=str(normalize_data.get("pre_filt", "0.002,0.005,2.0,4.0")),
            output_unit=str(normalize_data.get("output_unit", "VEL")),
            routing_type=str(normalize_data.get("routing_type", "earthscope-federator")),
            response_backend=str(normalize_data.get("response_backend", "local_sac_first")),
            overwrite=bool(normalize_data.get("overwrite", False)),
            selected_event_ids=_string_items(normalize_data.get("selected_event_ids")),
            limit_events=_optional_positive_int(normalize_data.get("limit_events")),
        ),
    )


def copy_config_into_workspace(config_path: Path, workspace_root: Path) -> Path:
    target = workspace_root / "00_config" / "copied_config.toml"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(config_path, target)
    return target
