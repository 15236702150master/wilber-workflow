from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from obspy import UTCDateTime


@dataclass(frozen=True)
class CandidateEvent:
    event_id: int
    event_time: UTCDateTime
    latitude: float
    longitude: float
    depth_km: float | None
    author: str
    catalog: str
    contributor: str
    contributor_id: str
    magnitude_type: str
    magnitude: float | None
    magnitude_author: str
    description: str

    @property
    def output_event_id(self) -> str:
        return self.event_time.strftime("%Y_%m_%d_%H_%M_%S")

    @property
    def event_key(self) -> str:
        return f"{self.output_event_id}_{self.event_id}"

    def event_data_payload(self) -> dict[str, object]:
        return {
            "id": self.event_id,
            "time": self.event_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "latitude": self.latitude,
            "longitude": self.longitude,
            "depth": self.depth_km,
            "magnitude": self.magnitude,
            "description": self.description,
            "author": self.author,
            "catalog": self.catalog,
            "contributor": self.contributor,
            "type": self.magnitude_type,
        }


@dataclass(frozen=True)
class StationLocation:
    code: str
    instrument: str
    depth: str
    channels: tuple[str, ...]


@dataclass(frozen=True)
class StationRecord:
    network: str
    network_name: str
    station: str
    station_id: str
    latitude: float
    longitude: float
    elevation_m: float | None
    name: str
    virtual_networks: tuple[str, ...]
    data_center: str
    locations: tuple[StationLocation, ...]


@dataclass(frozen=True)
class SelectedStation:
    event_key: str
    output_event_id: str
    wilber_event_id: int
    wilber_event_time: UTCDateTime
    station: StationRecord
    distance_deg: float
    distance_km: float
    azimuth: float
    back_azimuth: float
    selected_location_code: str
    selected_channels: tuple[str, ...]
    selected_channel_count: int
    matching_location_codes: tuple[str, ...]
    matching_channels: tuple[str, ...]
    matching_channel_count: int


@dataclass(frozen=True)
class MailMatch:
    request_label: str
    download_url: str
    original_download_url: str
    subject: str
    from_addr: str
    message_date_raw: str
    message_date_utc: str
    message_date_asia_shanghai: str
    message_uid: str
    request_event_key: str
    requested_at_utc: str
    consistency_status: str
    consistency_note: str


@dataclass(frozen=True)
class ExtractionSummary:
    package_name: str
    tar_path: str
    extracted_dir: str
    top_level_dir: str
    member_count: int
    sac_file_count: int
    sacpz_file_count: int
    data_center_dirs: str
    extracted: str
    note: str


@dataclass(frozen=True)
class EventInfo:
    event_dir: Path
    event_id: str
    event_time: UTCDateTime
    event_label: str


@dataclass(frozen=True)
class TraceJob:
    event: EventInfo
    sac_paths: tuple[Path, ...]
    pz_path: Path | None
    output_path: Path
    network: str
    station: str
    location_code: str
    channel: str
