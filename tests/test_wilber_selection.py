from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from obspy import UTCDateTime

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from wilberflow.models import CandidateEvent, StationLocation, StationRecord
from wilberflow.wilber import (
    _build_request_plan_row,
    _ensure_browser_bridge_ready,
    _wilber_page_selection_lines,
    build_selection_lines,
    select_preferred_location,
)


class _RequestConfig:
    channels = "?HZ"
    channel_selection_mode = "preferred"
    window_start_before_min = 0
    window_start_phase = ""
    window_end_after_min = 20
    window_end_phase = ""


class WilberSelectionTests(unittest.TestCase):
    def test_select_preferred_location_prefers_best_channel_before_location_priority(self) -> None:
        station = StationRecord(
            network="XP",
            network_name="Example",
            station="N132",
            station_id="XP.N132",
            latitude=0.0,
            longitude=0.0,
            elevation_m=0.0,
            name="Example XP N132",
            virtual_networks=(),
            data_center="IRISDMC",
            locations=(
                StationLocation(code="01", instrument="", depth="", channels=("LHZ",)),
                StationLocation(code="02", instrument="", depth="", channels=("BHZ",)),
            ),
        )

        preferred = select_preferred_location(station, ["?HZ"], ["00", "", "10"], channel_selection_mode="preferred")

        self.assertEqual(preferred, ("02", ("BHZ",), 1))

    def test_select_preferred_location_uses_location_priority_to_break_same_channel_rank(self) -> None:
        station = StationRecord(
            network="IU",
            network_name="Example",
            station="LCO",
            station_id="IU.LCO",
            latitude=0.0,
            longitude=0.0,
            elevation_m=0.0,
            name="Example IU LCO",
            virtual_networks=(),
            data_center="IRISDMC",
            locations=(
                StationLocation(code="10", instrument="", depth="", channels=("BHZ",)),
                StationLocation(code="00", instrument="", depth="", channels=("BHZ",)),
            ),
        )

        preferred = select_preferred_location(station, ["?HZ"], ["00", "", "10"], channel_selection_mode="preferred")

        self.assertEqual(preferred, ("00", ("BHZ",), 1))

    def test_select_preferred_location_can_keep_all_matching_channels(self) -> None:
        station = StationRecord(
            network="G",
            network_name="Example",
            station="SPB",
            station_id="G.SPB",
            latitude=0.0,
            longitude=0.0,
            elevation_m=0.0,
            name="Example G SPB",
            virtual_networks=(),
            data_center="RESIF",
            locations=(StationLocation(code="--", instrument="", depth="", channels=("BHZ", "LHZ", "VHZ")),),
        )

        preferred = select_preferred_location(station, ["?HZ"], ["00", "", "10"], channel_selection_mode="all_matching")

        self.assertEqual(preferred, ("", ("BHZ", "LHZ", "VHZ"), 3))

    def test_build_selection_lines_requests_selected_channel_only(self) -> None:
        event = CandidateEvent(
            event_id=123,
            event_time=UTCDateTime("2002-02-10T01:47:07"),
            latitude=0.0,
            longitude=0.0,
            depth_km=10.0,
            author="ISC",
            catalog="ISC",
            contributor="ISC",
            contributor_id="ISC",
            magnitude_type="MW",
            magnitude=5.9,
            magnitude_author="ISC",
            description="Example event",
        )
        station = StationRecord(
            network="G",
            network_name="Example",
            station="SPB",
            station_id="G.SPB",
            latitude=1.0,
            longitude=1.0,
            elevation_m=0.0,
            name="Example G SPB",
            virtual_networks=(),
            data_center="RESIF",
            locations=(StationLocation(code="--", instrument="", depth="", channels=("BHZ", "LHZ", "VHZ")),),
        )
        selected_station = type("Selected", (), {
            "station": station,
            "distance_deg": 10.0,
            "selected_location_code": "",
            "channel_selection_mode": "preferred",
            "selected_channels": ("BHZ",),
        })()

        lines = build_selection_lines(event, [selected_station], _RequestConfig(), model=None)  # type: ignore[arg-type]

        self.assertEqual(lines, ["G SPB * BHZ 2002-02-10T01:47:07 2002-02-10T02:07:07"])

    def test_build_request_plan_row_falls_back_when_wilber_page_backend_fails(self) -> None:
        event = CandidateEvent(
            event_id=123,
            event_time=UTCDateTime("2002-02-10T01:47:07"),
            latitude=0.0,
            longitude=0.0,
            depth_km=10.0,
            author="ISC",
            catalog="ISC",
            contributor="ISC",
            contributor_id="ISC",
            magnitude_type="MW",
            magnitude=5.9,
            magnitude_author="ISC",
            description="Example event",
        )
        row = {"EventKey": event.event_key}
        events = {event.event_key: event}
        pipeline_config = SimpleNamespace(
            request=SimpleNamespace(
                channels="BHZ",
                networks="",
                stations="",
                channel_selection_mode="preferred",
                station_selection_backend="wilber_page",
                min_distance_deg=35.0,
                max_distance_deg=95.0,
                min_azimuth_deg=-180.0,
                max_azimuth_deg=180.0,
                window_start_before_min=2,
                window_start_phase="P",
                window_end_after_min=5,
                window_end_phase="P",
                output_format="sacbl",
                bundle="tar",
                user="Test User",
                email="test@example.com",
                request_label_prefix="wilberflow",
                submit=False,
                timeout=30,
                sleep_seconds=0.3,
                max_request_attempts=5,
            ),
            event_search=SimpleNamespace(wilber_base_url="https://ds.iris.edu/wilber3"),
        )

        with tempfile.TemporaryDirectory() as tmpdir_text:
            workspace_root = Path(tmpdir_text)
            per_event_dir = workspace_root / "02_stations" / "per_event_selected"
            per_event_dir.mkdir(parents=True, exist_ok=True)
            per_event_csv = per_event_dir / f"{event.event_key}.csv"
            per_event_csv.write_text(
                "EventKey,OutputEventID,WilberEventID,WilberEventTimeUTC,Network,Station,StationID,SelectedLocationCode,ChannelSelectionMode,SelectedChannels,SelectedChannelCount,MatchingLocationCodes,MatchingChannels,MatchingChannelCount,DataCenter,VirtualNetworks,StaLat,StaLon,ElevationM,DistanceDeg,DistanceKm,Azimuth,BackAzimuth\n"
                f"{event.event_key},{event.output_event_id},{event.event_id},{event.event_time},G,SPB,G.SPB,--,preferred,BHZ,1,--,BHZ,1,RESIF,,1.0,1.0,0.0,10.0,1111.0,20.0,200.0\n",
                encoding="utf-8",
            )
            request_body_dir = workspace_root / "03_requests" / "request_bodies"
            request_body_dir.mkdir(parents=True, exist_ok=True)

            with patch("wilberflow.wilber._wilber_page_selection_lines", side_effect=RuntimeError("bridge failed")):
                result = _build_request_plan_row(row, events, workspace_root, pipeline_config, request_body_dir)

            body_text = (request_body_dir / f"{event.event_key}.txt").read_text(encoding="utf-8")
            self.assertIn("G SPB * BHZ", body_text)
            self.assertEqual(result["plan_row"]["SubmitStatus"], "not_submitted")

    def test_build_request_plan_row_uses_wilber_page_selection_when_available(self) -> None:
        event = CandidateEvent(
            event_id=123,
            event_time=UTCDateTime("2002-02-10T01:47:07"),
            latitude=0.0,
            longitude=0.0,
            depth_km=10.0,
            author="ISC",
            catalog="ISC",
            contributor="ISC",
            contributor_id="ISC",
            magnitude_type="MW",
            magnitude=5.9,
            magnitude_author="ISC",
            description="Example event",
        )
        row = {"EventKey": event.event_key}
        events = {event.event_key: event}
        pipeline_config = SimpleNamespace(
            request=SimpleNamespace(
                channels="?HZ",
                networks="",
                stations="",
                channel_selection_mode="preferred",
                station_selection_backend="wilber_page",
                min_distance_deg=35.0,
                max_distance_deg=95.0,
                min_azimuth_deg=-180.0,
                max_azimuth_deg=180.0,
                window_start_before_min=2,
                window_start_phase="P",
                window_end_after_min=5,
                window_end_phase="P",
                output_format="sacbl",
                bundle="tar",
                user="Test User",
                email="test@example.com",
                request_label_prefix="wilberflow",
                submit=False,
                timeout=30,
                sleep_seconds=0.3,
                max_request_attempts=5,
            ),
            event_search=SimpleNamespace(wilber_base_url="https://ds.iris.edu/wilber3"),
        )

        with tempfile.TemporaryDirectory() as tmpdir_text:
            workspace_root = Path(tmpdir_text)
            per_event_dir = workspace_root / "02_stations" / "per_event_selected"
            per_event_dir.mkdir(parents=True, exist_ok=True)
            per_event_csv = per_event_dir / f"{event.event_key}.csv"
            per_event_csv.write_text(
                "EventKey,OutputEventID,WilberEventID,WilberEventTimeUTC,Network,Station,StationID,SelectedLocationCode,ChannelSelectionMode,SelectedChannels,SelectedChannelCount,MatchingLocationCodes,MatchingChannels,MatchingChannelCount,DataCenter,VirtualNetworks,StaLat,StaLon,ElevationM,DistanceDeg,DistanceKm,Azimuth,BackAzimuth\n"
                f"{event.event_key},{event.output_event_id},{event.event_id},{event.event_time},G,SPB,G.SPB,--,preferred,BHZ,1,--,BHZ,1,RESIF,,1.0,1.0,0.0,10.0,1111.0,20.0,200.0\n",
                encoding="utf-8",
            )
            request_body_dir = workspace_root / "03_requests" / "request_bodies"
            request_body_dir.mkdir(parents=True, exist_ok=True)

            with patch("wilberflow.wilber._wilber_page_selection_lines", return_value=(["IU HOPE * ?HZ 2013-03-19T03:29:56 2013-03-19T03:40:56"], 151, 858)):
                result = _build_request_plan_row(row, events, workspace_root, pipeline_config, request_body_dir)

            body_text = (request_body_dir / f"{event.event_key}.txt").read_text(encoding="utf-8")
            self.assertEqual(body_text, "IU HOPE * ?HZ 2013-03-19T03:29:56 2013-03-19T03:40:56\n")
            self.assertEqual(result["plan_row"]["SelectedStationCount"], 151)
            self.assertEqual(result["plan_row"]["SelectedChannelCount"], 858)

    def test_build_request_plan_row_can_use_wilber_page_without_station_csv(self) -> None:
        event = CandidateEvent(
            event_id=123,
            event_time=UTCDateTime("2002-02-10T01:47:07"),
            latitude=0.0,
            longitude=0.0,
            depth_km=10.0,
            author="ISC",
            catalog="ISC",
            contributor="ISC",
            contributor_id="ISC",
            magnitude_type="MW",
            magnitude=5.9,
            magnitude_author="ISC",
            description="Example event",
        )
        row = {"EventKey": event.event_key}
        events = {event.event_key: event}
        pipeline_config = SimpleNamespace(
            request=SimpleNamespace(
                channels="?HZ",
                networks="",
                stations="",
                channel_selection_mode="preferred",
                station_selection_backend="wilber_page",
                min_distance_deg=35.0,
                max_distance_deg=95.0,
                min_azimuth_deg=-180.0,
                max_azimuth_deg=180.0,
                window_start_before_min=2,
                window_start_phase="P",
                window_end_after_min=5,
                window_end_phase="P",
                output_format="sacbl",
                bundle="tar",
                user="Test User",
                email="test@example.com",
                request_label_prefix="wilberflow",
                submit=False,
                timeout=30,
                sleep_seconds=0.3,
                max_request_attempts=5,
            ),
            event_search=SimpleNamespace(wilber_base_url="https://ds.iris.edu/wilber3"),
        )

        with tempfile.TemporaryDirectory() as tmpdir_text:
            workspace_root = Path(tmpdir_text)
            request_body_dir = workspace_root / "03_requests" / "request_bodies"
            request_body_dir.mkdir(parents=True, exist_ok=True)

            with patch(
                "wilberflow.wilber._wilber_page_selection_lines",
                return_value=(["IU HOPE * ?HZ 2013-03-19T03:29:56 2013-03-19T03:40:56"], 151, 858),
            ):
                result = _build_request_plan_row(row, events, workspace_root, pipeline_config, request_body_dir)

            body_text = (request_body_dir / f"{event.event_key}.txt").read_text(encoding="utf-8")
            self.assertEqual(body_text, "IU HOPE * ?HZ 2013-03-19T03:29:56 2013-03-19T03:40:56\n")
            self.assertEqual(result["plan_row"]["SelectedStationCount"], 151)
            self.assertEqual(result["plan_row"]["SelectedChannelCount"], 858)
            self.assertEqual(result["plan_row"]["StationCsvPath"], "")

    def test_wilber_page_selection_lines_parse_browser_bridge_json_wrappers(self) -> None:
        event = CandidateEvent(
            event_id=123,
            event_time=UTCDateTime("2002-02-10T01:47:07"),
            latitude=0.0,
            longitude=0.0,
            depth_km=10.0,
            author="ISC",
            catalog="ISC",
            contributor="ISC",
            contributor_id="ISC",
            magnitude_type="MW",
            magnitude=5.9,
            magnitude_author="ISC",
            description="Example event",
        )
        pipeline_config = SimpleNamespace(
            request=SimpleNamespace(
                channels="?HZ",
                networks="",
                stations="",
                request_label_prefix="wilberflow",
                min_distance_deg=35.0,
                max_distance_deg=95.0,
                min_azimuth_deg=-180.0,
                max_azimuth_deg=180.0,
                window_start_before_min=2,
                window_start_phase="P",
                window_end_after_min=5,
                window_end_phase="P",
                output_format="sacbl",
                bundle="tar",
                user="Test User",
                email="test@example.com",
            ),
            event_search=SimpleNamespace(wilber_base_url="https://ds.iris.edu/wilber3"),
        )
        responses = [
            {"ok": True, "ready": True, "tabCount": 0, "tabs": []},
            {"ok": True, "tabs": []},
            {"ok": True, "ready": True, "target": "ABCD1234", "tab": {"target": "ABCD1234", "url": "https://ds.iris.edu/wilber3/find_stations/123"}},
            {"ok": True, "tabs": [{"target": "ABCD1234", "url": "https://ds.iris.edu/wilber3/find_stations/123"}]},
            {"ok": True, "target": "ABCD1234", "result": "true"},
            {
                "ok": True,
                "target": "ABCD1234",
                "result": '{"selection":["IU HOPE * ?HZ 2013-03-19T03:29:56 2013-03-19T03:40:56"],"requestInfo":{"numStations":151,"numChannels":858}}',
            },
        ]

        with patch("wilberflow.wilber._browser_bridge_json", side_effect=responses):
            lines, station_count, channel_count = _wilber_page_selection_lines(event, pipeline_config)

        self.assertEqual(lines, ["IU HOPE * ?HZ 2013-03-19T03:29:56 2013-03-19T03:40:56"])
        self.assertEqual(station_count, 151)
        self.assertEqual(channel_count, 858)

    def test_ensure_browser_bridge_ready_restarts_bridge_when_list_discovery_fails(self) -> None:
        responses = [
            RuntimeError('{"ok": false, "error": "Command failed: list"}'),
            {"ok": True, "ready": True, "tabCount": 1, "tabs": []},
        ]

        with patch("wilberflow.wilber._browser_bridge_json", side_effect=responses) as bridge_json:
            with patch("wilberflow.wilber._start_browser_bridge") as start_bridge:
                _ensure_browser_bridge_ready()

        self.assertEqual(bridge_json.call_count, 2)
        start_bridge.assert_called_once()

    def test_ensure_browser_bridge_ready_raises_when_restart_cannot_recover(self) -> None:
        with patch(
            "wilberflow.wilber._browser_bridge_json",
            side_effect=RuntimeError('{"ok": false, "error": "Command failed: list"}'),
        ):
            with patch("wilberflow.wilber._start_browser_bridge"):
                with self.assertRaises(RuntimeError):
                    _ensure_browser_bridge_ready()


if __name__ == "__main__":
    unittest.main()
