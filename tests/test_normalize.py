from __future__ import annotations

import copy
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import obspy
from obspy import Trace, UTCDateTime, read_inventory

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from wilberflow.common import normalize_user_path_text
from wilberflow.models import EventInfo
from wilberflow.normalize import (
    build_jobs,
    clean_trace_data,
    clear_user_phase_markers_workspace,
    find_matching_pz,
    hampel_despike,
    inventory_cache_filename,
    inventory_channel_count,
    load_cached_inventory,
    remove_response_with_inventory,
    write_phase_arrivals_workspace,
)


def _inventory_fixture_path() -> Path:
    return Path(obspy.__file__).resolve().parent / "io" / "stationxml" / "tests" / "data" / "IRIS_single_channel_with_response.xml"


def _make_trace(channel: str, starttime: UTCDateTime, npts: int = 4000, sampling_rate: float = 40.0) -> Trace:
    times = np.arange(npts, dtype=np.float32) / np.float32(sampling_rate)
    data = (
        np.sin(2 * np.pi * 0.2 * times)
        + 0.2 * np.sin(2 * np.pi * 1.5 * times)
        + 0.05 * np.random.default_rng(42).standard_normal(npts).astype(np.float32)
    ).astype(np.float32)
    trace = Trace(data=data)
    trace.stats.network = "IU"
    trace.stats.station = "ANMO"
    trace.stats.location = "10"
    trace.stats.channel = channel
    trace.stats.starttime = starttime
    trace.stats.sampling_rate = sampling_rate
    return trace


class NormalizeTests(unittest.TestCase):
    def test_normalize_user_path_text_converts_windows_drive_to_wsl_path(self) -> None:
        self.assertEqual(
            normalize_user_path_text(r"D:\Data\01_Projects\2026\data\test"),
            "/mnt/d/Data/01_Projects/2026/data/test",
        )
        self.assertEqual(
            normalize_user_path_text(r"D:/Data/01_Projects/2026/data/test"),
            "/mnt/d/Data/01_Projects/2026/data/test",
        )

    def test_build_jobs_keeps_all_components_for_channel_wildcard(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir_text:
            tmpdir = Path(tmpdir_text)
            event_dir = tmpdir / "1996_01_01_00_00_00_demo"
            event_dir.mkdir()
            pz_root = event_dir / "pz"
            pz_root.mkdir()
            event = EventInfo(
                event_dir=event_dir,
                event_id="1996_01_01_00_00_00",
                event_time=UTCDateTime("1996-01-01T00:00:00"),
                event_label=event_dir.name,
            )

            for channel in ("BHE", "BHN", "BHZ"):
                trace = _make_trace(channel=channel, starttime=event.event_time)
                trace.write(str(event_dir / f"IU.ANMO.00.{channel}.SAC"), format="SAC")
                (pz_root / f"SACPZ.IU.ANMO.00.{channel}").write_text("ZEROS 0\nPOLES 0\nCONSTANT 1\n", encoding="utf-8")

            jobs, skipped = build_jobs(
                [event],
                tmpdir / "out",
                overwrite=True,
                channel_patterns=["BH?"],
                keep_only_preferred_location=False,
            )

            self.assertEqual({job.channel for job in jobs}, {"BHE", "BHN", "BHZ"})
            self.assertEqual(len(jobs), 3)
            self.assertEqual(skipped, [])

    def test_build_jobs_supports_custom_question_hz_pattern(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir_text:
            tmpdir = Path(tmpdir_text)
            event_dir = tmpdir / "1996_01_01_00_00_00_demo"
            event_dir.mkdir()
            pz_root = event_dir / "pz"
            pz_root.mkdir()
            event = EventInfo(
                event_dir=event_dir,
                event_id="1996_01_01_00_00_00",
                event_time=UTCDateTime("1996-01-01T00:00:00"),
                event_label=event_dir.name,
            )

            for channel in ("BHZ", "HHZ", "LHZ", "BHN"):
                trace = _make_trace(channel=channel, starttime=event.event_time)
                trace.write(str(event_dir / f"IU.ANMO.00.{channel}.SAC"), format="SAC")
                (pz_root / f"SACPZ.IU.ANMO.00.{channel}").write_text("ZEROS 0\nPOLES 0\nCONSTANT 1\n", encoding="utf-8")

            jobs, skipped = build_jobs(
                [event],
                tmpdir / "out",
                overwrite=True,
                channel_patterns=["?HZ"],
                keep_only_preferred_location=False,
            )

            self.assertEqual({job.channel for job in jobs}, {"BHZ", "HHZ", "LHZ"})
            self.assertEqual({row["Channel"] for row in skipped}, {"BHN"})

    def test_find_matching_pz_searches_recursively_and_tolerates_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir_text:
            tmpdir = Path(tmpdir_text)
            event_dir = tmpdir / "1996_01_01_00_00_00_demo"
            event_dir.mkdir()
            nested_pz_dir = event_dir / "IRISDMC"
            nested_pz_dir.mkdir()

            trace = _make_trace(channel="BHE", starttime=UTCDateTime("1996-01-01T00:00:00"))
            sac_path = event_dir / "IU.ANMO.00.BHE.SAC"
            trace.write(str(sac_path), format="SAC")
            pz_path = nested_pz_dir / "SACPZ.IU.ANMO.00.BHE"
            pz_path.write_text("ZEROS 0\nPOLES 0\nCONSTANT 1\n", encoding="utf-8")

            self.assertEqual(find_matching_pz(event_dir, sac_path), pz_path)

            missing_trace = _make_trace(channel="BHN", starttime=UTCDateTime("1996-01-01T00:00:00"))
            missing_sac_path = event_dir / "IU.ANMO.00.BHN.SAC"
            missing_trace.write(str(missing_sac_path), format="SAC")
            self.assertIsNone(find_matching_pz(event_dir, missing_sac_path))

    def test_build_jobs_allows_missing_pz_for_inventory_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir_text:
            tmpdir = Path(tmpdir_text)
            event_dir = tmpdir / "1996_01_01_00_00_00_demo"
            event_dir.mkdir()
            event = EventInfo(
                event_dir=event_dir,
                event_id="1996_01_01_00_00_00",
                event_time=UTCDateTime("1996-01-01T00:00:00"),
                event_label=event_dir.name,
            )

            trace = _make_trace(channel="BHE", starttime=event.event_time)
            trace.write(str(event_dir / "IU.ANMO.00.BHE.SAC"), format="SAC")

            jobs, skipped = build_jobs(
                [event],
                tmpdir / "out",
                overwrite=True,
                channel_patterns=["BH?"],
                keep_only_preferred_location=False,
            )

            self.assertEqual(len(jobs), 1)
            self.assertIsNone(jobs[0].pz_path)
            self.assertEqual(skipped, [])

    def test_build_jobs_keeps_only_preferred_location_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir_text:
            tmpdir = Path(tmpdir_text)
            event_dir = tmpdir / "1996_01_01_00_00_00_demo"
            event_dir.mkdir()
            event = EventInfo(
                event_dir=event_dir,
                event_id="1996_01_01_00_00_00",
                event_time=UTCDateTime("1996-01-01T00:00:00"),
                event_label=event_dir.name,
            )

            trace_10 = _make_trace(channel="BHZ", starttime=event.event_time)
            trace_10.stats.location = "10"
            trace_10.write(str(event_dir / "IU.ANMO.10.BHZ.SAC"), format="SAC")

            trace_00 = _make_trace(channel="BHZ", starttime=event.event_time)
            trace_00.stats.location = "00"
            trace_00.write(str(event_dir / "IU.ANMO.00.BHZ.SAC"), format="SAC")

            jobs, skipped = build_jobs(
                [event],
                tmpdir / "out",
                overwrite=True,
                channel_patterns=["BHZ"],
                location_priority=["00", "10"],
                keep_only_preferred_location=True,
            )

            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].location_code, "00")
            self.assertEqual(len(skipped), 1)
            self.assertIn("location_not_preferred:10->00", skipped[0]["Reason"])

    def test_inventory_cache_filename_and_load_cached_inventory(self) -> None:
        inventory = read_inventory(str(_inventory_fixture_path()))
        trace = _make_trace(channel="BHE", starttime=UTCDateTime("2012-03-13T08:15:00"))
        trace.stats.network = "IU"
        trace.stats.station = "ANMO"
        trace.stats.location = ""

        filename = inventory_cache_filename(trace)
        self.assertEqual(filename, "IU.ANMO.--.BHE.20120313T081500.stationxml")

        with tempfile.TemporaryDirectory() as tmpdir_text:
            cache_path = Path(tmpdir_text) / filename
            inventory.write(str(cache_path), format="STATIONXML")
            cached = load_cached_inventory(cache_path)

        self.assertIsNotNone(cached)
        self.assertGreater(inventory_channel_count(cached), 0)

    def test_remove_response_with_inventory_supports_multiple_channel_codes(self) -> None:
        inventory = read_inventory(str(_inventory_fixture_path()))
        starttime = UTCDateTime("2012-03-13T08:15:00")

        for channel_code in ("BHZ", "BHN", "BHE", "HHZ", "LHZ"):
            with self.subTest(channel_code=channel_code):
                current_inventory = copy.deepcopy(inventory)
                current_inventory[0][0][0].code = channel_code
                trace = _make_trace(channel=channel_code, starttime=starttime)
                processed = remove_response_with_inventory(
                    trace,
                    current_inventory,
                    pre_filt=(0.001, 0.005, 10.0, 15.0),
                    output_unit="VEL",
                )

                self.assertEqual(processed.stats.channel, channel_code)
                self.assertEqual(processed.stats.npts, trace.stats.npts)
                self.assertEqual(processed.data.dtype, np.float32)
                self.assertTrue(np.isfinite(processed.data).all())
                self.assertGreater(np.abs(processed.data).max(), 0.0)

    def test_hampel_despike_replaces_large_outlier(self) -> None:
        data = np.array([0.0, 0.0, 0.0, 50.0, 0.0, 0.0, 0.0], dtype=np.float64)
        cleaned, replaced = hampel_despike(data, window=5, nsigma=3.0)

        self.assertEqual(replaced, 1)
        self.assertEqual(cleaned[3], 0.0)

    def test_clean_trace_data_drops_zero_after_clean(self) -> None:
        trace = _make_trace(channel="BHZ", starttime=UTCDateTime("2012-03-13T08:15:00"), npts=100)
        trace.data = np.zeros(trace.stats.npts, dtype=np.float32)

        cleaned, info = clean_trace_data(
            trace,
            zero_threshold=1e-12,
            despike_mode="hampel",
            despike_window=11,
            despike_nsigma=8.0,
        )

        self.assertIsNone(cleaned)
        self.assertEqual(info["status"], "drop_zero_amplitude")

    def test_phase_arrivals_and_user_marker_workspace_processes_all_events(self) -> None:
        from obspy.io.sac import SACTrace

        with tempfile.TemporaryDirectory() as tmpdir_text:
            workspace = Path(tmpdir_text)
            events_root = workspace / "07_final" / "events"
            events_root.mkdir(parents=True)

            # Two events, two SAC files each, so a single workspace call must touch all 4.
            sample_event_dirs = []
            for event_index in range(2):
                event_dir = events_root / f"event_{event_index:02d}"
                event_dir.mkdir()
                sample_event_dirs.append(event_dir)
                for channel in ("BHZ", "BHE"):
                    sac = SACTrace()
                    sac.stla = 0.0
                    sac.stlo = 0.0
                    sac.evla = 10.0
                    sac.evlo = 10.0
                    sac.evdp = 100.0
                    sac.gcarc = 60.0
                    sac.dist = 60.0 * 111.195
                    sac.az = 90.0
                    sac.baz = 270.0
                    sac.b = 0.0
                    sac.o = 0.0
                    sac.data = np.zeros(1, dtype=np.float32)
                    sac.user0 = 99.0
                    sac.user2 = 99.0
                    sac.user3 = 99.0
                    sac.kuser0 = "X"
                    sac.kuser2 = "Y"
                    sac_path = event_dir / f"IU.ANMO.00.{channel}.SAC"
                    sac_path.parent.mkdir(parents=True, exist_ok=True)
                    sac.write(str(sac_path))

            arrivals_stats = write_phase_arrivals_workspace(workspace)
            self.assertEqual(arrivals_stats["events"], 2)
            self.assertEqual(arrivals_stats["files"], 4)
            self.assertEqual(arrivals_stats["failed"], 0)

            # Phase arrival must be written; user markers stay unchanged by the write pass.
            for event_dir in sample_event_dirs:
                for sac_path in event_dir.glob("*.SAC"):
                    reloaded = SACTrace.read(str(sac_path))
                    self.assertNotEqual(float(reloaded.t0), -12345.0)
                    self.assertEqual(reloaded.kt0, "P")
                    # Write pass should not silently clear user markers; the clear pass does that.
                    self.assertAlmostEqual(float(reloaded.user0), 99.0)

            clear_stats = clear_user_phase_markers_workspace(workspace)
            self.assertEqual(clear_stats["events"], 2)
            self.assertEqual(clear_stats["files"], 4)
            self.assertEqual(clear_stats["failed"], 0)

            for event_dir in sample_event_dirs:
                for sac_path in event_dir.glob("*.SAC"):
                    cleared = SACTrace.read(str(sac_path))
                    # SAC undefined sentinel (-12345.0) reads back as None on ObsPy.
                    self.assertIsNone(cleared.user0)
                    self.assertIsNone(cleared.user2)
                    self.assertIsNone(cleared.user3)
                    # Only kuser0/kuser1/kuser2 exist in SACTrace; user3 has no string label.
                    self.assertEqual(cleared.kuser0, "")
                    self.assertEqual(cleared.kuser2, "")


if __name__ == "__main__":
    unittest.main()
