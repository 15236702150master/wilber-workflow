from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
from obspy.io.sac import SACTrace

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from wilberflow.common import DEDUP_DROPPED_DIR_NAME, load_csv_rows
from wilberflow.config import DedupConfig, PipelineConfig  # noqa: F401
from wilberflow.dedup import dedup_workspace


def _write_sac(path: Path, network: str, station: str, location: str, channel: str) -> None:
    """Write a minimal SAC file with the identity header fields dedup reads."""
    sac = SACTrace()
    sac.knetwk = network
    sac.kstnm = station
    sac.kcmpnm = channel
    # SACTrace uses "--" / "" for empty location; dedup normalizes via normalize_loc_code.
    sac.khole = location if location else "--"
    sac.data = np.ones(4, dtype=np.float32)
    path.parent.mkdir(parents=True, exist_ok=True)
    sac.write(str(path))


def _make_pipeline_config(enabled: bool = True, location_priority: str = "00,10,--"):
    """Build a minimal PipelineConfig-like object with .dedup.enabled and .request.location_priority."""
    from wilberflow.config import (
        DownloadConfig, EventSearchConfig, MailConfig, NormalizeConfig, NotifyConfig, PipelineConfig, RequestConfig,
    )
    return PipelineConfig(
        event_search=EventSearchConfig(),
        request=RequestConfig(location_priority=location_priority),
        mail=MailConfig(),
        download=DownloadConfig(),
        normalize=NormalizeConfig(),
        dedup=DedupConfig(enabled=enabled),
        notify=NotifyConfig(),
    )


class _NullLogger:
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass


class DedupTests(unittest.TestCase):
    def _workspace_with_event(self, tmpdir: Path, event_id: str = "2013_03_19_03_29_02") -> Path:
        ws = tmpdir / "ws"
        events_root = ws / "07_final" / "events"
        event_dir = events_root / event_id
        event_dir.mkdir(parents=True)
        return ws, event_dir

    def test_dedup_keeps_highest_priority_channel_per_station(self) -> None:
        with tempfile.TemporaryDirectory() as t:
            ws, event_dir = self._workspace_with_event(Path(t))
            # Same station IU.ANMO, two bands -> keep BHZ, drop LHZ.
            _write_sac(event_dir / "IU.ANMO.2013.078.03.29.02.BHZ.sac", "IU", "ANMO", "", "BHZ")
            _write_sac(event_dir / "IU.ANMO.2013.078.03.29.02.LHZ.sac", "IU", "ANMO", "", "LHZ")
            stats = dedup_workspace(ws, _make_pipeline_config(), _NullLogger())
            self.assertEqual(stats["dropped"], 1)
            self.assertEqual(stats["kept"], 1)
            remaining = sorted(p.name for p in event_dir.glob("*.sac"))
            self.assertEqual(remaining, ["IU.ANMO.2013.078.03.29.02.BHZ.sac"])
            dropped_dir = ws / "07_final" / "events" / DEDUP_DROPPED_DIR_NAME / "2013_03_19_03_29_02"
            self.assertEqual(sorted(p.name for p in dropped_dir.glob("*.sac")),
                             ["IU.ANMO.2013.078.03.29.02.LHZ.sac"])
            rows = load_csv_rows(ws / "07_final" / "dedup_summary.csv")
            self.assertEqual(len(rows), 1)
            self.assertIn("lower_priority_channel", rows[0]["Reason"])

    def test_dedup_resolves_multi_location_via_preferred(self) -> None:
        with tempfile.TemporaryDirectory() as t:
            ws, event_dir = self._workspace_with_event(Path(t))
            # Same station+channel HHZ, three locations -> keep 00.
            for loc in ("00", "10", "--"):
                fname = f"IU.ANMO.{loc}.2013.078.03.29.02.HHZ.sac"
                _write_sac(event_dir / fname, "IU", "ANMO", loc, "HHZ")
            stats = dedup_workspace(ws, _make_pipeline_config(location_priority="00,10,--"), _NullLogger())
            self.assertEqual(stats["dropped"], 2)
            remaining = sorted(p.name for p in event_dir.glob("*.sac"))
            self.assertEqual(remaining, ["IU.ANMO.00.2013.078.03.29.02.HHZ.sac"])
            rows = load_csv_rows(ws / "07_final" / "dedup_summary.csv")
            for r in rows:
                self.assertIn("lower_priority_location", r["Reason"])

    def test_dedup_updates_processing_summary_and_backup(self) -> None:
        with tempfile.TemporaryDirectory() as t:
            ws, event_dir = self._workspace_with_event(Path(t))
            _write_sac(event_dir / "IU.ANMO.2013.078.03.29.02.BHZ.sac", "IU", "ANMO", "", "BHZ")
            _write_sac(event_dir / "IU.ANMO.2013.078.03.29.02.LHZ.sac", "IU", "ANMO", "", "LHZ")
            stage = ws / "07_final"
            from wilberflow.common import write_csv
            write_csv(stage / "processing_summary.csv",
                      ["EventID", "OutputPath", "Network", "Station", "Channel"],
                      [
                          {"EventID": "e1", "OutputPath": str(event_dir / "IU.ANMO.2013.078.03.29.02.BHZ.sac"),
                           "Network": "IU", "Station": "ANMO", "Channel": "BHZ"},
                          {"EventID": "e1", "OutputPath": str(event_dir / "IU.ANMO.2013.078.03.29.02.LHZ.sac"),
                           "Network": "IU", "Station": "ANMO", "Channel": "LHZ"},
                      ])
            dedup_workspace(ws, _make_pipeline_config(), _NullLogger())
            rows = load_csv_rows(stage / "processing_summary.csv")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["Channel"], "BHZ")
            # Backup created on first run.
            self.assertTrue((stage / "processing_summary.before_dedup.csv").exists())

    def test_dedup_idempotent_on_rerun(self) -> None:
        with tempfile.TemporaryDirectory() as t:
            ws, event_dir = self._workspace_with_event(Path(t))
            _write_sac(event_dir / "IU.ANMO.2013.078.03.29.02.BHZ.sac", "IU", "ANMO", "", "BHZ")
            _write_sac(event_dir / "IU.ANMO.2013.078.03.29.02.LHZ.sac", "IU", "ANMO", "", "LHZ")
            cfg = _make_pipeline_config()
            first = dedup_workspace(ws, cfg, _NullLogger())
            self.assertEqual(first["dropped"], 1)
            second = dedup_workspace(ws, cfg, _NullLogger())
            self.assertEqual(second["dropped"], 0)
            # dedup_summary accumulates (first run's row preserved), not duplicated.
            rows = load_csv_rows(ws / "07_final" / "dedup_summary.csv")
            self.assertEqual(len(rows), 1)

    def test_dedup_excludes_dropped_dir_from_iteration(self) -> None:
        with tempfile.TemporaryDirectory() as t:
            ws, event_dir = self._workspace_with_event(Path(t))
            _write_sac(event_dir / "IU.ANMO.2013.078.03.29.02.BHZ.sac", "IU", "ANMO", "", "BHZ")
            # Pre-place a stray file in the dropped dir; it must not be counted as an event.
            stray_dir = ws / "07_final" / "events" / DEDUP_DROPPED_DIR_NAME / "stray_event"
            _write_sac(stray_dir / "XX.YYYY.2013.078.03.29.02.BHZ.sac", "XX", "YYYY", "", "BHZ")
            stats = dedup_workspace(ws, _make_pipeline_config(), _NullLogger())
            self.assertEqual(stats["events"], 1)  # only the real event dir
            self.assertEqual(stats["dropped"], 0)

    def test_dedup_disabled_noop(self) -> None:
        with tempfile.TemporaryDirectory() as t:
            ws, event_dir = self._workspace_with_event(Path(t))
            _write_sac(event_dir / "IU.ANMO.2013.078.03.29.02.BHZ.sac", "IU", "ANMO", "", "BHZ")
            _write_sac(event_dir / "IU.ANMO.2013.078.03.29.02.LHZ.sac", "IU", "ANMO", "", "LHZ")
            stats = dedup_workspace(ws, _make_pipeline_config(enabled=False), _NullLogger())
            self.assertFalse(stats["enabled"])
            # Both files untouched.
            self.assertEqual(len(list(event_dir.glob("*.sac"))), 2)
            self.assertFalse((ws / "07_final" / "dedup_summary.csv").exists())

    def test_dedup_unknown_channel_ranked_last(self) -> None:
        with tempfile.TemporaryDirectory() as t:
            ws, event_dir = self._workspace_with_event(Path(t))
            _write_sac(event_dir / "IU.ANMO.2013.078.03.29.02.HHZ.sac", "IU", "ANMO", "", "HHZ")
            _write_sac(event_dir / "IU.ANMO.2013.078.03.29.02.XYZ.sac", "IU", "ANMO", "", "XYZ")
            stats = dedup_workspace(ws, _make_pipeline_config(), _NullLogger())
            self.assertEqual(stats["dropped"], 1)
            remaining = [p.name for p in event_dir.glob("*.sac")]
            self.assertEqual(remaining, ["IU.ANMO.2013.078.03.29.02.HHZ.sac"])


if __name__ == "__main__":
    unittest.main()
