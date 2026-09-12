from __future__ import annotations

import json
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

from wilberflow.common import write_csv
from wilberflow.config import (
    DedupConfig, DownloadConfig, EventSearchConfig, MailConfig, NormalizeConfig, NotifyConfig,
    PipelineConfig, RequestConfig,
)
from wilberflow.readme import generate_delivery_readme


class _NullLogger:
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass


def _make_pipeline_config() -> PipelineConfig:
    return PipelineConfig(
        event_search=EventSearchConfig(),
        request=RequestConfig(channels="?HZ", location_priority="00,10,--"),
        mail=MailConfig(),
        download=DownloadConfig(),
        normalize=NormalizeConfig(pre_filt="0.01,0.05,4.0,4.9", output_unit="VEL"),
        dedup=DedupConfig(enabled=True),
        notify=NotifyConfig(),
    )


class ReadmeTests(unittest.TestCase):
    def _setup_workspace(self, tmpdir: Path) -> tuple[Path, Path]:
        ws = tmpdir / "ws"
        final_root = ws / "07_final"
        events_root = final_root / "events"
        events_root.mkdir(parents=True)
        # copied config so readme can load parameters.
        cfg_dir = ws / "00_config"
        cfg_dir.mkdir()
        # Write a minimal config TOML that load_config accepts.
        (cfg_dir / "copied_config.toml").write_text(
            '[event_search]\n[request]\nchannels="?HZ"\nlocation_priority="00,10,--"\n'
            '[mail]\n[download]\n[normalize]\npre_filt="0.01,0.05,4.0,4.9"\noutput_unit="VEL"\n'
            '[dedup]\nenabled=true\n[notify]\n', encoding="utf-8",
        )
        return ws, final_root

    def test_generate_delivery_readme_contains_sections(self) -> None:
        with tempfile.TemporaryDirectory() as t:
            ws, final_root = self._setup_workspace(Path(t))
            # summary.json
            (final_root / "summary.json").write_text(json.dumps({
                "event_count": 1, "trace_job_count": 1095, "success_count": 1080,
                "failure_count": 15, "iris_fallback_success": 3,
            }), encoding="utf-8")
            # processing_summary.csv
            write_csv(final_root / "processing_summary.csv",
                      ["EventID", "OutputPath", "Network", "Station", "Channel"],
                      [
                          {"EventID": "e1", "OutputPath": "x/IU.ANMO.BHZ.sac", "Network": "IU", "Station": "ANMO", "Channel": "BHZ"},
                          {"EventID": "e1", "OutputPath": "x/IU.ANMO.HHZ.sac", "Network": "IU", "Station": "ANMO", "Channel": "HHZ"},
                      ])
            # processing_failures.csv
            write_csv(final_root / "processing_failures.csv",
                      ["EventID", "Network", "Station", "Channel", "Method", "Reason"],
                      [{"EventID": "e1", "Network": "1C", "Station": "MOND", "Channel": "HHZ",
                        "Method": "local_sac", "Reason": "drop_zero_after_clean"}])
            # dedup_summary.csv
            write_csv(final_root / "dedup_summary.csv",
                      ["EventID", "Network", "Station", "Channel", "Reason"],
                      [{"EventID": "e1", "Network": "IU", "Station": "ANMO", "Channel": "HHZ",
                        "Reason": "lower_priority_channel:BHZ>HHZ"}])

            target = generate_delivery_readme(ws, final_root, _NullLogger())
            self.assertEqual(target, final_root / "README.md")
            text = target.read_text(encoding="utf-8")
            for section in [
                "一、事件与处理统计", "二、目录结构", "三、处理参数",
                "四、输出文件分布", "五、处理错误", "六、去重移除记录",
                "00_config", "07_final", "BHZ>HHZ>SHZ>EHZ>DHZ>MHZ>LHZ>VHZ>UHZ",
            ]:
                self.assertIn(section, text, f"missing section: {section}")
            # Error snippet should include the failure reason verbatim.
            self.assertIn("drop_zero_after_clean", text)
            # Dedup record should report 1 dropped.
            self.assertIn("移除文件数：1", text)

    def test_readme_handles_missing_dedup_summary(self) -> None:
        with tempfile.TemporaryDirectory() as t:
            ws, final_root = self._setup_workspace(Path(t))
            (final_root / "summary.json").write_text("{}", encoding="utf-8")
            write_csv(final_root / "processing_summary.csv",
                      ["EventID", "Network", "Station", "Channel"], [])
            # No processing_failures.csv, no dedup_summary.csv.
            target = generate_delivery_readme(ws, final_root, _NullLogger())
            text = target.read_text(encoding="utf-8")
            # No failures -> graceful message; no dedup -> "无文件被去重移除".
            self.assertIn("无失败记录", text)
            self.assertIn("无文件被去重移除", text)


if __name__ == "__main__":
    unittest.main()
