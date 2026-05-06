from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from wilberflow.server import _batch_manifest_for_path


class ServerTests(unittest.TestCase):
    def test_batch_manifest_marks_config_only_placeholder(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir_text:
            batch_root = Path(tmpdir_text) / "wf_20260506_151900"
            runtime_config = batch_root / ".wilberflow-studio" / "runtime_config.toml"
            runtime_config.parent.mkdir(parents=True, exist_ok=True)
            runtime_config.write_text("[request]\nchannels = \"BH?\"\n", encoding="utf-8")

            manifest = _batch_manifest_for_path(batch_root)

        self.assertTrue(manifest["has_runtime_config"])
        self.assertFalse(manifest["has_request_plan"])
        self.assertFalse(manifest["has_pipeline_log"])
        self.assertTrue(manifest["is_config_only_placeholder"])
        self.assertEqual(manifest["display_status"], "仅保存配置，未启动")


if __name__ == "__main__":
    unittest.main()
