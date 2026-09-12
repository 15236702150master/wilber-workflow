from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from wilberflow.pipeline import workflow_stage_sequence


class PipelineStageSequenceTests(unittest.TestCase):
    def test_wilber_page_backend_skips_station_stage_for_request_flow(self) -> None:
        pipeline_config = SimpleNamespace(
            request=SimpleNamespace(
                metadata_only=False,
                submit=False,
                station_selection_backend="wilber_page",
            ),
        )

        stages = [item["key"] for item in workflow_stage_sequence(pipeline_config)]

        self.assertEqual(stages, ["events", "requests"])

    def test_metadata_only_wilber_page_backend_still_uses_requests_stage(self) -> None:
        pipeline_config = SimpleNamespace(
            request=SimpleNamespace(
                metadata_only=True,
                submit=False,
                station_selection_backend="wilber_page",
            ),
        )

        stages = [item["key"] for item in workflow_stage_sequence(pipeline_config)]

        self.assertEqual(stages, ["events", "requests"])


if __name__ == "__main__":
    unittest.main()
