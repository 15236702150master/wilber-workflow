from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from wilberflow.common import normalize_filter_text, parse_filter_tokens
from wilberflow.config import load_config


class ConfigTests(unittest.TestCase):
    def test_parse_filter_tokens_accepts_custom_channel_delimiters(self) -> None:
        self.assertEqual(parse_filter_tokens("BH?, ?HZ ; HHZ\nLHZ"), ["BH?", "?HZ", "HHZ", "LHZ"])

    def test_normalize_filter_text_keeps_custom_channel_patterns_compact(self) -> None:
        self.assertEqual(normalize_filter_text("BH?, ?HZ ; HHZ"), "BH?,?HZ,HHZ")
        self.assertEqual(normalize_filter_text(" BH? ,  ?HZ "), "BH?,?HZ")

    def test_load_config_normalizes_custom_channel_patterns(self) -> None:
        config_text = """
[event_search]

[request]
channels = " BH? ,  ?HZ "

[mail]

[download]

[normalize]
"""
        with tempfile.TemporaryDirectory() as tmpdir_text:
            config_path = Path(tmpdir_text) / "config.toml"
            config_path.write_text(config_text, encoding="utf-8")
            config = load_config(config_path)

        self.assertEqual(config.request.channels, "BH?,?HZ")


if __name__ == "__main__":
    unittest.main()
