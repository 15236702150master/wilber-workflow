from __future__ import annotations

import sys
import unittest
from http.client import IncompleteRead
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from wilberflow.common import http_get_text


class _FakeResponse:
    def __init__(self, text: str) -> None:
        self._payload = text.encode("utf-8")

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def read(self) -> bytes:
        return self._payload


class _FakeOpener:
    def __init__(self, results: list[object]) -> None:
        self._results = list(results)
        self.calls: list[tuple[str, int]] = []

    def open(self, url: str, timeout: int = 30):
        self.calls.append((url, timeout))
        result = self._results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class HttpGetTextTests(unittest.TestCase):
    def test_http_get_text_retries_incomplete_read(self) -> None:
        opener = _FakeOpener(
            [
                IncompleteRead(b"partial-body", 20),
                _FakeResponse("ok"),
            ]
        )

        text = http_get_text(
            opener,
            "https://example.com/query",
            retry_attempts=2,
            retry_sleep_seconds=0.0,
        )

        self.assertEqual(text, "ok")
        self.assertEqual(len(opener.calls), 2)


if __name__ == "__main__":
    unittest.main()
