from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from wilberflow.config import MailConfig
from wilberflow.mail import fetch_matches, poll_sleep_seconds


SUCCESS_RAW_MESSAGE = (
    "From: Wilber <wilber@iris.edu>\r\n"
    "Subject: [Success] Your request is ready\r\n"
    "Date: Thu, 25 Apr 2026 12:00:00 +0000\r\n"
    "Content-Type: text/plain; charset=utf-8\r\n"
    "\r\n"
    "Download: https://ds.iris.edu/pub/userdata/wilber/wilberflow_wf_20260425_120000/example.tar\r\n"
).encode("utf-8")


class FakeImapClient:
    def __init__(self, targeted_ids: bytes, all_ids: bytes, messages: dict[bytes, bytes]) -> None:
        self._targeted_ids = targeted_ids
        self._all_ids = all_ids
        self._messages = messages

    def login(self, user: str, password: str) -> tuple[str, list[bytes]]:
        return "OK", [b"logged in"]

    def select(self, mailbox: str, readonly: bool = True) -> tuple[str, list[bytes]]:
        return "OK", [b"1"]

    def noop(self) -> tuple[str, list[bytes]]:
        return "OK", [b"noop"]

    def search(self, charset, *criteria: str) -> tuple[str, list[bytes]]:
        if criteria == ("ALL",):
            return "OK", [self._all_ids]
        return "OK", [self._targeted_ids]

    def fetch(self, message_id: bytes, spec: str) -> tuple[str, list[tuple[bytes, bytes]]]:
        payload = self._messages[message_id]
        return "OK", [(b"RFC822", payload)]

    def close(self) -> None:
        return None

    def logout(self) -> None:
        return None


class MailTests(unittest.TestCase):
    def test_fetch_matches_falls_back_to_recent_messages_when_targeted_search_misses(self) -> None:
        config = MailConfig(max_messages=50, message_lookback_hours=1000)
        expected_requests = {
            "wilberflow_wf_20260425_120000": {
                "event_key": "demo",
                "requested_at_utc": "2026-04-25T11:55:00Z",
                "track_url": "",
                "output_event_id": "demo",
            }
        }
        fake_client = FakeImapClient(
            targeted_ids=b"",
            all_ids=b"101",
            messages={b"101": SUCCESS_RAW_MESSAGE},
        )

        with patch("wilberflow.mail.imaplib.IMAP4_SSL", return_value=fake_client):
            with patch("wilberflow.mail.require_env", side_effect=["user@qq.com", "secret"]):
                matches = fetch_matches(config, expected_requests)

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].request_label, "wilberflow_wf_20260425_120000")

    def test_poll_sleep_seconds_uses_fast_interval_first(self) -> None:
        config = MailConfig(poll_interval_seconds=30, fast_poll_interval_seconds=5, fast_poll_rounds=3)

        self.assertEqual(poll_sleep_seconds(config, 0), 5)
        self.assertEqual(poll_sleep_seconds(config, 1), 5)
        self.assertEqual(poll_sleep_seconds(config, 2), 5)
        self.assertEqual(poll_sleep_seconds(config, 3), 30)


if __name__ == "__main__":
    unittest.main()
