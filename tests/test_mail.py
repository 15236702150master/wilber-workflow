from __future__ import annotations

import sys
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from wilberflow.config import MailConfig
from wilberflow.mail import _build_search_groups, fetch_matches, poll_sleep_seconds


def success_raw_message(label: str, *, subject: str = "[Success] Your request is ready", sent_at: datetime | None = None) -> bytes:
    current = sent_at or datetime.now(UTC)
    rfc2822_date = current.strftime("%a, %d %b %Y %H:%M:%S +0000")
    return (
        "From: Wilber <wilber@iris.edu>\r\n"
        f"Subject: {subject}\r\n"
        f"Date: {rfc2822_date}\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n"
        f"Download: https://ds.iris.edu/pub/userdata/wilber/{label}/example.tar\r\n"
    ).encode("utf-8")


class FakeImapClient:
    def __init__(self, targeted_ids: bytes, all_ids: bytes, messages: dict[bytes, bytes]) -> None:
        self._targeted_ids = targeted_ids
        self._all_ids = all_ids
        self._messages = messages
        self.search_calls: list[tuple[str, ...]] = []
        self.fetch_calls: list[tuple[bytes, str]] = []

    def login(self, user: str, password: str) -> tuple[str, list[bytes]]:
        return "OK", [b"logged in"]

    def select(self, mailbox: str, readonly: bool = True) -> tuple[str, list[bytes]]:
        return "OK", [b"1"]

    def noop(self) -> tuple[str, list[bytes]]:
        return "OK", [b"noop"]

    def search(self, charset, *criteria: str) -> tuple[str, list[bytes]]:
        self.search_calls.append(criteria)
        if criteria == ("ALL",):
            return "OK", [self._all_ids]
        if "TEXT" in criteria:
            return "OK", [self._targeted_ids]
        return "OK", [self._all_ids]

    def fetch(self, message_id: bytes, spec: str) -> tuple[str, list[tuple[bytes, bytes]]]:
        self.fetch_calls.append((message_id, spec))
        payload = self._messages[message_id]
        if spec == "(BODY.PEEK[TEXT]<0.4096>)":
            msg = payload.decode("utf-8")
            if "\r\n\r\n" in msg:
                body = msg.split("\r\n\r\n", 1)[1]
            else:
                body = msg
            return "OK", [(b"TEXT", body.encode("utf-8"))]
        return "OK", [(b"RFC822", payload)]

    def close(self) -> None:
        return None

    def logout(self) -> None:
        return None


class MailTests(unittest.TestCase):
    def test_fetch_matches_falls_back_to_recent_messages_when_prefix_search_misses(self) -> None:
        config = MailConfig(max_messages=50, message_lookback_hours=1000)
        requested_at = (datetime.now(UTC) - timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
        expected_requests = {
            "wilberflow_wf_20260425_120000": {
                "event_key": "demo",
                "requested_at_utc": requested_at,
                "track_url": "",
                "output_event_id": "demo",
            }
        }
        fake_client = FakeImapClient(
            targeted_ids=b"",
            all_ids=b"101",
            messages={b"101": success_raw_message("wilberflow_wf_20260425_120000")},
        )

        with patch("wilberflow.mail.imaplib.IMAP4_SSL", return_value=fake_client):
            with patch("wilberflow.mail.require_env", side_effect=["user@qq.com", "secret"]):
                matches = fetch_matches(config, expected_requests)

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].request_label, "wilberflow_wf_20260425_120000")
        self.assertFalse(any(criteria == ("ALL",) for criteria in fake_client.search_calls))

    def test_poll_sleep_seconds_uses_fast_interval_first(self) -> None:
        config = MailConfig(poll_interval_seconds=30, fast_poll_interval_seconds=5, fast_poll_rounds=3)

        self.assertEqual(poll_sleep_seconds(config, 0), 5)
        self.assertEqual(poll_sleep_seconds(config, 1), 5)
        self.assertEqual(poll_sleep_seconds(config, 2), 5)
        self.assertEqual(poll_sleep_seconds(config, 3), 30)

    def test_build_search_groups_prefers_recent_prefix_queries(self) -> None:
        config = MailConfig(message_lookback_hours=24)
        requested_at = (datetime.now(UTC) - timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
        expected_requests = {
            "wilberflow_wf_20260425_120000_9050504_bhz": {
                "event_key": "demo",
                "requested_at_utc": requested_at,
                "track_url": "",
                "output_event_id": "demo",
            }
        }

        groups = _build_search_groups(config, None, expected_requests)

        self.assertGreaterEqual(len(groups), 2)
        self.assertTrue(groups[0][0].startswith("recent-prefix:"))
        self.assertIn("TEXT", groups[0][1])
        self.assertIn("SINCE", groups[0][1])
        self.assertIn("recent-base", [label for label, _ in groups])

    def test_fetch_matches_only_downloads_full_message_after_preview_hits_label(self) -> None:
        config = MailConfig(max_messages=50, message_lookback_hours=1000)
        requested_at = (datetime.now(UTC) - timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
        expected_requests = {
            "wilberflow_wf_20260425_120000": {
                "event_key": "demo",
                "requested_at_utc": requested_at,
                "track_url": "",
                "output_event_id": "demo",
            }
        }
        fake_client = FakeImapClient(
            targeted_ids=b"101 102",
            all_ids=b"",
            messages={
                b"101": success_raw_message(
                    "other_batch_label",
                    subject="[Success] Other request is ready",
                ),
                b"102": success_raw_message("wilberflow_wf_20260425_120000"),
            },
        )

        with patch("wilberflow.mail.imaplib.IMAP4_SSL", return_value=fake_client):
            with patch("wilberflow.mail.require_env", side_effect=["user@qq.com", "secret"]):
                matches = fetch_matches(config, expected_requests)

        self.assertEqual(len(matches), 1)
        full_fetches = [call for call in fake_client.fetch_calls if call[1] == "(RFC822)"]
        self.assertEqual(full_fetches, [(b"102", "(RFC822)")])


if __name__ == "__main__":
    unittest.main()
