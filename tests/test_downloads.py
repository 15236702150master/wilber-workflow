from __future__ import annotations

import io
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from urllib.error import URLError
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from wilberflow.config import DownloadConfig
from wilberflow.downloads import attempt_download, download_packages, inferred_event_dir_name, validate_tar_archive


class DummyLogger:
    def info(self, *args, **kwargs) -> None:
        return None

    def warning(self, *args, **kwargs) -> None:
        return None

    def error(self, *args, **kwargs) -> None:
        return None


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def build_tar_bytes() -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        payload = b"demo" * 4096
        info = tarfile.TarInfo("demo/file.txt")
        info.size = len(payload)
        archive.addfile(info, io.BytesIO(payload))
    return buffer.getvalue()


class DownloadTests(unittest.TestCase):
    def test_inferred_event_dir_name_prefers_short_event_time_from_package_name(self) -> None:
        tar_path = Path("wilberflow_batch_wf_20260521_210035_1994_07_25_22_00_24_377768_hz.tar")
        self.assertEqual(
            inferred_event_dir_name(tar_path, "wilberflow_batch_wf_20260521_210035_1994_07_25_22_00_24_377768_hz"),
            "1994_07_25_22_00_24",
        )

    def test_validate_tar_archive_rejects_truncated_tar(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tar_path = Path(tmp_dir) / "bad.tar"
            tar_bytes = build_tar_bytes()
            tar_path.write_bytes(tar_bytes[:-4096])

            error = validate_tar_archive(tar_path)

            self.assertIsNotNone(error)
            self.assertIn("end of data", error)

    def test_download_packages_fails_fast_for_invalid_tar(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace_root = Path(tmp_dir)
            mail_dir = workspace_root / "04_mail"
            mail_dir.mkdir(parents=True)
            (mail_dir / "success_mail_links.csv").write_text(
                "RequestLabel,DownloadURL\nreq-1,https://example.com/demo.tar\n",
                encoding="utf-8",
            )
            tar_bytes = build_tar_bytes()
            invalid_payload = tar_bytes[:-4096]

            def fake_urlopen(url: str, timeout: int):
                return FakeResponse(invalid_payload)

            with patch("wilberflow.downloads.urlopen", side_effect=fake_urlopen):
                with self.assertRaisesRegex(RuntimeError, "downloaded tar validation failed"):
                    download_packages(workspace_root, DownloadConfig(), DummyLogger())

            manifest = (workspace_root / "05_downloads" / "download_manifest.csv").read_text(encoding="utf-8")
            summary = (workspace_root / "05_downloads" / "summary.json").read_text(encoding="utf-8")

            self.assertIn("invalid_tar", manifest)
            self.assertIn("AttemptsUsed", manifest)
            self.assertIn("RecoveredFromInvalid", manifest)
            self.assertIn("unexpected end of data", manifest)
            self.assertIn("\"status\": \"failed_invalid_tar\"", summary)

    def test_attempt_download_recovers_after_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "demo.tar"
            tar_bytes = build_tar_bytes()
            invalid_payload = tar_bytes[:-4096]
            responses = [
                FakeResponse(invalid_payload),
                FakeResponse(tar_bytes),
            ]

            def fake_urlopen(url: str, timeout: int):
                return responses.pop(0)

            with patch("wilberflow.downloads.urlopen", side_effect=fake_urlopen):
                attempts_used, error = attempt_download(
                    output_path,
                    "https://example.com/demo.tar",
                    DownloadConfig(retry_attempts=2, retry_sleep_seconds=0, final_retry_timeout=120),
                )

            self.assertEqual(attempts_used, 2)
            self.assertIsNone(error)
            self.assertTrue(output_path.exists())
            self.assertIsNone(validate_tar_archive(output_path))

    def test_attempt_download_returns_last_error_after_exhausting_retries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "demo.tar"
            error = URLError("network down")

            with patch("wilberflow.downloads.urlopen", side_effect=error):
                attempts_used, final_error = attempt_download(
                    output_path,
                    "https://example.com/demo.tar",
                    DownloadConfig(retry_attempts=2, retry_sleep_seconds=0, final_retry_timeout=120),
                )

            self.assertEqual(attempts_used, 2)
            self.assertIsNotNone(final_error)
            self.assertIn("network down", final_error)
            self.assertFalse(output_path.exists())


if __name__ == "__main__":
    unittest.main()
