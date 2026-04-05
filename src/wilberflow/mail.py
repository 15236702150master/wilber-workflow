from __future__ import annotations

import csv
import email
import email.header
import imaplib
import os
import re
import time
from datetime import UTC, datetime, timedelta, timezone
from email.message import Message
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Callable

from .common import write_csv, write_stage_summary
from .config import MailConfig
from .models import MailMatch


URL_RE = re.compile(r"https?://ds\.iris\.edu/pub/userdata/wilber/[^\s<>\"]+\.tar")
MailProgressCallback = Callable[[int, int, int, str], None]


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"missing required environment variable: {name}")
    return value


def decode_header_text(raw_value: str | None) -> str:
    if not raw_value:
        return ""
    parts: list[str] = []
    for chunk, encoding in email.header.decode_header(raw_value):
        if isinstance(chunk, bytes):
            codec = (encoding or "utf-8").lower()
            if codec == "unknown-8bit":
                codec = "utf-8"
            try:
                parts.append(chunk.decode(codec, errors="replace"))
            except LookupError:
                parts.append(chunk.decode("utf-8", errors="replace"))
        else:
            parts.append(chunk)
    return "".join(parts)


def message_text(msg: Message) -> str:
    parts: list[str] = []
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition", ""))
            if "attachment" in disposition.lower():
                continue
            if content_type not in {"text/plain", "text/html"}:
                continue
            payload = part.get_payload(decode=True)
            if payload is None:
                continue
            charset = part.get_content_charset() or "utf-8"
            parts.append(payload.decode(charset, errors="replace"))
    else:
        payload = msg.get_payload(decode=True)
        if payload is not None:
            charset = msg.get_content_charset() or "utf-8"
            parts.append(payload.decode(charset, errors="replace"))
    return "\n".join(parts)


def normalize_date_fields(raw_value: str) -> tuple[str, str, datetime | None]:
    if not raw_value:
        return "", "", None
    dt = parsedate_to_datetime(raw_value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    dt_utc = dt.astimezone(UTC)
    shanghai_tz = timezone(timedelta(hours=8))
    dt_shanghai = dt_utc.astimezone(shanghai_tz)
    return (
        dt_utc.isoformat().replace("+00:00", "Z"),
        dt_shanghai.isoformat(),
        dt_utc,
    )


def extract_label_from_url(url: str) -> str:
    return Path(url).parent.name.lower()


def normalize_download_url(url: str, prefer_https: bool) -> str:
    if prefer_https and url.startswith("http://"):
        return "https://" + url[len("http://") :]
    return url


def load_expected_requests(request_plan_path: Path) -> dict[str, dict[str, str]]:
    requests: dict[str, dict[str, str]] = {}
    with request_plan_path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("SubmitStatus") == "submitted":
                label = row.get("RequestLabel", "").strip().lower()
                if label:
                    requests[label] = {
                        "event_key": row.get("EventKey", "").strip(),
                        "output_event_id": row.get("OutputEventID", "").strip(),
                        "requested_at_utc": row.get("RequestedAtUTC", "").strip(),
                        "track_url": row.get("TrackURL", "").strip(),
                    }
    return requests


def earliest_request_time(expected_requests: dict[str, dict[str, str]]) -> datetime | None:
    values: list[datetime] = []
    for item in expected_requests.values():
        raw_value = item.get("requested_at_utc", "").strip()
        if not raw_value:
            continue
        normalized = raw_value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        values.append(dt.astimezone(UTC))
    if not values:
        return None
    return min(values)


def consistency_check(label: str, expected_requests: dict[str, dict[str, str]]) -> tuple[str, str, str, str]:
    expected = expected_requests.get(label)
    if expected is None:
        return "", "", "unexpected_label", "label not present in current request_plan"
    return (
        expected.get("event_key", ""),
        expected.get("requested_at_utc", ""),
        "matched_request_label",
        "",
    )


def fetch_matches(config: MailConfig, expected_requests: dict[str, dict[str, str]], logger=None) -> list[MailMatch]:
    imap_user = require_env(config.imap_user_env)
    imap_password = require_env(config.imap_password_env)
    now_utc = datetime.now(UTC)
    min_dt = now_utc - timedelta(hours=config.message_lookback_hours)
    requested_min_dt = earliest_request_time(expected_requests)
    if requested_min_dt is not None:
        min_dt = max(min_dt, requested_min_dt - timedelta(minutes=15))

    if logger is not None:
        logger.info("connecting to IMAP host=%s port=%s", config.imap_host, config.imap_port)
    client = imaplib.IMAP4_SSL(config.imap_host, config.imap_port, timeout=config.imap_timeout_seconds)
    client.login(imap_user, imap_password)
    try:
        if logger is not None:
            logger.info("IMAP login succeeded, selecting mailbox=%s", config.mailbox)
        status, _ = client.select(config.mailbox, readonly=True)
        if status != "OK":
            raise RuntimeError(f"failed to open mailbox: {config.mailbox}")

        search_criteria: list[str] = []
        if config.from_substring.strip():
            search_criteria.extend(["FROM", config.from_substring.strip()])
        if config.subject_substring.strip():
            search_criteria.extend(["SUBJECT", config.subject_substring.strip()])
        if requested_min_dt is not None:
            imap_since_dt = requested_min_dt.astimezone(timezone.utc)
            search_criteria.extend(["SINCE", imap_since_dt.strftime("%d-%b-%Y")])

        if search_criteria:
            status, data = client.search(None, *search_criteria)
            if logger is not None:
                logger.info("IMAP server-side search criteria=%s status=%s", " ".join(search_criteria), status)
            if status != "OK" or not data or not data[0]:
                status, data = client.search(None, "ALL")
                if logger is not None:
                    logger.info("IMAP fallback search criteria=ALL status=%s", status)
        else:
            status, data = client.search(None, "ALL")

        if status != "OK" or not data or not data[0]:
            return []
        message_ids = list(reversed(data[0].split()[-config.max_messages :]))

        if logger is not None:
            logger.info("IMAP search returned %s candidate messages", len(message_ids))

        matches: dict[str, MailMatch] = {}
        for message_id in message_ids:
            status, fetched = client.fetch(message_id, "(RFC822)")
            if status != "OK" or not fetched:
                continue
            raw_bytes = None
            for item in fetched:
                if isinstance(item, tuple) and len(item) == 2:
                    raw_bytes = item[1]
                    break
            if raw_bytes is None:
                continue
            msg = email.message_from_bytes(raw_bytes)
            subject = decode_header_text(msg.get("Subject"))
            from_addr = decode_header_text(msg.get("From"))
            if config.subject_substring.lower() not in subject.lower():
                continue
            if config.from_substring.lower() not in from_addr.lower():
                continue

            raw_date = decode_header_text(msg.get("Date"))
            date_utc_text, date_shanghai_text, message_dt = normalize_date_fields(raw_date)
            if message_dt is not None and message_dt < min_dt:
                continue

            body = message_text(msg)
            urls = URL_RE.findall(body)
            if not urls:
                continue

            uid_text = message_id.decode("ascii", errors="replace")
            for original_url in urls:
                download_url = normalize_download_url(original_url, config.prefer_https)
                label = extract_label_from_url(download_url)
                if expected_requests and label not in expected_requests:
                    continue
                event_key, requested_at_utc, consistency_status, consistency_note = consistency_check(label, expected_requests)
                matches.setdefault(
                    download_url,
                    MailMatch(
                        request_label=label,
                        download_url=download_url,
                        original_download_url=original_url,
                        subject=subject,
                        from_addr=from_addr,
                        message_date_raw=raw_date,
                        message_date_utc=date_utc_text,
                        message_date_asia_shanghai=date_shanghai_text,
                        message_uid=uid_text,
                        request_event_key=event_key,
                        requested_at_utc=requested_at_utc,
                        consistency_status=consistency_status,
                        consistency_note=consistency_note,
                    ),
                )
        matched = list(matches.values())
        if logger is not None:
            logger.info("IMAP matched %s success mails for current request labels", len(matched))
        return matched
    finally:
        try:
            client.close()
        except Exception:
            pass
        client.logout()


def write_mail_outputs(stage_dir: Path, matches: list[MailMatch], pending_labels: set[str]) -> None:
    write_csv(
        stage_dir / "success_mail_links.csv",
        [
            "RequestLabel",
            "DownloadURL",
            "OriginalDownloadURL",
            "Subject",
            "From",
            "MessageDateRaw",
            "MessageDateUTC",
            "MessageDateAsiaShanghai",
            "MessageUID",
            "RequestEventKey",
            "RequestedAtUTC",
            "ConsistencyStatus",
            "ConsistencyNote",
        ],
        [
            {
                "RequestLabel": match.request_label,
                "DownloadURL": match.download_url,
                "OriginalDownloadURL": match.original_download_url,
                "Subject": match.subject,
                "From": match.from_addr,
                "MessageDateRaw": match.message_date_raw,
                "MessageDateUTC": match.message_date_utc,
                "MessageDateAsiaShanghai": match.message_date_asia_shanghai,
                "MessageUID": match.message_uid,
                "RequestEventKey": match.request_event_key,
                "RequestedAtUTC": match.requested_at_utc,
                "ConsistencyStatus": match.consistency_status,
                "ConsistencyNote": match.consistency_note,
            }
            for match in matches
        ],
    )
    write_csv(
        stage_dir / "pending_request_labels.csv",
        ["RequestLabel"],
        [{"RequestLabel": label} for label in sorted(pending_labels)],
    )


def poll_success_mail(
    workspace_root: Path,
    config: MailConfig,
    logger,
    progress_callback: MailProgressCallback | None = None,
) -> list[MailMatch]:
    stage_dir = workspace_root / "04_mail"
    stage_dir.mkdir(parents=True, exist_ok=True)
    request_plan_path = workspace_root / "03_requests" / "request_plan.csv"
    expected_requests = load_expected_requests(request_plan_path)
    expected_labels = set(expected_requests)
    if not expected_labels:
        if progress_callback is not None:
            progress_callback(0, 0, 0, "没有需要等待的邮件请求")
        write_mail_outputs(stage_dir, [], set())
        write_stage_summary(
            stage_dir,
            {
                "expected_label_count": 0,
                "received_label_count": 0,
                "pending_label_count": 0,
                "status": "skipped_no_submitted_requests",
            },
        )
        return []

    deadline = time.time() + config.max_wait_minutes * 60
    matches: list[MailMatch] = []
    received_labels: set[str] = set()
    if progress_callback is not None:
        progress_callback(len(expected_labels), 0, len(expected_labels), "正在等待 [Success] 邮件")
    while time.time() <= deadline:
        try:
            matches = fetch_matches(config, expected_requests, logger=logger)
        except Exception as exc:
            logger.warning("mail polling iteration failed: %s", exc)
            if progress_callback is not None:
                progress_callback(len(expected_labels), len(received_labels), len(expected_labels - received_labels), f"邮件检查出错，准备重试: {exc}")
            time.sleep(config.poll_interval_seconds)
            continue
        received_labels = {match.request_label for match in matches}
        pending_labels = expected_labels - received_labels
        if progress_callback is not None:
            progress_callback(
                len(expected_labels),
                len(received_labels),
                len(pending_labels),
                "正在等待剩余 [Success] 邮件" if pending_labels else "全部 [Success] 邮件已收到",
            )
        write_mail_outputs(stage_dir, matches, pending_labels)
        if not pending_labels:
            write_stage_summary(
                stage_dir,
                {
                    "expected_label_count": len(expected_labels),
                    "received_label_count": len(received_labels),
                    "pending_label_count": 0,
                    "status": "completed",
                },
            )
            logger.info("all success mails received: %s", len(received_labels))
            return matches
        logger.info("mail polling pending=%s received=%s", len(pending_labels), len(received_labels))
        time.sleep(config.poll_interval_seconds)

    pending_labels = expected_labels - received_labels
    write_mail_outputs(stage_dir, matches, pending_labels)
    write_stage_summary(
        stage_dir,
        {
            "expected_label_count": len(expected_labels),
            "received_label_count": len(received_labels),
            "pending_label_count": len(pending_labels),
            "status": "partial_timeout_continued" if received_labels else "timeout",
        },
    )
    if received_labels:
        logger.warning(
            "mail polling timed out, but %s/%s success mails already arrived; continuing with received subset",
            len(received_labels),
            len(expected_labels),
        )
        if progress_callback is not None:
            progress_callback(
                len(expected_labels),
                len(received_labels),
                len(pending_labels),
                "等待超时，但已收到部分 [Success] 邮件，继续处理已成功部分",
            )
        return matches
    raise TimeoutError(f"mail polling timed out with {len(pending_labels)} pending request labels")
