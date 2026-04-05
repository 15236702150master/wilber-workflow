from __future__ import annotations

import csv
import fnmatch
import json
import logging
import os
import re
import sys
import time
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode
from urllib.error import URLError
from urllib.request import HTTPCookieProcessor, Request, build_opener

from obspy import UTCDateTime


DEFAULT_EVENT_SERVICE_URL = "https://service.iris.edu/fdsnws/event/1/query"
DEFAULT_WILBER_BASE_URL = "https://ds.iris.edu/wilber3"
DEFAULT_USER_AGENT = "winner-wilber-workflow/0.1"
WINDOWS_DRIVE_PATTERN = re.compile(r"^(?P<drive>[A-Za-z]):[\\/](?P<rest>.*)$")


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def normalize_user_path_text(text: str) -> str:
    raw = text.strip()
    if os.name == "nt":
        return raw
    match = WINDOWS_DRIVE_PATTERN.match(raw)
    if not match:
        return raw
    drive = match.group("drive").lower()
    rest = match.group("rest").replace("\\", "/").lstrip("/")
    return f"/mnt/{drive}/{rest}"


def resolve_user_path(text: str) -> Path:
    return Path(normalize_user_path_text(text)).expanduser().resolve()


def setup_logger(log_path: Path, logger_name: str = "wilberflow") -> logging.Logger:
    ensure_dir(log_path.parent)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, object]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_json(path: Path, payload: object) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def write_key_value_csv(path: Path, payload: dict[str, object]) -> None:
    write_csv(
        path,
        ["Key", "Value"],
        [{"Key": key, "Value": value} for key, value in payload.items()],
    )


def write_stage_summary(stage_dir: Path, payload: dict[str, object]) -> None:
    write_json(stage_dir / "summary.json", payload)
    write_key_value_csv(stage_dir / "summary.csv", payload)


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def build_http_opener(user_agent: str = DEFAULT_USER_AGENT):
    cookie_jar = CookieJar()
    opener = build_opener(HTTPCookieProcessor(cookie_jar))
    opener.addheaders = [("User-Agent", user_agent)]
    return opener


def http_get_text(
    opener,
    url: str,
    params: dict[str, object] | None = None,
    timeout: int = 30,
    retry_attempts: int = 1,
    retry_sleep_seconds: float = 0.0,
) -> str:
    final_url = url
    if params:
        query = urlencode([(key, value) for key, value in params.items() if value is not None])
        final_url = f"{url}?{query}"
    last_error: Exception | None = None
    for attempt in range(1, max(1, retry_attempts) + 1):
        try:
            with opener.open(final_url, timeout=timeout) as response:
                return response.read().decode("utf-8")
        except (URLError, TimeoutError, OSError) as exc:
            last_error = exc
            if attempt >= max(1, retry_attempts):
                raise
            if retry_sleep_seconds > 0:
                time.sleep(retry_sleep_seconds)
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"unexpected http_get_text state for {final_url}")


def http_post_form(
    opener,
    url: str,
    data: dict[str, object],
    timeout: int = 30,
    headers: dict[str, str] | None = None,
    retry_attempts: int = 1,
    retry_sleep_seconds: float = 0.0,
) -> tuple[int, str]:
    encoded = urlencode([(key, value) for key, value in data.items()]).encode("utf-8")
    last_error: Exception | None = None
    for attempt in range(1, max(1, retry_attempts) + 1):
        try:
            request = Request(url, data=encoded)
            request.add_header("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
            if headers:
                for key, value in headers.items():
                    request.add_header(key, value)
            with opener.open(request, timeout=timeout) as response:
                return response.getcode(), response.read().decode("utf-8")
        except (URLError, TimeoutError, OSError) as exc:
            last_error = exc
            if attempt >= max(1, retry_attempts):
                raise
            if retry_sleep_seconds > 0:
                time.sleep(retry_sleep_seconds)
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"unexpected http_post_form state for {url}")


def sanitize_text(text: str, fallback: str = "unknown", max_length: int = 120) -> str:
    cleaned = re.sub(r"[^\w.-]+", "_", text.strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("._")
    cleaned = cleaned[:max_length].strip("._")
    return cleaned or fallback


def format_ws_datetime(value: UTCDateTime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S")


def parse_filter_tokens(text: str) -> list[str]:
    return [item.strip() for item in text.split(",") if item.strip()]


def normalize_location_token(token: str) -> str:
    return "" if token.strip() == "--" else token.strip()


def parse_location_priority(text: str) -> list[str]:
    return [normalize_location_token(item) for item in text.split(",") if item.strip()]


def match_any(value: str, patterns: list[str]) -> bool:
    if not patterns:
        return True
    return any(fnmatch.fnmatchcase(value, pattern) for pattern in patterns)


def parse_pre_filt(text: str) -> tuple[float, float, float, float]:
    parts = [float(part.strip()) for part in text.split(",") if part.strip()]
    if len(parts) != 4:
        raise ValueError(f"pre_filt must contain exactly four numbers, got: {text}")
    if not parts[0] < parts[1] < parts[2] < parts[3]:
        raise ValueError(f"pre_filt must satisfy f1 < f2 < f3 < f4, got: {text}")
    return tuple(parts)  # type: ignore[return-value]


def load_env_file(path: Path) -> int:
    if not path.exists():
        return 0
    loaded = 0
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key:
            continue
        os.environ[key] = value
        loaded += 1
    return loaded
