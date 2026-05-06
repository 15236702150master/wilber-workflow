from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from urllib import request

from .config import NotifyConfig


def should_send_feishu_notification(config: NotifyConfig, status: str) -> bool:
    if not config.feishu_webhook_url.strip():
        return False
    if status == "completed":
        return config.notify_on_success
    if status == "failed":
        return config.notify_on_failure
    return False


def build_feishu_workflow_message(
    *,
    status: str,
    mode: str,
    batch_id: str,
    workspace_root: Path,
    log_path: Path,
    finished_at_utc: str | None,
    detail: str,
) -> str:
    status_text = "成功" if status == "completed" else "失败"
    mode_text = "完整流程" if mode == "run_all" else "补跑收信与下载"
    lines = [
        f"WilberFlow 运行{status_text}",
        f"批次号：{batch_id or '未记录'}",
        f"运行模式：{mode_text}",
        f"完成时间：{_format_finished_time(finished_at_utc)}",
        f"工作目录：{workspace_root}",
        f"日志文件：{log_path}",
    ]
    if detail.strip():
        lines.append(f"说明：{detail.strip()}")
    return "\n".join(lines)


def send_feishu_text_message(config: NotifyConfig, text: str) -> None:
    payload = json.dumps(
        {
            "msg_type": "text",
            "content": {
                "text": text,
            },
        },
        ensure_ascii=False,
    ).encode("utf-8")
    http_request = request.Request(
        config.feishu_webhook_url,
        data=payload,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with request.urlopen(http_request, timeout=config.timeout_seconds) as response:
        response.read()


def _format_finished_time(value: str | None) -> str:
    if not value:
        return "未知"
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return value
    local_dt = parsed.astimezone(timezone.utc)
    return local_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
