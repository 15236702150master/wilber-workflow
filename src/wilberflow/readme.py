from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from .common import ensure_dir, load_csv_rows
from .config import load_config

# Fixed cross-band dedup channel priority, mirrored from wilber.CHANNEL_PREFERENCE_ORDER for the README.
DEDUP_CHANNEL_PRIORITY = "BHZ>HHZ>SHZ>EHZ>DHZ>MHZ>LHZ>VHZ>UHZ"

# 00-07 stage directory descriptions.
STAGE_DIR_DESCRIPTIONS = [
    ("00_config", "配置文件副本（运行所用 TOML 的快照）"),
    ("01_events", "Wilber 事件搜索结果（事件清单与元数据）"),
    ("02_stations", "事件台站筛选结果（每事件的可用/选中台站）"),
    ("03_requests", "生成的 Wilber 请求（请求体与请求清单）"),
    ("04_mail", "[Success] 邮件匹配记录（下载链接来源）"),
    ("05_downloads", "下载的 Wilber 数据包（.tar）"),
    ("06_extract", "解压后的原始 SAC 波形与 SACPZ 响应文件"),
    ("07_final", "最终交付：events/ 去响应后波形 + metadata/ 处理汇总 + README.md"),
]


def _load_summary_json(final_root: Path) -> dict[str, Any]:
    path = final_root / "summary.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_config_for_readme(workspace_root: Path):
    copied = workspace_root / "00_config" / "copied_config.toml"
    if not copied.exists():
        return None
    try:
        return load_config(copied)
    except Exception:
        return None


def _file_distribution(final_root: Path) -> dict[str, Any]:
    rows = load_csv_rows(final_root / "processing_summary.csv")
    by_network = Counter(r.get("Network", "") for r in rows if r.get("Network"))
    by_channel = Counter(r.get("Channel", "") for r in rows if r.get("Channel"))
    return {
        "total": len(rows),
        "by_network": dict(by_network.most_common()),
        "by_channel": dict(by_channel.most_common()),
    }


def _failure_snippets(final_root: Path, limit: int = 20) -> list[dict[str, str]]:
    rows = load_csv_rows(final_root / "processing_failures.csv")
    snippets = []
    for r in rows[:limit]:
        snippets.append({
            "EventID": r.get("EventID", ""),
            "Station": f"{r.get('Network','')}.{r.get('Station','')}.{r.get('Channel','')}".strip("."),
            "Method": r.get("Method", ""),
            "Reason": r.get("Reason", ""),
        })
    return snippets


def _dedup_stats(final_root: Path) -> dict[str, Any]:
    rows = load_csv_rows(final_root / "dedup_summary.csv")
    by_reason = Counter(r.get("Reason", "").split(":", 1)[0] for r in rows if r.get("Reason"))
    return {"dropped": len(rows), "by_reason": dict(by_reason.most_common())}


def _format_kv_line(label: str, value: Any) -> str:
    return f"- {label}：{value}"


def _render_readme(
    workspace_root: Path,
    final_root: Path,
    summary: dict[str, Any],
    cfg,
    distribution: dict[str, Any],
    failures: list[dict[str, str]],
    dedup: dict[str, Any],
    failure_total: int,
) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: list[str] = []
    lines.append("# Wilber 地震数据自动化处理交付说明\n")
    lines.append(f"> 生成时间：{now}　工作目录：`{workspace_root}`\n")

    # 一、事件与处理统计
    lines.append("## 一、事件与处理统计\n")
    lines.append(_format_kv_line("事件数量", summary.get("event_count", "—")))
    lines.append(_format_kv_line(
        "处理作业数 / 成功 / 失败",
        f"{summary.get('trace_job_count','—')} / {summary.get('success_count','—')} / {summary.get('failure_count','—')}",
    ))
    lines.append(_format_kv_line("IRIS 回退成功", summary.get("iris_fallback_success", "—")))
    if dedup.get("dropped", 0) or dedup:
        lines.append(_format_kv_line(
            "跨频带去重（保留 / 移除）",
            f"{distribution.get('total',0)} / {dedup.get('dropped',0)}",
        ))
    lines.append("")

    # 二、目录结构
    lines.append("## 二、目录结构（00–07）\n")
    lines.append("各阶段产物分目录存放，需查看详情时进入对应目录：\n")
    for name, desc in STAGE_DIR_DESCRIPTIONS:
        lines.append(f"- `{name}/`　{desc}")
    lines.append("")

    # 三、处理参数
    lines.append("## 三、处理参数\n")
    if cfg is not None:
        req = cfg.request
        norm = cfg.normalize
        lines.append(_format_kv_line("渠道筛选", req.channels))
        lines.append(_format_kv_line(
            "时间窗口",
            f"P 到时前 {req.window_start_before_min} min ~ P 到时后 {req.window_end_after_min} min",
        ))
        lines.append(_format_kv_line("震中距范围", f"{req.min_distance_deg}°–{req.max_distance_deg}°"))
        lines.append(_format_kv_line("location 优先级", req.location_priority or "（未设置，全部保留）"))
        lines.append(_format_kv_line("去仪器响应 pre_filt", norm.pre_filt))
        lines.append(_format_kv_line("输出单位", norm.output_unit))
        lines.append(_format_kv_line("去响应后端", norm.response_backend))
        lines.append(_format_kv_line(
            "去毛刺",
            f"mode={norm.despike_mode} window={norm.despike_window} nsigma={norm.despike_nsigma}",
        ))
    else:
        lines.append("- 配置文件不可用（00_config/copied_config.toml 缺失或解析失败）")
    lines.append(_format_kv_line("跨频带去重渠道优先级（固定）", DEDUP_CHANNEL_PRIORITY))
    lines.append("")

    # 四、输出文件分布
    lines.append("## 四、输出文件分布\n")
    lines.append(_format_kv_line("文件总数", distribution.get("total", 0)))
    by_net = distribution.get("by_network", {})
    if by_net:
        lines.append("- 按台网：")
        for net, cnt in list(by_net.items())[:15]:
            lines.append(f"  - {net}：{cnt}")
        if len(by_net) > 15:
            lines.append(f"  - …（共 {len(by_net)} 个台网）")
    by_ch = distribution.get("by_channel", {})
    if by_ch:
        lines.append("- 按渠道：")
        for ch, cnt in by_ch.items():
            lines.append(f"  - {ch}：{cnt}")
    lines.append("")

    # 五、处理错误 / 文件异常原文片段
    lines.append("## 五、处理错误 / 文件异常（原文片段）\n")
    if failure_total == 0:
        lines.append("- 无失败记录。")
    else:
        lines.append(f"- 共 {failure_total} 条失败记录，下列为前 {len(failures)} 条：\n")
        lines.append("| 事件 | 台网.台站.渠道 | 方法 | 原因 |")
        lines.append("|---|---|---|---|")
        for f in failures:
            reason = (f["Reason"] or "").replace("|", "\\|")
            lines.append(f"| {f['EventID']} | {f['Station']} | {f['Method']} | {reason} |")
    lines.append("")

    # 六、去重移除记录
    lines.append("## 六、去重移除记录\n")
    dropped = dedup.get("dropped", 0)
    if dropped == 0:
        lines.append("- 无文件被去重移除（或去重未执行）。")
    else:
        lines.append(_format_kv_line("移除文件数", dropped))
        by_reason = dedup.get("by_reason", {})
        if by_reason:
            lines.append("- 按原因：")
            for reason, cnt in by_reason.items():
                lines.append(f"  - {reason}：{cnt}")
        lines.append(f"- 移除文件存放于 `07_final/events/_dedup_dropped/<event_id>/`，可回退。")
    lines.append("")

    return "\n".join(lines)


def generate_delivery_readme(workspace_root: Path, delivery_final_root: Path, logger) -> Path:
    """Render the Chinese delivery README.md at the 07_final root.

    delivery_final_root is the delivered 07_final directory (parent of events/ and metadata/).
    """
    summary = _load_summary_json(delivery_final_root)
    cfg = _load_config_for_readme(workspace_root)
    distribution = _file_distribution(delivery_final_root)
    failures = _failure_snippets(delivery_final_root)
    dedup = _dedup_stats(delivery_final_root)
    failure_rows = load_csv_rows(delivery_final_root / "processing_failures.csv")
    failure_total = len(failure_rows)

    text = _render_readme(workspace_root, delivery_final_root, summary, cfg, distribution, failures, dedup, failure_total)
    target = delivery_final_root / "README.md"
    ensure_dir(target.parent)
    target.write_text(text, encoding="utf-8")
    logger.info("wrote delivery README to %s", target)
    return target
