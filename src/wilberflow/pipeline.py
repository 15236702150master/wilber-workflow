from __future__ import annotations

from pathlib import Path
from typing import Callable

from .config import PipelineConfig, copy_config_into_workspace
from .downloads import download_packages, extract_packages
from .export_final import default_export_roots, export_final_layout
from .mail import MailProgressCallback, poll_success_mail
from .normalize import normalize_workspace
from .wilber import build_requests, fetch_and_select_stations, query_events

StageCallback = Callable[[str, str], None]
StageProgressCallback = Callable[[str, int | None, int | None, str | None, str | None, dict[str, object] | None], None]

WORKFLOW_STAGE_DEFINITIONS: dict[str, dict[str, str]] = {
    "events": {"key": "events", "label": "搜索事件", "message": "正在搜索 Wilber 事件"},
    "stations": {"key": "stations", "label": "筛选台站", "message": "正在获取并筛选事件台站"},
    "requests": {"key": "requests", "label": "生成请求", "message": "正在生成并提交 Wilber 请求"},
    "mail": {"key": "mail", "label": "检查邮件", "message": "正在等待并检查 [Success] 邮件"},
    "download": {"key": "download", "label": "下载数据", "message": "正在下载 Wilber 数据包"},
    "extract": {"key": "extract", "label": "解压数据", "message": "正在解压原始数据包"},
    "response": {"key": "response", "label": "去仪器响应", "message": "正在去仪器响应"},
    "deliver": {"key": "deliver", "label": "整理交付", "message": "正在整理最终交付目录"},
}


def workflow_stage_sequence(pipeline_config: PipelineConfig, mode: str = "run_all") -> list[dict[str, str]]:
    if mode == "resume_from_mail":
        stage_keys = ["mail", "download", "extract", "response", "deliver"]
    else:
        stage_keys = ["events", "stations"]
        if not pipeline_config.request.metadata_only:
            stage_keys.append("requests")
            if pipeline_config.request.submit:
                stage_keys.extend(["mail", "download", "extract", "response", "deliver"])
    return [dict(WORKFLOW_STAGE_DEFINITIONS[key]) for key in stage_keys]


def _enter_stage(stage_callback: StageCallback | None, stage_key: str) -> None:
    if stage_callback is None:
        return
    stage = WORKFLOW_STAGE_DEFINITIONS[stage_key]
    stage_callback(stage["key"], stage["message"])


def prepare_workspace(workspace_root: Path, config_path: Path, logger) -> None:
    workspace_root.mkdir(parents=True, exist_ok=True)
    copy_config_into_workspace(config_path, workspace_root)
    logger.info("workspace prepared at %s", workspace_root)


def run_search(
    workspace_root: Path,
    pipeline_config: PipelineConfig,
    logger,
    progress_callback: StageProgressCallback | None = None,
) -> None:
    query_events(pipeline_config.event_search, workspace_root / "01_events", logger, progress_callback=progress_callback)


def run_station_selection(
    workspace_root: Path,
    pipeline_config: PipelineConfig,
    logger,
    progress_callback: StageProgressCallback | None = None,
) -> None:
    fetch_and_select_stations(workspace_root, pipeline_config, logger, progress_callback=progress_callback)


def run_request_submission(
    workspace_root: Path,
    pipeline_config: PipelineConfig,
    logger,
    progress_callback: StageProgressCallback | None = None,
) -> None:
    build_requests(workspace_root, pipeline_config, logger, progress_callback=progress_callback)


def run_mail_polling(
    workspace_root: Path,
    pipeline_config: PipelineConfig,
    logger,
    progress_callback: MailProgressCallback | None = None,
) -> None:
    poll_success_mail(workspace_root, pipeline_config.mail, logger, progress_callback=progress_callback)


def run_package_download(
    workspace_root: Path,
    pipeline_config: PipelineConfig,
    logger,
    progress_callback: StageProgressCallback | None = None,
) -> None:
    download_packages(workspace_root, pipeline_config.download, logger, progress_callback=progress_callback)


def run_extraction(
    workspace_root: Path,
    pipeline_config: PipelineConfig,
    logger,
    progress_callback: StageProgressCallback | None = None,
) -> None:
    extract_packages(workspace_root, pipeline_config.download.overwrite, logger, progress_callback=progress_callback)


def run_normalize(
    workspace_root: Path,
    pipeline_config: PipelineConfig,
    logger,
    response_progress_callback: StageProgressCallback | None = None,
) -> None:
    normalize_workspace(workspace_root, pipeline_config, logger, progress_callback=response_progress_callback)


def run_delivery_export(
    workspace_root: Path,
    logger,
    progress_callback: StageProgressCallback | None = None,
) -> None:
    event_root, metadata_root = default_export_roots(workspace_root)
    export_final_layout(
        workspace_root=workspace_root,
        event_root=event_root,
        metadata_root=metadata_root,
        logger=logger,
        progress_callback=progress_callback,
    )
    logger.info("exported final delivery layout to %s and %s", event_root, metadata_root)


def run_resume_from_mail(
    workspace_root: Path,
    pipeline_config: PipelineConfig,
    logger,
    stage_callback: StageCallback | None = None,
    mail_progress_callback: MailProgressCallback | None = None,
    stage_progress_callback: StageProgressCallback | None = None,
) -> None:
    _enter_stage(stage_callback, "mail")
    run_mail_polling(workspace_root, pipeline_config, logger, progress_callback=mail_progress_callback)
    _enter_stage(stage_callback, "download")
    run_package_download(workspace_root, pipeline_config, logger, progress_callback=stage_progress_callback)
    _enter_stage(stage_callback, "extract")
    run_extraction(workspace_root, pipeline_config, logger, progress_callback=stage_progress_callback)
    _enter_stage(stage_callback, "response")
    run_normalize(workspace_root, pipeline_config, logger, response_progress_callback=stage_progress_callback)
    _enter_stage(stage_callback, "deliver")
    run_delivery_export(workspace_root, logger, progress_callback=stage_progress_callback)


def run_all(
    workspace_root: Path,
    pipeline_config: PipelineConfig,
    logger,
    stage_callback: StageCallback | None = None,
    mail_progress_callback: MailProgressCallback | None = None,
    stage_progress_callback: StageProgressCallback | None = None,
) -> None:
    _enter_stage(stage_callback, "events")
    run_search(workspace_root, pipeline_config, logger, progress_callback=stage_progress_callback)
    _enter_stage(stage_callback, "stations")
    run_station_selection(workspace_root, pipeline_config, logger, progress_callback=stage_progress_callback)
    if pipeline_config.request.metadata_only:
        logger.info(
            "metadata_only enabled; stopping after station selection with 01_events and 02_stations outputs",
        )
        return
    _enter_stage(stage_callback, "requests")
    run_request_submission(workspace_root, pipeline_config, logger, progress_callback=stage_progress_callback)
    if not pipeline_config.request.submit:
        logger.info(
            "request.submit disabled; stopping after request planning with 03_requests outputs",
        )
        return
    _enter_stage(stage_callback, "mail")
    run_mail_polling(workspace_root, pipeline_config, logger, progress_callback=mail_progress_callback)
    _enter_stage(stage_callback, "download")
    run_package_download(workspace_root, pipeline_config, logger, progress_callback=stage_progress_callback)
    _enter_stage(stage_callback, "extract")
    run_extraction(workspace_root, pipeline_config, logger, progress_callback=stage_progress_callback)
    _enter_stage(stage_callback, "response")
    run_normalize(workspace_root, pipeline_config, logger, response_progress_callback=stage_progress_callback)
    _enter_stage(stage_callback, "deliver")
    run_delivery_export(workspace_root, logger, progress_callback=stage_progress_callback)
