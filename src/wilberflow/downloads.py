from __future__ import annotations

import csv
import os
import shutil
import tarfile
from pathlib import Path
from typing import Callable
from urllib.request import urlopen

from .common import write_csv, write_stage_summary
from .config import DownloadConfig
from .models import ExtractionSummary

StageProgressCallback = Callable[[str, int | None, int | None, str | None, str | None], None]

def download_packages(
    workspace_root: Path,
    config: DownloadConfig,
    logger,
    progress_callback: StageProgressCallback | None = None,
) -> list[dict[str, object]]:
    stage_dir = workspace_root / "05_downloads"
    package_dir = stage_dir / "packages"
    package_dir.mkdir(parents=True, exist_ok=True)

    success_mail_csv = workspace_root / "04_mail" / "success_mail_links.csv"
    if not success_mail_csv.exists():
        write_csv(
            stage_dir / "download_manifest.csv",
            ["RequestLabel", "DownloadURL", "OutputPath", "Status", "FileSizeBytes"],
            [],
        )
        write_stage_summary(
            stage_dir,
            {
                "mail_link_count": 0,
                "downloaded_count": 0,
                "skipped_existing_count": 0,
                "package_count": 0,
                "status": "skipped_no_success_mail_csv",
            },
        )
        return []
    rows: list[dict[str, str]]
    with success_mail_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = [dict(row) for row in csv.DictReader(handle)]

    manifest: list[dict[str, object]] = []
    downloaded_count = 0
    skipped_count = 0
    total_rows = len(rows)
    if progress_callback is not None:
        progress_callback("download", 0, total_rows, "等待下载数据包", "running")
    for row in rows:
        url = row["DownloadURL"]
        request_label = row["RequestLabel"]
        filename = Path(url).name
        output_path = package_dir / filename
        completed_count = downloaded_count + skipped_count
        if progress_callback is not None:
            progress_callback("download", completed_count, total_rows, f"正在处理 {completed_count + 1}/{total_rows}: {filename}", "running")
        if output_path.exists() and not config.overwrite:
            skipped_count += 1
            manifest.append(
                {
                    "RequestLabel": request_label,
                    "DownloadURL": url,
                    "OutputPath": str(output_path),
                    "Status": "skipped_existing",
                    "FileSizeBytes": output_path.stat().st_size,
                }
            )
            if progress_callback is not None:
                progress_callback("download", downloaded_count + skipped_count, total_rows, f"已跳过现有文件: {filename}", "running")
            continue
        logger.info("downloading %s", filename)
        with urlopen(url, timeout=config.timeout) as response, output_path.open("wb") as handle:
            shutil.copyfileobj(response, handle, length=config.chunk_size_bytes)
        downloaded_count += 1
        manifest.append(
            {
                "RequestLabel": request_label,
                "DownloadURL": url,
                "OutputPath": str(output_path),
                "Status": "downloaded",
                "FileSizeBytes": output_path.stat().st_size,
            }
        )
        if progress_callback is not None:
            progress_callback("download", downloaded_count + skipped_count, total_rows, f"已下载 {downloaded_count + skipped_count}/{total_rows}: {filename}", "running")

    write_csv(
        stage_dir / "download_manifest.csv",
        ["RequestLabel", "DownloadURL", "OutputPath", "Status", "FileSizeBytes"],
        manifest,
    )
    write_stage_summary(
        stage_dir,
        {
            "mail_link_count": len(rows),
            "downloaded_count": downloaded_count,
            "skipped_existing_count": skipped_count,
            "package_count": len(manifest),
        },
    )
    if progress_callback is not None:
        progress_callback("download", len(manifest), total_rows, f"下载阶段完成：{len(manifest)}/{total_rows}", "completed")
    return manifest


def iter_tar_paths(input_dir: Path) -> list[Path]:
    if not input_dir.exists():
        return []
    return sorted(path for path in input_dir.iterdir() if path.is_file() and path.suffix.lower() == ".tar")


def top_level_name(member_name: str) -> str:
    normalized = member_name.strip("/")
    if not normalized:
        return ""
    return normalized.split("/", 1)[0]


def collect_summary(tar_path: Path) -> tuple[str, int, int, int, list[str]]:
    with tarfile.open(tar_path, "r:*") as archive:
        members = archive.getmembers()
    member_names = [member.name for member in members if member.name]
    top_level_dir = ""
    for name in member_names:
        top_level_dir = top_level_name(name)
        if top_level_dir:
            break
    sac_count = 0
    sacpz_count = 0
    data_center_dirs: set[str] = set()
    for member in members:
        name = member.name.strip("/")
        if not name:
            continue
        lower_name = name.lower()
        if lower_name.endswith(".sac"):
            sac_count += 1
        base_name = Path(name).name.upper()
        if base_name.startswith("SACPZ."):
            sacpz_count += 1
        parts = Path(name).parts
        if top_level_dir and len(parts) >= 2 and parts[0] == top_level_dir:
            second = parts[1]
            if len(parts) >= 3:
                data_center_dirs.add(second)
            elif len(parts) == 2 and member.isdir():
                data_center_dirs.add(second)
    return top_level_dir, len(members), sac_count, sacpz_count, sorted(data_center_dirs)


def safe_extract(archive: tarfile.TarFile, destination: Path) -> None:
    dest_resolved = destination.resolve()
    for member in archive.getmembers():
        target_path = (destination / member.name).resolve()
        if os.path.commonpath([str(dest_resolved), str(target_path)]) != str(dest_resolved):
            raise RuntimeError(f"unsafe tar member path: {member.name}")
    archive.extractall(destination)


def extract_package(tar_path: Path, output_dir: Path, overwrite: bool) -> ExtractionSummary:
    top_level_dir, member_count, sac_count, sacpz_count, data_center_dirs = collect_summary(tar_path)
    extracted_dir = output_dir / top_level_dir if top_level_dir else output_dir / tar_path.stem
    if extracted_dir.exists() and not overwrite:
        return ExtractionSummary(
            package_name=tar_path.name,
            tar_path=str(tar_path),
            extracted_dir=str(extracted_dir),
            top_level_dir=top_level_dir,
            member_count=member_count,
            sac_file_count=sac_count,
            sacpz_file_count=sacpz_count,
            data_center_dirs=";".join(data_center_dirs),
            extracted="skipped_existing",
            note="top-level extracted directory already exists",
        )
    with tarfile.open(tar_path, "r:*") as archive:
        safe_extract(archive, output_dir)
    return ExtractionSummary(
        package_name=tar_path.name,
        tar_path=str(tar_path),
        extracted_dir=str(extracted_dir),
        top_level_dir=top_level_dir,
        member_count=member_count,
        sac_file_count=sac_count,
        sacpz_file_count=sacpz_count,
        data_center_dirs=";".join(data_center_dirs),
        extracted="yes",
        note="",
    )


def extract_packages(
    workspace_root: Path,
    overwrite: bool,
    logger,
    progress_callback: StageProgressCallback | None = None,
) -> list[ExtractionSummary]:
    stage_dir = workspace_root / "06_extract"
    raw_dir = stage_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    tar_paths = iter_tar_paths(workspace_root / "05_downloads" / "packages")
    results: list[ExtractionSummary] = []
    total_paths = len(tar_paths)
    if progress_callback is not None:
        progress_callback("extract", 0, total_paths, "等待解压数据包", "running")
    for index, path in enumerate(tar_paths, start=1):
        if progress_callback is not None:
            progress_callback("extract", index - 1, total_paths, f"正在解压 {index}/{total_paths}: {path.name}", "running")
        results.append(extract_package(path, raw_dir, overwrite))
        if progress_callback is not None:
            progress_callback("extract", index, total_paths, f"已处理 {index}/{total_paths}: {path.name}", "running")
    write_csv(
        stage_dir / "extraction_manifest.csv",
        [
            "PackageName",
            "TarPath",
            "ExtractedDir",
            "TopLevelDir",
            "MemberCount",
            "SacFileCount",
            "SacpzFileCount",
            "DataCenterDirs",
            "Extracted",
            "Note",
        ],
        [
            {
                "PackageName": item.package_name,
                "TarPath": item.tar_path,
                "ExtractedDir": item.extracted_dir,
                "TopLevelDir": item.top_level_dir,
                "MemberCount": item.member_count,
                "SacFileCount": item.sac_file_count,
                "SacpzFileCount": item.sacpz_file_count,
                "DataCenterDirs": item.data_center_dirs,
                "Extracted": item.extracted,
                "Note": item.note,
            }
            for item in results
        ],
    )
    write_stage_summary(
        stage_dir,
        {
            "package_count": len(results),
            "extracted_count": sum(1 for item in results if item.extracted == "yes"),
            "skipped_existing_count": sum(1 for item in results if item.extracted == "skipped_existing"),
            "total_sac_files": sum(item.sac_file_count for item in results),
            "total_sacpz_files": sum(item.sacpz_file_count for item in results),
        },
    )
    if progress_callback is not None:
        progress_callback("extract", len(results), total_paths, f"解压阶段完成：{len(results)}/{total_paths}", "completed")
    logger.info("packages extracted=%s", len(results))
    return results
