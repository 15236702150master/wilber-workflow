from __future__ import annotations

import argparse
from pathlib import Path

from .common import load_env_file, setup_logger
from .config import load_config
from .export_final import default_export_roots, export_final_layout
from .pipeline import (
    prepare_workspace,
    run_all,
    run_extraction,
    run_mail_polling,
    run_normalize,
    run_package_download,
    run_request_submission,
    run_resume_from_mail,
    run_search,
    run_station_selection,
)
from .server import serve


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Wilber automation workflow")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in [
        "search-events",
        "select-stations",
        "submit-requests",
        "poll-mail",
        "download-packages",
        "extract-packages",
        "normalize",
        "resume-from-mail",
        "run-all",
    ]:
        sub = subparsers.add_parser(command)
        sub.add_argument("--config", type=Path, required=True)
        sub.add_argument("--workspace-root", type=Path, required=True)
        sub.add_argument("--env-file", type=Path, required=False)
    serve_parser = subparsers.add_parser("serve")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8765)
    serve_parser.add_argument("--workspace-root", type=Path, required=False, default=Path.cwd() / ".wilberflow-studio")
    serve_parser.add_argument("--env-file", type=Path, required=False)
    export_parser = subparsers.add_parser("export-final")
    export_parser.add_argument("--config", type=Path, required=True)
    export_parser.add_argument("--workspace-root", type=Path, required=True)
    export_parser.add_argument("--env-file", type=Path, required=False)
    export_parser.add_argument("--event-root", type=Path, required=False)
    export_parser.add_argument("--metadata-root", type=Path, required=False)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    project_root = Path(__file__).resolve().parents[2]
    if args.env_file:
        load_env_file(args.env_file)
    else:
        default_env = project_root / ".env.local"
        if default_env.exists():
            load_env_file(default_env)
    logger = setup_logger(args.workspace_root / "logs" / "pipeline.log")

    if args.command == "serve":
        serve(args.host, args.port, logger)
        return

    pipeline_config = load_config(args.config)
    prepare_workspace(args.workspace_root, args.config, logger)

    if args.command == "search-events":
        run_search(args.workspace_root, pipeline_config, logger)
    elif args.command == "select-stations":
        run_station_selection(args.workspace_root, pipeline_config, logger)
    elif args.command == "submit-requests":
        run_request_submission(args.workspace_root, pipeline_config, logger)
    elif args.command == "poll-mail":
        run_mail_polling(args.workspace_root, pipeline_config, logger)
    elif args.command == "download-packages":
        run_package_download(args.workspace_root, pipeline_config, logger)
    elif args.command == "extract-packages":
        run_extraction(args.workspace_root, pipeline_config, logger)
    elif args.command == "normalize":
        run_normalize(args.workspace_root, pipeline_config, logger)
    elif args.command == "resume-from-mail":
        run_resume_from_mail(args.workspace_root, pipeline_config, logger)
    elif args.command == "run-all":
        run_all(args.workspace_root, pipeline_config, logger)
    elif args.command == "export-final":
        default_event_root, default_metadata_root = default_export_roots(args.workspace_root)
        export_final_layout(
            workspace_root=args.workspace_root,
            event_root=args.event_root or default_event_root,
            metadata_root=args.metadata_root or default_metadata_root,
            logger=logger,
        )


if __name__ == "__main__":
    main()
