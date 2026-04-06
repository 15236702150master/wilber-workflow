# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, adapted to the current stage of this repository.

## [Unreleased]

### Added

- GitHub issue templates for bug reports and feature requests
- Pull request template
- GitHub Actions CI workflow
- Repository badges for CI, release, and stars
- `SECURITY.md`
- `CHANGELOG.md`

### Changed

- CI workflow now installs `ripgrep` before running the sensitive file check
- CI workflow updated to newer GitHub Action major versions and explicit read-only contents permission
- Repository metadata, topics, labels, and public-facing documentation were refined after the initial release

## [0.1.0] - 2026-04-06

### Added

- Local web studio for configuring Wilber workflows
- Wilber event search and station selection pipeline
- Wilber request generation and optional submission flow
- QQ IMAP success mail polling and resume support
- Package download, extraction, response removal, and final event export pipeline
- Batch-based workspace layout with per-stage summaries and event-level resume behavior
- Local cache reuse for raw station lists
- Example config, example output tree, release checklist, contributing guide, and public README assets

### Notes

- Recommended runtime remains `WSL/Linux` for the service side
- Output directories may still point to Linux paths or mounted Windows paths such as `/mnt/d/...`
