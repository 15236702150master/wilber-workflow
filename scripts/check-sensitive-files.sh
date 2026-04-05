#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v rg >/dev/null 2>&1; then
  echo "error: ripgrep (rg) is required for this check." >&2
  exit 1
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "error: this script must run inside the git repository." >&2
  exit 1
fi

TMP_FILE_LIST="$(mktemp)"
trap 'rm -f "$TMP_FILE_LIST"' EXIT

git ls-files -co --exclude-standard > "$TMP_FILE_LIST"

if [ ! -s "$TMP_FILE_LIST" ]; then
  echo "No tracked or publishable files found."
  exit 0
fi

mapfile -t FILES < "$TMP_FILE_LIST"

FOUND=0

run_check() {
  local label="$1"
  local pattern="$2"
  local ignore_pattern="${3:-}"

  local result
  if [ -n "$ignore_pattern" ]; then
    result="$(rg -n -I -e "$pattern" "${FILES[@]}" 2>/dev/null | rg -v "$ignore_pattern" || true)"
  else
    result="$(rg -n -I -e "$pattern" "${FILES[@]}" 2>/dev/null || true)"
  fi

  if [ -n "$result" ]; then
    FOUND=1
    echo
    echo "[warn] $label"
    printf '%s\n' "$result"
  fi
}

run_check "Possible GitHub token" 'ghp_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}'
run_check "Possible private key material" '-----BEGIN (RSA|OPENSSH|EC|DSA|PGP) PRIVATE KEY-----'
run_check "Possible QQ IMAP auth code assignment" 'QQ_IMAP_AUTH_CODE\s*=\s*.+' 'your_mail_auth_code|example_auth_code|placeholder_auth_code'
run_check "Possible real QQ mailbox assignment" 'QQ_IMAP_USER\s*=\s*.+@qq\.com' 'your_qq_mail@qq\.com|example@qq\.com|placeholder@qq\.com'
run_check "Possible generic mail credential assignment" '(REQUEST_EMAIL|EMAIL|MAIL_USER)\s*=\s*.+@' 'example@|your_|placeholder'
run_check "Possible cookie header" '(^|[^A-Za-z])cookie[:=]'
run_check "Possible bearer token" 'Bearer [A-Za-z0-9._-]{20,}'

if [ "$FOUND" -ne 0 ]; then
  echo
  echo "Sensitive-looking content found. Review the lines above before publishing."
  exit 1
fi

echo "No obvious secrets found in tracked or publishable files."
