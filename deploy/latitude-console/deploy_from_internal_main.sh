#!/usr/bin/env bash
set -euo pipefail

SOURCE_BARE="${LATITUDE_SOURCE_BARE:-/home/ubuntu/repos/polymarket-bot.git}"
SOURCE_REF="${LATITUDE_SOURCE_REF:-refs/heads/main}"
TARGET_ROOT="${LATITUDE_TARGET_ROOT:-/home/ubuntu/polymarket-bot}"
PYTHON="${LATITUDE_PYTHON:-${TARGET_ROOT}/.venv/bin/python}"
SERVICE="${LATITUDE_CONSOLE_SERVICE:-latitude-console.service}"

tmp_dir="$(mktemp -d /tmp/latitude-console-deploy.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT

git --git-dir="$SOURCE_BARE" archive "$SOURCE_REF" \
  deploy/latitude-console/console_app.py \
  deploy/latitude-console/console.html \
  deploy/latitude-console/ipo_import_daily.sh \
  deploy/systemd/latitude-ipo-import.timer \
  tests/test_latitude_console_truth.py \
  | tar -xf - -C "$tmp_dir"

(
  cd "$tmp_dir"
  PYTHONPATH=. "$PYTHON" -m pytest -q tests/test_latitude_console_truth.py
)

stamp="$(date +%Y%m%d-%H%M%S)"
target_dir="${TARGET_ROOT}/deploy/latitude-console"
cp "${target_dir}/console_app.py" "${target_dir}/console_app.py.bak-${stamp}"
cp "${target_dir}/console.html" "${target_dir}/console.html.bak-${stamp}"
install -m 0644 "${tmp_dir}/deploy/latitude-console/console_app.py" "${target_dir}/console_app.py"
install -m 0644 "${tmp_dir}/deploy/latitude-console/console.html" "${target_dir}/console.html"

sudo systemctl restart "$SERVICE"
sudo systemctl is-active --quiet "$SERVICE"

test "$(sha256sum "${tmp_dir}/deploy/latitude-console/console_app.py" | cut -d' ' -f1)" = \
     "$(sha256sum "${target_dir}/console_app.py" | cut -d' ' -f1)"
test "$(sha256sum "${tmp_dir}/deploy/latitude-console/console.html" | cut -d' ' -f1)" = \
     "$(sha256sum "${target_dir}/console.html" | cut -d' ' -f1)"

printf 'Latitude console deployed from %s at %s\n' \
  "$(git --git-dir="$SOURCE_BARE" rev-parse "$SOURCE_REF")" "$stamp"
