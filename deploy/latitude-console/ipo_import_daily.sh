#!/usr/bin/env bash
set -u

IMPORT_URL="${IPO_IMPORT_URL:-http://100.82.86.62:8080/dashboard/ipo/import/hkex}"
SUCCESS_STAMP="${IPO_IMPORT_SUCCESS_STAMP:-/home/ubuntu/ipo_import.success}"
LOG_FILE="${IPO_IMPORT_LOG:-/home/ubuntu/ipo_import.log}"
TODAY="$(date +%F)"

# The timer runs frequently so a transient Windows/Tailnet outage can recover.  Once
# today's import succeeded, later checks are intentionally no-ops.
if [[ -f "$SUCCESS_STAMP" ]] && grep -q "^${TODAY}T" "$SUCCESS_STAMP"; then
  exit 0
fi

TMP_RESPONSE="$(mktemp /tmp/ipo-import.XXXXXX.json)"
trap 'rm -f "$TMP_RESPONSE"' EXIT
printf '%s import start\n' "$(date --iso-8601=seconds)" >> "$LOG_FILE"

CURL_RC=0
curl -fS --connect-timeout 8 --max-time 180 \
  -X POST "$IMPORT_URL" \
  -H "Content-Type: application/json" \
  -d '{}' \
  -o "$TMP_RESPONSE" || CURL_RC=$?

if [[ "$CURL_RC" -eq 0 ]] \
  && grep -q '"ok"[[:space:]]*:[[:space:]]*true' "$TMP_RESPONSE"; then
  STAMP_TMP="${SUCCESS_STAMP}.tmp"
  date --iso-8601=seconds > "$STAMP_TMP"
  mv "$STAMP_TMP" "$SUCCESS_STAMP"
  printf '%s import success (%s bytes)\n' \
    "$(date --iso-8601=seconds)" "$(wc -c < "$TMP_RESPONSE")" >> "$LOG_FILE"
  exit 0
fi

if [[ "$CURL_RC" -eq 0 ]]; then
  CURL_RC=65
fi
printf '%s import failed (rc=%s); timer will retry\n' \
  "$(date --iso-8601=seconds)" "$CURL_RC" >> "$LOG_FILE"
exit 1
