#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' \
  'DISABLED: polymarket-bot is not the Latitude Dashboard source.' \
  'Use the reviewed exact-SHA release workflow in ejson8282/latitude-alpha.' >&2
exit 64
