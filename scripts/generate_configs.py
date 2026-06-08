"""
Generate config_1.json … config_N.json for multi-account runs.

Input
-----
A JSON roster describing one entry per account. Defaults to
`scripts/accounts.json`:

    [
      {
        "funder": "0xAAAA...",
        "clash_port": 7901
      },
      {
        "funder": "0xBBBB...",
        "clash_port": 7902
      }
    ]

You can also set `signer_server_url` / `signer_token` per account to
override the base. Everything else (markets, ws_url, rest_base_url) is
inherited from the base config.

Output
------
`config_1.json` … `config_N.json` sitting next to the base config, so
multi_runner.py picks them up automatically.

Usage
-----
    python scripts/generate_configs.py
    python scripts/generate_configs.py --roster scripts/accounts.json
    python scripts/generate_configs.py --base platforms/polymarket/maker/config.json
    python scripts/generate_configs.py --out-dir platforms/polymarket/maker
    python scripts/generate_configs.py --dry-run
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BASE = REPO_ROOT / "platforms" / "polymarket" / "maker" / "config.json"
DEFAULT_ROSTER = REPO_ROOT / "scripts" / "accounts.json"
DEFAULT_OUT_DIR = DEFAULT_BASE.parent


def _load_json(path: Path) -> dict | list:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        sys.exit(f"ERROR: file not found: {path}")
    except json.JSONDecodeError as e:
        sys.exit(f"ERROR: {path} is not valid JSON — {e}")


def _validate_roster(roster: object) -> list[dict]:
    if not isinstance(roster, list) or not roster:
        sys.exit("ERROR: roster must be a non-empty JSON array")
    if len(roster) > 30:
        sys.exit(f"ERROR: roster has {len(roster)} accounts; multi_runner caps at 30")
    ports_seen: dict[int, int] = {}
    funders_seen: dict[str, int] = {}
    for i, entry in enumerate(roster, start=1):
        if not isinstance(entry, dict):
            sys.exit(f"ERROR: roster entry {i} must be an object")
        funder = str(entry.get("funder", "")).strip()
        port = entry.get("clash_port")
        if not funder.startswith("0x") or len(funder) != 42:
            sys.exit(f"ERROR: roster entry {i}: funder must be a 0x… 42-char address (got {funder!r})")
        if not isinstance(port, int) or not (1024 <= port <= 65535):
            sys.exit(f"ERROR: roster entry {i}: clash_port must be an int 1024–65535 (got {port!r})")
        if port in ports_seen:
            sys.exit(f"ERROR: clash_port {port} used by both account {ports_seen[port]} and {i}")
        if funder.lower() in funders_seen:
            sys.exit(f"ERROR: funder {funder} used by both account {funders_seen[funder.lower()]} and {i}")
        ports_seen[port] = i
        funders_seen[funder.lower()] = i
    return roster  # type: ignore[return-value]


def _build_proxy_pool(clash_host: str, port: int) -> dict:
    return {
        "enabled": True,
        "use_for_reads": True,
        "use_for_ws": True,
        "items": [
            {"url": f"http://{clash_host}:{port}", "enabled": True}
        ],
    }


def _render(base: dict, entry: dict, clash_host: str) -> dict:
    out = copy.deepcopy(base)
    account = dict(out.get("account") or {})
    # Keep existing signer_server_url / signer_token from base unless overridden
    account["funder"] = entry["funder"]
    # A blank private_key is the expected state for remote-signer mode; the
    # signer holds all keys on the Mac Mini.
    account["private_key"] = "REDACTED"
    if "signer_server_url" in entry:
        account["signer_server_url"] = entry["signer_server_url"]
    if "signer_token" in entry:
        account["signer_token"] = entry["signer_token"]
    out["account"] = account
    out["proxy_pool"] = _build_proxy_pool(clash_host, entry["clash_port"])
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate per-account config_N.json from a roster")
    ap.add_argument("--roster", default=str(DEFAULT_ROSTER), help=f"JSON roster (default: {DEFAULT_ROSTER})")
    ap.add_argument("--base", default=str(DEFAULT_BASE), help=f"Base config to clone (default: {DEFAULT_BASE})")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help=f"Output directory (default: {DEFAULT_OUT_DIR})")
    ap.add_argument("--clash-host", default="127.0.0.1", help="Host for the Clash listeners (default: 127.0.0.1)")
    ap.add_argument("--dry-run", action="store_true", help="Print what would be written without touching disk")
    args = ap.parse_args()

    base_path = Path(args.base).resolve()
    roster_path = Path(args.roster).resolve()
    out_dir = Path(args.out_dir).resolve()

    base = _load_json(base_path)
    if not isinstance(base, dict):
        sys.exit(f"ERROR: base {base_path} must be a JSON object")
    roster = _validate_roster(_load_json(roster_path))

    if not args.dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Base:   {base_path}")
    print(f"Roster: {roster_path}  ({len(roster)} account(s))")
    print(f"Output: {out_dir}")
    print()

    for idx, entry in enumerate(roster, start=1):
        cfg = _render(base, entry, args.clash_host)
        out_path = out_dir / f"config_{idx}.json"
        funder = entry["funder"]
        port = entry["clash_port"]
        if args.dry_run:
            print(f"  [dry-run] would write {out_path.name}  funder={funder[:10]}… port={port}")
            continue
        if out_path.exists():
            # Confirm that we're only overwriting a generated config, not a
            # hand-maintained one. Heuristic: generated ones always have
            # `proxy_pool.items[0].url` pointing at the matching clash port.
            existing = _load_json(out_path)
            if not (
                isinstance(existing, dict)
                and existing.get("proxy_pool", {}).get("items")
            ):
                sys.exit(f"ERROR: {out_path} exists and doesn't look generated. Back it up first.")
        out_path.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  wrote {out_path.name}  funder={funder[:10]}… port={port}")

    if args.dry_run:
        print("\n(dry-run — no files written)")
    else:
        print(f"\nDone. multi_runner.py will now pick up {len(roster)} account(s).")


if __name__ == "__main__":
    main()
