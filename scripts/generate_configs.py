"""
Generate config_1.json … config_N.json for multi-account runs.

Input
-----
A non-secret JSON roster describing one entry per account. Defaults to
`scripts/accounts.json`:

    {
      "schema_version": 1,
      "accounts": [
        {
          "account_index": 1,
          "host_id": "vps1",
          "funder": "0xAAAA...",
          "clash_port": 7901
        }
      ]
    }

Optional `lp_account` metadata configures an account's LP type, principal, and
shared allocation. Signer tokens and private credentials are rejected; they
must come from the host environment. Everything else (markets, ws_url,
rest_base_url) is inherited from the base config.

Output
------
`config_1.json` … `config_N.json` sitting next to the base config, so
multi_runner.py picks them up automatically.

Usage
-----
    python scripts/generate_configs.py
    python scripts/generate_configs.py --roster scripts/accounts.json
    python scripts/generate_configs.py --base platforms/polymarket/maker/config.json
    python scripts/generate_configs.py --market-universe /path/markets.runtime.json
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
sys.path.insert(0, str(REPO_ROOT))

from platforms.polymarket.maker.account_roster import (  # noqa: E402
    local_runtime_accounts,
    market_universe_sha256,
    parse_runtime_roster,
    runtime_roster_scope,
    roster_hosts,
    routing_roster_sha256,
)
from platforms.polymarket.maker.market_universe import (  # noqa: E402
    apply_market_universe,
    load_json_object,
)

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
    try:
        accounts = parse_runtime_roster(roster)
    except ValueError as exc:
        sys.exit(f"ERROR: {exc}")
    return [account.generation_entry() for account in accounts]


def _build_proxy_pool(clash_host: str, port: int) -> dict:
    return {
        "enabled": True,
        "use_for_reads": True,
        "use_for_ws": True,
        "items": [
            {"url": f"http://{clash_host}:{port}", "enabled": True}
        ],
    }


def _render(
    base: dict,
    entry: dict,
    clash_host: str,
    *,
    roster_sha256: str | None = None,
    runtime_scope: str = "",
) -> dict:
    out = copy.deepcopy(base)
    account = dict(out.get("account") or {})
    account["funder"] = entry["funder"]
    # Generated configs are non-secret. Runtime authentication comes from the
    # host environment; private keys remain on the Mac mini signer.
    account["private_key"] = "REDACTED"
    for secret_field in (
        "api_key",
        "api_passphrase",
        "api_secret",
        "signer_token",
    ):
        account.pop(secret_field, None)
    out["account"] = account
    if "lp_account" in entry:
        # LP metadata is intentionally non-sensitive and account-local.
        out["lp_account"] = copy.deepcopy(entry["lp_account"])
    else:
        out.pop("lp_account", None)
    if roster_sha256:
        out["runtime_account"] = {
            "account_index": int(entry["account_index"]),
            "host_id": str(entry["host_id"]),
            "clash_port": int(entry["clash_port"]),
            "routing_roster_sha256": roster_sha256,
            "market_universe_sha256": market_universe_sha256(out),
        }
        if runtime_scope:
            out["runtime_account"]["runtime_scope"] = runtime_scope
    out["proxy_pool"] = _build_proxy_pool(clash_host, entry["clash_port"])
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate per-account config_N.json from a roster")
    ap.add_argument("--roster", default=str(DEFAULT_ROSTER), help=f"JSON roster (default: {DEFAULT_ROSTER})")
    ap.add_argument("--base", default=str(DEFAULT_BASE), help=f"Base config to clone (default: {DEFAULT_BASE})")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help=f"Output directory (default: {DEFAULT_OUT_DIR})")
    ap.add_argument("--clash-host", default="127.0.0.1", help="Host for the Clash listeners (default: 127.0.0.1)")
    ap.add_argument(
        "--market-universe",
        default="",
        help="Reviewed market-universe JSON; required for multi-host rosters",
    )
    ap.add_argument(
        "--host-id",
        default="",
        help="Generate only accounts assigned to this host (required for multi-host rosters)",
    )
    ap.add_argument("--dry-run", action="store_true", help="Print what would be written without touching disk")
    args = ap.parse_args()

    base_path = Path(args.base).resolve()
    roster_path = Path(args.roster).resolve()
    out_dir = Path(args.out_dir).resolve()

    base = _load_json(base_path)
    if not isinstance(base, dict):
        sys.exit(f"ERROR: base {base_path} must be a JSON object")
    roster_payload = _load_json(roster_path)
    try:
        accounts = parse_runtime_roster(roster_payload)
        runtime_scope = runtime_roster_scope(roster_payload)
    except ValueError as exc:
        sys.exit(f"ERROR: {exc}")

    requested_host = args.host_id.strip().lower()
    hosts = roster_hosts(accounts)
    if len(hosts) > 1 and not args.market_universe:
        sys.exit("ERROR: --market-universe is required for a multi-host roster")
    if args.market_universe:
        try:
            base = apply_market_universe(
                base,
                load_json_object(Path(args.market_universe).expanduser().resolve()),
            )
        except ValueError as exc:
            sys.exit(f"ERROR: {exc}")
    if requested_host:
        host_id = requested_host
    elif len(hosts) == 1:
        host_id = hosts[0]
    else:
        sys.exit(
            "ERROR: --host-id is required when the roster contains multiple hosts: "
            + ", ".join(hosts)
        )
    local_accounts = local_runtime_accounts(accounts, host_id)
    if not local_accounts:
        sys.exit(f"ERROR: roster has no enabled accounts for host {host_id!r}")
    roster_sha = routing_roster_sha256(accounts, runtime_scope)

    if not args.dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Base:   {base_path}")
    print(f"Roster: {roster_path}  ({len(accounts)} global account(s))")
    print(f"Host:   {host_id}  ({len(local_accounts)} local account(s))")
    print(f"Route:  {roster_sha}")
    print(f"Scope:  {runtime_scope or 'legacy'}")
    print(f"Output: {out_dir}")
    print()

    for account in local_accounts:
        idx = account.account_index
        entry = account.generation_entry()
        cfg = _render(
            base,
            entry,
            args.clash_host,
            roster_sha256=roster_sha,
            runtime_scope=runtime_scope,
        )
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
        print(f"\nDone. multi_runner.py will pick up {len(local_accounts)} account(s) on {host_id}.")


if __name__ == "__main__":
    main()
