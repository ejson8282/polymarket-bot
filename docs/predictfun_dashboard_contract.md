# Predict.fun Unified Dashboard Contract

Predict.fun should use the same information architecture as the Polymarket
maker without sharing its process, account state, or controls. The business
repository publishes one read-only status document per VPS profile. The
unified Dashboard in `latitude-alpha` owns the presentation.

## Source

Each profile writes:

```text
/home/ubuntu/predictfun-runtime/data/predictfun_mainnet_status.json
```

The document has `schema_version=1`, `project=predictfun`, and an explicit
deployment identity. VPS1 must report only `account_01`; VPS2 must report only
`account_02`. A missing, stale, malformed, or mismatched document must remain
unknown rather than borrowing another host's values.

## Polymarket-style sections

| Dashboard section | Predict status fields | Current truth |
| --- | --- | --- |
| Overview | `deployment`, `health`, `overview` | Read-only dry-run |
| Markets | `markets` | Live REST scan plus WS orderbooks |
| Orders | `desired_orders`, `simulated_active_orders` | Simulated |
| Fills and exits | `recent_actions`, `simulated_positions` | Simulated |
| Scan | market/research summaries | Live public data |
| Risk | `risk_checks`, `health.risk` | Enforced for dry-run planning |
| Settings | reviewed config summary | Read-only |

The UI may reuse the Polymarket navigation and table density, but labels must
say `模拟` for simulated orders, fills, positions, and PnL. It must not present
these values as actual account state.

## Capability gates

Buttons are controlled by `capabilities`, not by visual assumptions:

- `live_order_submit=false`: hide or disable Start/Run Live.
- `live_order_cancel=false`: hide or disable Cancel/Stop-and-cancel.
- `live_balance_read=false`: show account balance as unavailable.
- `live_position_read=false`: do not infer positions from simulated fills.
- `live_fill_stream=false`: do not claim real-time fills or automatic exits.

The initial contract deliberately keeps all five capabilities false. They can
change only after the Mac mini signer exposes reviewed account-scoped routes,
the business adapter reconciles official IDs and on-chain cancellation, and
production tests prove account isolation.

## Health semantics

- `healthy`: runner, signer, WS transport, and risk gate are usable.
- `attention`: the transport remains usable but an isolated market or runner
  detail needs inspection.
- `blocked`: required WS transport or a hard risk check failed.

An isolated malformed market is skipped and reported as attention. A WS
disconnect, stale heartbeat, empty cache, or hard risk failure blocks all new
quotes. The Dashboard should show the affected market reason without dumping
raw JSON or stack traces.

## Security

The status document contains no API key, private key, JWT, bearer token,
webhook, cookie, or raw signer payload. The Dashboard must not read the Mac
mini secret file and must never turn state-only account discovery into a
service-control target.
