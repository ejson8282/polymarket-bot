# Read-only shadow validation: 2026-07-13

The existing Python workers remained stopped. Validation copied only their
JSON state files to a temporary local directory. No signer, exchange write API,
cancel endpoint, or order endpoint was contacted.

## Predict.fun

- Desired intents: 7
- Simulated active orders: 9
- Normalized books: 7
- Python/Rust result parity: pass
- Safety result: blocked because the simulated active-order state was stale

The exporter deliberately uses the older of the plan and active-order state
timestamps. A fresh plan cannot make an old order snapshot executable.

## Polymarket

- Engine state files: 2
- Desired orders available in legacy state: 0
- Observed live orders: 4
- Managed orders: 0
- Python/Rust result parity: pass

The state files predate `desired_plan_sig`. All observed orders therefore stay
unmanaged and the shared reconciler emits no cancel or replace action.

## Conclusion

Both production-shaped state formats can be normalized without granting the
Rust core execution authority. True Polymarket plan parity begins only after a
running Python engine writes the new observation fields; that is a separate,
explicitly reviewed step.
