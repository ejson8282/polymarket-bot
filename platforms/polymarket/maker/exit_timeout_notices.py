"""Durable, account-scoped at-most-once attempts for exit timeout notices.

Claims precede Discord submission. A crash or transport failure after a claim
can lose that notice, but cannot produce a repeated timeout alert. This store
is notification metadata only; it never changes an order or trading state.
"""

from pathlib import Path
import sqlite3


def claim_exit_timeout_notice(
    path: Path, account_index: int, token_id: str, order_id: str
) -> bool:
    if type(account_index) is not int or account_index < 0:
        raise ValueError("invalid notice account")
    if not all(isinstance(value, str) and value.strip() for value in (token_id, order_id)):
        raise ValueError("notice requires token and order identity")
    connection = sqlite3.connect(str(path), timeout=0.25)
    try:
        connection.execute("PRAGMA synchronous=FULL")
        with connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS exit_timeout_notices ("
                "account_index INTEGER NOT NULL, token_id TEXT NOT NULL, "
                "order_id TEXT NOT NULL, "
                "PRIMARY KEY (account_index, token_id, order_id))"
            )
            inserted = connection.execute(
                "INSERT OR IGNORE INTO exit_timeout_notices VALUES (?, ?, ?)",
                (account_index, token_id, order_id),
            ).rowcount
        return inserted == 1
    finally:
        connection.close()
