"""Loopback-only CONNECT proxy for the isolated aggressive LP runtime."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import suppress
import ipaddress
import json
from pathlib import Path
import socket
from typing import Iterable

try:
    from .account_roster import local_runtime_accounts, parse_runtime_roster
except ImportError:  # pragma: no cover - direct service execution
    from account_roster import local_runtime_accounts, parse_runtime_roster


MAX_HEADER_BYTES = 16 * 1024
CONNECT_TIMEOUT_SEC = 10.0
HEADER_TIMEOUT_SEC = 5.0


def proxy_ports(roster: object, host_id: str) -> tuple[int, ...]:
    accounts = local_runtime_accounts(parse_runtime_roster(roster), host_id)
    ports = tuple(sorted({account.clash_port for account in accounts}))
    if not ports:
        raise ValueError(f"roster has no enabled account for {host_id!r}")
    return ports


def parse_connect_target(request_line: bytes) -> tuple[str, int]:
    try:
        method, target, version = request_line.decode("ascii").split()
    except (UnicodeDecodeError, ValueError) as exc:
        raise ValueError("malformed proxy request line") from exc
    if method.upper() != "CONNECT" or not version.startswith("HTTP/"):
        raise ValueError("only HTTP CONNECT is supported")
    if target.count(":") != 1:
        raise ValueError("CONNECT target must be host:port")
    host, raw_port = target.rsplit(":", 1)
    host = host.strip().rstrip(".").lower()
    if not host or any(char.isspace() for char in host):
        raise ValueError("CONNECT host is invalid")
    try:
        port = int(raw_port)
    except ValueError as exc:
        raise ValueError("CONNECT port is invalid") from exc
    if port != 443:
        raise ValueError("only HTTPS port 443 is allowed")
    return host, port


async def public_addresses(host: str, port: int) -> tuple[str, ...]:
    loop = asyncio.get_running_loop()
    rows = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    addresses = []
    for _family, _socktype, _proto, _canonname, sockaddr in rows:
        address = str(sockaddr[0])
        if ipaddress.ip_address(address).is_global:
            addresses.append(address)
    unique = tuple(dict.fromkeys(addresses))
    if not unique:
        raise ValueError("CONNECT destination has no public address")
    return unique


async def open_public_connection(host: str, port: int) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    last_error: OSError | None = None
    for address in await public_addresses(host, port):
        try:
            return await asyncio.wait_for(
                asyncio.open_connection(address, port),
                timeout=CONNECT_TIMEOUT_SEC,
            )
        except OSError as exc:
            last_error = exc
    raise OSError(f"unable to connect to public destination {host}:{port}") from last_error


async def relay(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(64 * 1024):
            writer.write(data)
            await writer.drain()
    finally:
        writer.close()
        with suppress(Exception):
            await writer.wait_closed()


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    upstream_writer: asyncio.StreamWriter | None = None
    try:
        header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), HEADER_TIMEOUT_SEC)
        if len(header) > MAX_HEADER_BYTES:
            raise ValueError("proxy request headers are too large")
        host, port = parse_connect_target(header.split(b"\r\n", 1)[0])
        upstream_reader, upstream_writer = await open_public_connection(host, port)
        writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        await writer.drain()
        await asyncio.gather(
            relay(reader, upstream_writer),
            relay(upstream_reader, writer),
        )
    except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError, OSError, ValueError):
        if not writer.is_closing():
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n")
            with suppress(Exception):
                await writer.drain()
    finally:
        if upstream_writer is not None and not upstream_writer.is_closing():
            upstream_writer.close()
        if not writer.is_closing():
            writer.close()
        with suppress(Exception):
            await writer.wait_closed()


async def serve(ports: Iterable[int]) -> None:
    servers = [
        await asyncio.start_server(handle_client, "127.0.0.1", port)
        for port in ports
    ]
    print("aggressive proxy listening on " + ", ".join(f"127.0.0.1:{port}" for port in ports), flush=True)
    await asyncio.gather(*(server.serve_forever() for server in servers))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the isolated aggressive LP proxy")
    parser.add_argument("--roster", required=True)
    parser.add_argument("--host-id", required=True)
    args = parser.parse_args()
    roster = json.loads(Path(args.roster).read_text(encoding="utf-8"))
    try:
        asyncio.run(serve(proxy_ports(roster, args.host_id)))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
