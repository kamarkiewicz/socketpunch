#!/usr/bin/env python3
"""
Run socketpunch against the rolling Polymarket BTC 5-minute Up/Down market.

This stays intentionally outside the socketpunch binary: Polymarket-specific
market discovery happens here, and the Rust tool only sees a generic websocket
URL, subscription payload, and heartbeat.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_EVENT_BY_SLUG = "https://gamma-api.polymarket.com/events/slug/{slug}"
WINDOW_SECS = 300


def log(message: str) -> None:
    print(f"[polymarket-btc-5m] {message}", file=sys.stderr, flush=True)


def request_json(url: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "socketpunch-polymarket-test/0.1",
        },
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def parse_jsonish(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def token_ids_from_event(event: dict[str, Any]) -> list[str]:
    markets = event.get("markets") or []
    if not markets:
        raise ValueError("event has no markets")

    token_ids = parse_jsonish(markets[0].get("clobTokenIds"))
    if not isinstance(token_ids, list) or not token_ids:
        raise ValueError("market has no clobTokenIds")

    return [str(token_id) for token_id in token_ids]


def find_btc_5m_event(now: int) -> tuple[int, dict[str, Any]]:
    current_window = now - (now % WINDOW_SECS)

    for offset in (0, 1, -1, 2, -2):
        window_start = current_window + offset * WINDOW_SECS
        slug = f"btc-updown-5m-{window_start}"
        url = GAMMA_EVENT_BY_SLUG.format(slug=slug)

        try:
            event = request_json(url)
            token_ids_from_event(event)
            return window_start, event
        except urllib.error.HTTPError as exc:
            if exc.code not in (403, 404):
                log(f"{slug}: HTTP {exc.code}")
        except Exception as exc:
            log(f"{slug}: {exc}")

    raise RuntimeError("could not find a nearby BTC 5-minute Polymarket event")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_socketpunch_command(root: Path) -> list[str]:
    release_bin = root / "target" / "release" / "socketpunch"
    debug_bin = root / "target" / "debug" / "socketpunch"

    if release_bin.exists():
        return [str(release_bin)]
    if debug_bin.exists():
        return [str(debug_bin)]
    return ["cargo", "run", "--release", "--"]


def socketpunch_command(args: argparse.Namespace, root: Path) -> list[str]:
    command = args.socketpunch or default_socketpunch_command(root)
    if isinstance(command, str):
        command = [command]

    if args.build and command[:1] != ["cargo"]:
        subprocess.run(["cargo", "build", "--release"], cwd=root, check=True)
        command = [str(root / "target" / "release" / "socketpunch")]

    return command


def stop_process(process: subprocess.Popen[bytes]) -> None:
    for stop in (lambda: process.send_signal(signal.SIGINT), process.terminate, process.kill):
        if process.poll() is not None:
            return

        stop()
        try:
            process.wait(timeout=3)
            return
        except subprocess.TimeoutExpired:
            pass


def run_until_timeout(
    command: list[str],
    timeout: float,
    cwd: Path,
    stdout: int | Any | None,
) -> int:
    process = subprocess.Popen(command, cwd=cwd, stdout=stdout)
    try:
        return process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        stop_process(process)
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Subscribe socketpunch to rolling Polymarket BTC 5-minute market ticks."
    )
    parser.add_argument(
        "--duration-secs",
        type=int,
        default=300,
        help="total wall-clock runtime, default 300 seconds",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="socketpunch websocket connection count",
    )
    parser.add_argument(
        "--socketpunch",
        nargs="+",
        help="socketpunch command to run, default target/release, target/debug, then cargo run --release",
    )
    parser.add_argument(
        "--build",
        action="store_true",
        help="build target/release/socketpunch before running",
    )
    parser.add_argument(
        "--custom-features",
        action="store_true",
        help="request Polymarket custom market events in addition to regular updates",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="write socketpunch ticks to this file instead of stdout",
    )
    args = parser.parse_args()

    root = repo_root()
    deadline = time.monotonic() + args.duration_secs
    base_command = socketpunch_command(args, root)
    output = args.output.open("ab") if args.output else None

    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return 0

            now = int(time.time())
            window_start, event = find_btc_5m_event(now)
            token_ids = token_ids_from_event(event)
            title = event.get("title") or event.get("slug") or f"btc-updown-5m-{window_start}"
            run_for = min(remaining, max(1, window_start + WINDOW_SECS - now + 3))

            payload = json.dumps(
                {
                    "assets_ids": token_ids,
                    "type": "market",
                    "custom_feature_enabled": bool(args.custom_features),
                },
                separators=(",", ":"),
            )

            command = [
                *base_command,
                "--url",
                WS_URL,
                "--subscribe-payload",
                payload,
                "--heartbeat-text",
                "PING",
                "--heartbeat-secs",
                "10",
                "--count",
                str(args.count),
            ]

            log(f"{title}")
            log(f"token ids: {', '.join(token_ids)}")
            if args.output:
                log(f"writing ticks to {args.output}")
            log(f"running socketpunch for {run_for:.1f}s")

            code = run_until_timeout(command, run_for, root, output)
            if code != 0:
                log(f"socketpunch exited with code {code}")
    finally:
        if output:
            output.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        os.write(2, b"\n")
        raise SystemExit(130)
