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
ACTIVE_PROCESS: subprocess.Popen[bytes] | None = None
SHUTTING_DOWN = False


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


def send_process_signal(process: subprocess.Popen[bytes], signum: int) -> None:
    try:
        if os.name == "posix":
            os.killpg(process.pid, signum)
        else:
            process.send_signal(signum)
    except ProcessLookupError:
        pass


def forward_signal(signum: int, _frame: Any) -> None:
    global SHUTTING_DOWN

    if SHUTTING_DOWN and ACTIVE_PROCESS and ACTIVE_PROCESS.poll() is None:
        send_process_signal(ACTIVE_PROCESS, signal.SIGKILL)
        return

    SHUTTING_DOWN = True
    if ACTIVE_PROCESS and ACTIVE_PROCESS.poll() is None:
        send_process_signal(ACTIVE_PROCESS, signum)


def wait_for_exit(process: subprocess.Popen[bytes], timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return True
        time.sleep(0.05)

    return process.poll() is not None


def stop_process(process: subprocess.Popen[bytes]) -> None:
    stops = (
        lambda: send_process_signal(process, signal.SIGINT),
        lambda: send_process_signal(process, signal.SIGTERM),
        lambda: send_process_signal(process, signal.SIGKILL),
    )

    for stop in stops:
        if process.poll() is not None:
            return

        stop()
        if wait_for_exit(process, 3):
            return


def run_until_timeout(
    command: list[str],
    timeout: float,
    cwd: Path,
    stdout: int | Any | None,
) -> int:
    global ACTIVE_PROCESS

    process = subprocess.Popen(command, cwd=cwd, stdout=stdout, start_new_session=True)
    ACTIVE_PROCESS = process

    try:
        deadline = time.monotonic() + timeout
        while True:
            code = process.poll()
            if code is not None:
                return code

            if SHUTTING_DOWN:
                stop_process(process)
                return 130

            if time.monotonic() >= deadline:
                stop_process(process)
                return 0

            time.sleep(0.1)
    finally:
        ACTIVE_PROCESS = None


def main() -> int:
    signal.signal(signal.SIGINT, forward_signal)
    signal.signal(signal.SIGTERM, forward_signal)

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
        default=100,
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
            if SHUTTING_DOWN:
                return 130

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
            if SHUTTING_DOWN:
                return 130

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
