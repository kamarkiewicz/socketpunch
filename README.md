<p align="center">
  <img src="assets/socketpunch-logo.svg" alt="socketpunch logo" width="100%">
</p>

**Redundant websocket ingestion for feeds where the first copy wins.**

`socketpunch` opens parallel websocket connections to the same endpoint,
subscribes each one the same way, races identical feed messages against each
other, emits the first copy that arrives, and suppresses the duplicates that
follow.

It is not a parser, trading bot, exchange SDK, or storage engine. It is a
latency-oriented websocket primitive: many sockets in, first unique payload out.

```text
              websocket A ──┐
              websocket B ──┼── first arrival ──► stdout
              websocket C ──┘          │
                                       ▼
                              duplicate copies drop
```

## The Problem

A single websocket connection is a single network path, a single kernel socket,
a single TLS stream, a single server-side session, and a single chance to be
early.

For realtime market data and tick feeds, that is fragile:

- one connection may receive a message later than another connection;
- one server-side session may stall or batch differently;
- one TCP stream may hit transient congestion;
- one feed connection may reconnect at the exact wrong moment;
- multiple redundant connections create duplicate payloads you still need to
  collapse quickly.

The interesting part is not simply receiving more messages. The interesting
part is receiving the **earliest copy of each unique message** without making
deduplication, formatting, or output the new latency bottleneck.

## The Solution

`socketpunch` uses redundant websocket fanout plus cheap shared deduplication.
Each worker connects to the same URL, sends the same optional subscription
payload, then reads frames independently. When a message arrives, the worker
hashes the payload and checks a shared atomic dedup table:

- first observed payload hash passes through;
- later copies are dropped;
- output receives one line per unique payload.

That makes the feed behave like a latency race. If connection 7 sees a tick
before connection 2, connection 7 wins. The duplicate from connection 2 is
discarded when it lands.

```text
remote feed
    │
    ├─ connection 1 ─┐
    ├─ connection 2 ─┼─► hash payload ─► atomic dedup ─► raw output
    ├─ connection 3 ─┤
    └─ connection N ─┘
```

## Latency Model

`socketpunch` cannot make the remote exchange publish faster, and it cannot
beat physics. What it can do is reduce your dependence on any one websocket
session being the fastest session.

With `N` redundant connections, every message gets `N` chances to arrive early.
The output path takes the first unique payload observed by any worker. In
practice, this helps when latency variation comes from:

- server fanout jitter;
- per-connection buffering;
- network path variance;
- reconnect gaps;
- runtime scheduling noise.

The tradeoff is extra connections, extra inbound duplicate traffic, and a small
hash/dedup cost per frame. `socketpunch` is built so that cost stays tiny:

- no JSON parsing in the Rust binary;
- no per-message stdout lock;
- no per-message UTF-8 formatting pass;
- no unbounded dedup map growth;
- no vendor-specific branching on the hot path.

## Features

- **Parallel websocket racing** with `--count`.
- **First-copy-wins emission** across redundant workers.
- **Universal subscription payloads** with `--subscribe-payload`.
- **Generic text heartbeats** with `--heartbeat-text` and `--heartbeat-secs`.
- **Shared lock-free lossy deduplication** tuned for high-throughput ticks.
- **Raw byte output** to avoid parse and formatting latency.
- **Buffered batched stdout** to move output cost away from socket readers.
- **Release builds tuned for throughput** with LTO and one codegen unit.
- **Zero platform-specific behavior in the binary.**

## Install

```sh
cargo build --release
```

The binary lands at:

```sh
target/release/socketpunch
```

## Quick Start

Race 8 websocket sessions and print first-arriving unique messages:

```sh
target/release/socketpunch \
  --url wss://example.com/ws \
  --count 8
```

Race 16 subscribed sessions:

```sh
target/release/socketpunch \
  --url wss://example.com/ws \
  --subscribe-payload '{"type":"subscribe","channel":"ticks"}' \
  --count 16
```

Keep each connection alive with a text heartbeat:

```sh
target/release/socketpunch \
  --url wss://example.com/ws \
  --subscribe-payload '{"channel":"market"}' \
  --heartbeat-text PING \
  --heartbeat-secs 10
```

## CLI

```text
Usage: socketpunch [OPTIONS] --url <URL>

Options:
  -u, --url <URL>
          WebSocket URL to connect to
  -c, --count <COUNT>
          Number of concurrent connections to open [default: 10]
  -s, --subscribe-payload <SUBSCRIBE_PAYLOAD>
          Optional subscription payload to send upon connection
      --heartbeat-text <HEARTBEAT_TEXT>
          Text heartbeat payload to send periodically, e.g. PING
      --heartbeat-secs <HEARTBEAT_SECS>
          Heartbeat interval in seconds [default: 10]
      --dedup-power <DEDUP_POWER>
          Dedup cache size as a power of two [default: 16]
```

## Output

`socketpunch` writes each unique websocket message as raw bytes followed by
`\n`. That is intentional.

It means JSON feeds become newline-delimited JSON. It also means binary payloads
are not prettified, decoded, or replaced with a placeholder. If the feed sends
bytes, you get bytes.

```sh
target/release/socketpunch ... > ticks.ndjson
```

## Dedup Model

Deduplication is built for speed, not archival certainty.

Each message is hashed and checked against a fixed-size atomic slot table. The
slot table is shared by all websocket workers. A matching hash in the selected
slot is treated as a duplicate; a mismatch overwrites the slot and passes
through.

This makes dedup:

- lock-free;
- bounded memory;
- extremely cheap per message;
- lossy under collisions or very high churn.

Lossy is a deliberate choice. The goal is not to prove a permanent historical
set of every payload ever seen. The goal is to suppress near-term duplicate
copies from redundant websocket sessions with minimum latency.

Tune the table size with:

```sh
--dedup-power 20
```

That gives `2^20` slots. Larger values reduce collision churn and use more
memory.

## Polymarket BTC 5m Test Harness

The binary stays universal. The repo includes a separate Python harness for
testing against Polymarket’s rolling BTC 5-minute market.

```sh
scripts/polymarket_btc_5m_ticks.py \
  --duration-secs 300 \
  --count 100 \
  --output /tmp/btc-5m-ticks.ndjson
```

The script:

- discovers the current `btc-updown-5m-{timestamp}` event;
- extracts the CLOB token IDs;
- launches `socketpunch` with a generic websocket URL, payload, and heartbeat;
- rolls into the next 5-minute market if the run crosses a boundary;
- forwards `Ctrl+C` to the child process group.

Stream directly to the terminal:

```sh
scripts/polymarket_btc_5m_ticks.py --duration-secs 300
```

## Performance Notes

The hot path is intentionally short:

1. read websocket frame;
2. hash payload;
3. check/update atomic dedup slot;
4. move payload to the output channel when possible;
5. batch-drain and write raw bytes to stdout.

The important architectural choice is that websocket workers do not parse,
normalize, pretty-print, or persist messages. Those jobs belong downstream.
`socketpunch` is the thin intake layer whose job is to get the earliest unique
payload out of the socket race.

That is also why the binary does not know what exchange, chain, venue,
protocol, or data provider you are testing. Vendor-specific discovery belongs
in wrappers. The Rust binary remains a sharp universal websocket primitive.

For best results:

- use `cargo build --release`;
- redirect output to a file or pipe built for volume;
- increase `--dedup-power` for very hot feeds;
- keep `--count` high enough for redundancy, not so high that the remote feed
  throttles you;
- avoid terminal rendering for serious captures.
