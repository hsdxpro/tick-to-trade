# tick-to-trade

A minimal HFT pipeline in Rust and C++. Every stage benchmarked, every custom
structure required to beat the standard library.

```text
UDP ticks → [parse + book + BBO] →ring→ [strategy] →ring→ [gateway] → TCP orders
```

## Tick-to-trade

| Path | min | p50 | p99 |
|---|---:|---:|---:|
| Compute only (parse + book + decide + encode) | <100 ns | **~100 ns** | 300 ns |
| Production layout (3 threads, SPSC rings) | 200 ns | **~400 ns** | 700 ns |
| Wire-to-wire, unpinned | 9.7 µs | 12.5 µs | 25.1 µs |
| Wire-to-wire, **pinned** (UDP in, TCP out, loopback) | 9.3 µs | **10.0 µs** | 19.3 µs |

- Pinning cut p50 by 20%. A migrated thread lands on a cold core.
- C++ matches: ~100 ns compute, ~400 ns staged.
- The ~10 µs internal-to-wire gap is the two kernel network stacks. That is the
  kernel-bypass motivation, priced.
- T0/T1 are stamped at the **counterparty**, never by the engine about itself.
- Windows clock quantizes near 100 ns. `min 0` is the instrument's floor.

## Where the time goes

Amortized over a million probes. Each probe carries two ITCH messages, so the
book does two updates.

| Stage | cost | share |
|---|---:|---:|
| book (ladder + order map) | 69.2 ns | **71.8%** |
| parse (ITCH framing + fields) | 14.7 ns | 15.3% |
| arbitrate (MoldUDP64 A/B) | 13.2 ns | 13.7% |
| decide + encode | <1 ns | ~0% |
| total | 96.3 ns | 100% |

## Parsers

ns/msg, best of three. Byte-identical streams in both languages: same seed,
FNV fingerprints pinned in both test suites.

| Format | Rust | C++ (MSVC) | C++ (GCC 15) |
|---|---:|---:|---:|
| ITCH 5.0 (binary) | **8.0** | 9.3 | 9.4 |
| FIX 4.4 (tag=value, checksum) | 95.5 | 103.8 | **90.1** |
| JSON (schema scanner) | **194.0** | 213.7 | 197.7 |
| JSON via `serde_json` | 331.3 | — | — |

- The GCC column answers the MSVC column: same C++ source beats Rust on FIX,
  matches on JSON. The Windows gap is MSVC's backend, not the code.
- ITCH is the one format where LLVM leads on both sides, by ~1.3 ns.
- `serde_json` parses arbitrary JSON; the scanner parses one schema. That gap
  is what knowing the schema is worth.

## Custom vs standard library

| Structure | Custom | Standard | Gain |
|---|---:|---:|---:|
| Ladder, isolated (Rust) | 2.7 ms | BTreeMap 30.4 ms | **11×** |
| Order map, isolated (Rust) | 36.5 ms | HashMap 44.2 ms | 1.2× |
| Book blended, Rust | 53.7 ns/ev | 102.8 ns/ev | 1.9× |
| Book blended, C++ | 48.5 ns/ev | std::map 214.5 ns/ev | 4.4× |
| SPSC ring, 20M items | 1.6 ns/item | sync_channel 13.9, crossbeam 27.5 | **8.7×** |

- Book rows: same 992,670 operations, identical final state.
- The first ladder was a sorted array. It lost to `BTreeMap` and was deleted —
  the rule that killed it is the point.
- Not strawmen: crossbeam's `ArrayQueue` pays for MPMC, `sync_channel` for
  blocking. Ring one-way hop is p50 50 ns, p99 100 ns (ping-pong ÷ 2).

## The ladder

One structure, any instrument, no range declared in advance.

- **Window follows the market.** Dense array over a slice of the price grid.
  Shifts the minimum that admits the price plus a quarter window of hysteresis.
  Centring would discard half the depth — the deep-book test catches that.
- **Price to index is a multiply.** A runtime tick makes `(price - base) / tick`
  a hardware divide per message. A reciprocal computed once replaces it with a
  multiply and a shift, exact rather than approximate-then-fixed.
- Off-grid prices are refused and counted, never rounded into a neighbour.
- One unsigned compare covers below-window, above-window and never-placed.

Tested on a penny-tick equity at $30, a cent-tick perpetual at $100,000, and a
satoshi-tick pair at $0.30 — each walked across dozens of window widths.

## Feed sequencing and order entry

MoldUDP64: sequenced datagrams sent twice down independent paths.

- Each sequence delivered exactly once, in order, from whichever line won.
- Fast path is **one compare** — a packet lost on line A arrives *in sequence*
  on line B. Only a packet ahead of expectation touches the stash.
- Verified: 20,000 sequences, two lines, 20% independent loss, both languages.

Order entry is a session, not a socket.

- Sequence numbers, heartbeats, retain ring for resend.
- Send is stamp, copy 32 bytes, write. No allocation, no scan, no clock read.
- A resend past the retain window is refused, not answered with a hole.

## Receive transports

| Transport | Windows p50 | Linux p50 |
|---|---:|---:|
| blocking `recv` | 8.2 µs | 18.8 µs |
| busy-poll `recv` | **5.9 µs** | **3.4 µs** |
| io_uring (polled CQ) | — | 4.6 µs |

io_uring does **not** beat busy-poll here — it amortizes syscalls across a
batch, and a one-packet probe has none. AF_XDP and DPDK sit behind the same
`Receiver` trait as documented seams; DPDK is behind a feature no build
enables, because a figure from a machine with no DPDK-bound NIC is invented.

## Layout

```text
rust/spsc       wait-free SPSC ring       loom-model-checked
rust/feed       ITCH · FIX · JSON · Mold  one event; A/B arbitration
rust/book       L2/L3 maintenance         windowed bitmap ladder + open addressing
rust/pipeline   engine · harness · rxlat  stages, session, affinity, transports
cpp/            protocol-identical twins  MSVC + GCC, TSAN on Linux
```

One event type crosses the system; no stage knows its wire format. Prices are
fixed-point 1e8 — floats never appear.

## Run

Rust 2024 edition.

```bash
cd rust
cargo test --release                    # differential tests
cargo bench -p t2t-spsc                 # ring throughput + hop latency
cargo bench -p t2t-feed                 # parser table
cargo bench -p t2t-book                 # book tables
cargo bench -p t2t-pipeline             # internal tick-to-trade + attribution
cargo run --release --bin rxlat         # transport table
RUSTFLAGS="--cfg loom" cargo test -p t2t-spsc --release   # model checking
```

Wire-to-wire, two terminals. The harness binds the listener, so it goes first.

```bash
cargo run --release -p t2t-pipeline --bin harness
```

```bash
cargo run --release -p t2t-pipeline --bin engine
```

C++20 — the newest standard MSVC, GCC and Clang all implement properly rather
than as a preview. Verified on MSVC 2022 and GCC 15.

```bash
cd cpp && cmake -S . -B build && cmake --build build --config Release && ctest --test-dir build -C Release
```

TSAN checks the ring's memory orderings. Needs GCC or Clang; `-DT2T_TSAN=ON` is
a no-op under MSVC.

```bash
cd cpp && g++ -std=c++20 -O1 -g -fsanitize=thread -pthread tests/test_spsc.cpp -o t_spsc_tsan && ./t_spsc_tsan
```

## Verification

- Parsers differential against generators producing bytes and meaning
  independently. Every prefix of a valid stream parses to a clean short-read.
  Corruption is refused, not reinterpreted.
- Books compared against std-collection references: full ladders, order state,
  unknown-order counts.
- Ring orderings loom-model-checked and TSAN-checked.
- Every checker mutation-tested — the ladder's window strategy, its reciprocal
  rounding, and the ring's orderings were each broken on purpose to confirm a
  test fails. Benchmarks that could not discriminate were deleted.

## Not here

No real signal — the strategy is one deterministic rule. No routing, no risk
layer; those live in
[exchange-core](https://github.com/hsdxpro/exchange-core).

MIT.
