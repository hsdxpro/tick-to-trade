# tick-to-trade

A minimal HFT pipeline in Rust and C++. Every stage benchmarked, every latency
decomposed, every custom structure required to beat the standard library.

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

Pinning each stage to its own core cut p50 by 20%: a migrated thread arrives on
a core holding none of its working set. C++ matches: ~100 ns compute, ~400 ns
staged. The ~10 µs between internal and wire is the two kernel network stacks —
the kernel-bypass motivation, priced.

Wire figures are timestamped at the **counterparty**: T0 before the tick leaves
the harness, T1 when the order's bytes arrive back. The engine's own `send`
times never enter it. Windows clock quantizes near 100 ns; `min 0` in raw
output is the instrument's floor, not sub-nanosecond code.

Where the internal time goes, amortized over a million probes. Each probe
carries two ITCH messages, so the book does two updates:

| Stage | cost | share |
|---|---:|---:|
| book (ladder + order map) | 69.2 ns | **71.8%** |
| parse (ITCH framing + fields) | 14.7 ns | 15.3% |
| arbitrate (MoldUDP64 A/B) | 13.2 ns | 13.7% |
| decide + encode | <1 ns | ~0% |
| total | 96.3 ns | 100% |

## Parsers

ns/msg, best of three runs, byte-identical streams in both languages —
generators seeded alike, FNV fingerprints pinned in both test suites.

| Format | Rust | C++ (MSVC) | C++ (GCC 15) |
|---|---:|---:|---:|
| ITCH 5.0 (binary) | **8.0** | 9.3 | 9.4 |
| FIX 4.4 (tag=value, checksum) | 95.5 | 103.8 | **90.1** |
| JSON (schema scanner) | **194.0** | 213.7 | 197.7 |
| JSON via `serde_json` | 331.3 | — | — |

The third column is there because the second one raised a question worth
answering. Under GCC the same C++ source beats Rust on FIX and matches it on
JSON, so the Windows gap is MSVC's backend rather than the code; ITCH is the
one format where LLVM leads on both sides, by about 1.3 ns. The `serde_json`
row is not a criticism of serde — it parses arbitrary JSON where the scanner
parses one schema. That gap is what knowing the schema is worth.

## Custom vs standard library

| Structure | Custom | Standard | Gain |
|---|---:|---:|---:|
| Ladder, isolated (Rust) | 2.7 ms | BTreeMap 30.4 ms | **11×** |
| Order map, isolated (Rust) | 36.5 ms | HashMap 44.2 ms | 1.2× |
| Book blended, Rust | 53.7 ns/ev | 102.8 ns/ev | 1.9× |
| Book blended, C++ | 48.5 ns/ev | std::map 214.5 ns/ev | 4.4× |
| SPSC ring, 20M items | 1.6 ns/item | sync_channel 13.9, crossbeam 27.5 | **8.7×** |

The book rows are the same 992,670 operations with identical final state. The
first ladder was a sorted array; it lost to `BTreeMap` and was deleted. The
module doc keeps that story, because the rule that killed it is the point: a
custom structure that cannot beat the standard library on the real operation
stream has no reason to exist.

The ring's comparisons are not strawmen — crossbeam's `ArrayQueue` pays for
MPMC safety it cannot give up, and `sync_channel` pays for blocking. One-way
hop latency is p50 50 ns, p99 100 ns, measured ping-pong and halved.

## The ladder

One structure serves any instrument, with no range declared in advance:

- **The window follows the market.** A dense array over a slice of the price
  grid, shifted when a price falls outside it. It shifts the minimum that
  admits the price plus a quarter window of hysteresis — centring would discard
  half the existing depth every time, which is what the deep-book test catches.
- **Price to index is a multiply.** `(price - base) / tick` with a runtime tick
  is a hardware divide on the per-message path. A reciprocal computed once
  makes it one multiply and a shift, exact rather than approximate-then-fixed.
  The same multiply detects an off-grid price, which is refused and counted
  rather than silently rounded into its neighbour.
- One unsigned compare handles below-window, above-window, and never-placed.

Tested on a penny-tick equity at $30, a cent-tick perpetual at $100,000, and a
satoshi-tick pair at $0.30 — each walked across dozens of window widths, each
agreeing with a `BTreeMap` reference on every level.

## Feed sequencing and order-entry session

Real feeds arrive as MoldUDP64: sequenced datagrams sent twice down independent
paths. The arbitrator delivers each sequence exactly once, in order, from
whichever line won — and the fast path is **one compare**, because a packet
lost on line A arrives *in sequence* on line B. Only a packet ahead of
expectation means both lines lost the same data; only then is the stash
touched. Verified by 20,000 sequences across two lines at 20% independent loss,
in both languages: every sequence delivered exactly once, in order.

Order entry is a session, not a socket: sequence numbers, heartbeats, and a
retain ring for resend. Sending is stamp, copy 32 bytes, write — no allocation,
no scan, no clock read on the send path. A resend request past what is retained
is refused rather than answered with a hole.

## Receive transports

| Transport | Windows p50 | Linux p50 |
|---|---:|---:|
| blocking `recv` | 8.2 µs | 18.8 µs |
| busy-poll `recv` | **5.9 µs** | **3.4 µs** |
| io_uring (polled CQ) | — | 4.6 µs |

io_uring does **not** beat busy-poll here: its advantage is amortizing syscalls
across a batch, and a one-packet probe has none. AF_XDP and DPDK sit behind the
same `Receiver` trait as documented seams — DPDK written against the poll-mode
API behind a feature no build enables, because a figure from a machine with no
DPDK-bound NIC would be invented.

## Layout

```text
rust/spsc       wait-free SPSC ring       loom-model-checked
rust/feed       ITCH · FIX · JSON · Mold  one event; A/B arbitration
rust/book       L2/L3 maintenance         windowed bitmap ladder + open addressing
rust/pipeline   engine · harness · rxlat  stages, session, affinity, transports
cpp/            protocol-identical twins  MSVC + GCC, TSAN on Linux
```

One event type crosses the system; no stage knows its wire format. Prices are
fixed-point 1e8 throughout — floats never appear.

## Run

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

Wire-to-wire, two terminals — the harness binds the order listener, so it goes
first:

```bash
cargo run --release -p t2t-pipeline --bin harness
```

```bash
cargo run --release -p t2t-pipeline --bin engine
```

C++20 — the newest standard MSVC, GCC and Clang all implement properly, rather
than as a preview mode. Verified on MSVC 2022 and GCC 15:

```bash
cd cpp && cmake -S . -B build && cmake --build build --config Release && ctest --test-dir build -C Release
```

ThreadSanitizer checks the ring's memory orderings. It needs GCC or Clang —
`-DT2T_TSAN=ON` is a no-op under MSVC, which has no `-fsanitize=thread`:

```bash
cd cpp && g++ -std=c++20 -O1 -g -fsanitize=thread -pthread tests/test_spsc.cpp -o t_spsc_tsan && ./t_spsc_tsan
```

## Verification

Parsers are differential against generators that produce bytes and meaning
independently; every prefix of a valid stream must parse to a clean short-read;
corruption is refused, not reinterpreted. Books are compared against
std-collection references — full ladders, order state, unknown-order counts.
The ring's orderings are loom-model-checked and TSAN-checked.

Every checker was mutation-tested: the ladder's window strategy, its reciprocal
rounding, and the ring's memory orderings were each broken on purpose to
confirm a test fails. A benchmark or test that could not tell the difference
was deleted rather than kept as false assurance.

## Not here

No real signal — the strategy is one deterministic rule; the decision belongs
to a deployment, and everything around it is what this measures. No routing, no
risk layer: those live in [exchange-core](https://github.com/hsdxpro/exchange-core).

MIT.
