# tick-to-trade

[![ci](https://github.com/hsdxpro/tick-to-trade/actions/workflows/ci.yml/badge.svg)](https://github.com/hsdxpro/tick-to-trade/actions/workflows/ci.yml)

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

### Closed loop against open loop

Those rows send a probe, wait for its order, send the next — one in flight, so
they measure service time with no queue in front of it. A harness that does
only that flatters the system it measures: when the pipeline stalls it politely
stops sending, and the stall never lands in a sample. `--rate` holds a schedule
instead and charges every probe from when it was **due**.

| Mode | p50 | p99 | p99.9 | max |
|---|---:|---:|---:|---:|
| Closed loop, 1 in flight | 10.4 µs | 37.5 µs | 300.9 µs | 1.68 ms |
| Open loop, 10,000/sec | 13.4 µs | 916.6 µs | 1.70 ms | 2.41 ms |
| Open loop, 50,000/sec | 10.3 µs | **1.27 ms** | 2.61 ms | 2.86 ms |

- **The median barely moves; the tail goes 34× at p99.** Service time was never
  the question — queueing was, and closed loop cannot see it.
- **This tail is the host, not the pipeline.** Closed loop already shows a
  1.68 ms max on an idle machine, so millisecond hiccups exist before any load
  is applied; open loop merely makes one stall punish everything queued behind
  it. A trading host isolates cores, pins interrupts and disables C-states.
  This is a Windows desktop over loopback with the generator sharing its CPUs.
- What the repository can claim is the **method**: measured from the schedule,
  not from departure. What it cannot claim from this machine is a tail figure.

## Where the time goes

Amortized over a million probes. Each probe carries two ITCH messages, so the
book does two updates.

| Stage | cost | share |
|---|---:|---:|
| book (ladder + order map) | 57.0 ns | **66.2%** |
| parse (ITCH framing + fields) | 14.8 ns | 17.2% |
| arbitrate (MoldUDP64 A/B) | 12.8 ns | 14.9% |
| decide + encode | 1.4 ns | 1.7% |
| total | **86.0 ns** | 100% |

## Parsers

ns/msg, best of three. Byte-identical streams in both languages: same seed,
FNV fingerprints pinned in both test suites.

| Format | Rust | C++ (MSVC) | C++ (GCC 15) |
|---|---:|---:|---:|
| ITCH 5.0 (binary) | **8.4** | 9.3 | 9.4 |
| FIX 4.4 (tag=value, checksum) | 105.1 | 103.8 | **90.1** |
| JSON (schema scanner) | **212.3** | 213.7 | 197.7 |
| JSON via `serde_json` | 368.2 | — | — |

Every accumulator is bounded against a digit run no integer can hold, and the
bound costs nothing: the digits wrap deliberately and the length is checked
once per field. Testing it per digit was the obvious way and measured **18%**
of the FIX parser, which is the whole reason the bound sits where it does.

- The GCC column answers the MSVC column: same C++ source beats Rust on FIX
  and JSON. The Windows gap is MSVC's backend, not the code.
- ITCH is the one format where LLVM leads on both sides, by ~1 ns.
- `serde_json` parses arbitrary JSON; the scanner parses one schema. That gap
  is what knowing the schema is worth.

## Custom vs standard library

| Structure | Custom | Standard | Gain |
|---|---:|---:|---:|
| Ladder, isolated (Rust) | 2.3 ms | BTreeMap 27.9 ms | **12×** |
| Order map, isolated (Rust) | 37.1 ms | HashMap 46.0 ms | 1.2× |
| Book blended, Rust | 39.9 ns/ev | 90.7 ns/ev | 2.3× |
| Book blended, C++ | 53.9 ns/ev | std::map 239.4 ns/ev | 4.4× |
| SPSC ring, 20M items | 1.6 ns/item | sync_channel 13.9, crossbeam 27.5 | **8.7×** |

- The order map's key and value arrays were separate allocations, so a
  successful lookup paid two cache misses. Interleaving them into one array
  cut the book stage 17% and the whole internal path from 96.3 ns to 86.0.
- Book rows: same 992,670 operations, identical final state. The order-map row
  measures `reduce` -- take quantity off, drop the order if that finished it --
  against the same thing spelled out on a `HashMap`, because serving one
  execution is the question, not what a single method costs alone.
- The first ladder was a sorted array. It lost to `BTreeMap` and was deleted —
  the rule that killed it is the point.
- Not strawmen: crossbeam's `ArrayQueue` pays for MPMC, `sync_channel` for
  blocking. Ring one-way hop is p50 50 ns, p99 100 ns (ping-pong ÷ 2).

## The ladder

One structure, any instrument, no range declared in advance.

- **Window follows the market.** Dense array over a slice of the price grid.
  Shifts the minimum that admits the price plus a quarter window of hysteresis.
  Centring would discard half the depth — the deep-book test catches that.
- **A shift is a memmove.** Every level moves by the same index delta, so the
  carry is one `copy_within` plus a bitmap shift, not a per-level replay:
  **9,389 → 428 ns** for a 2,500-level book, no allocation, no scratch.
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
| blocking `recv` | 8.2 µs | 17.4 µs |
| busy-poll `recv` | **5.9 µs** | **2.8 µs** |
| io_uring (polled CQ) | — | 4.0 µs |

Linux column measured under WSL2, one-way, ping-pong halved over 50,000 rounds.

io_uring does **not** beat busy-poll here — it amortizes syscalls across a
batch, and a one-packet probe has none. AF_XDP and DPDK sit behind the same
`Receiver` trait as documented seams; DPDK is behind a feature no build
enables, because a figure from a machine with no DPDK-bound NIC is invented.

## Layout

```text
rust/spsc       wait-free SPSC ring       loom-model-checked
rust/feed       ITCH · FIX · JSON · Mold  one event; A/B arbitration; fuzzed
rust/book       L2/L3 maintenance         windowed bitmap ladder + open addressing
rust/pipeline   engine · harness · rxlat  stages, session, affinity, transports
cpp/            protocol-identical twins  MSVC + GCC, TSAN on Linux
```

One event type crosses the system; no stage knows its wire format. Prices are
fixed-point 1e8 — floats never appear. The deployable engine binary is the
Rust one; the C++ side ships the same stages, parsers and structures, benched
identically, without the Mold/session wiring.

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
- Parsers fuzzed in both languages: 90,000 corrupted and arbitrary streams a
  run, seeded so a failure reproduces. Rust runs it with overflow checks on,
  C++ under ASan and UBSan. It found an unbounded accumulator that four
  passes of reading had missed.
- Books compared against std-collection references: full ladders, order state,
  unknown-order counts.
- Ring orderings loom-model-checked and TSAN-checked.
- Both C++ suites run clean under ASan and UBSan (GCC, Linux).
- Every one of the above runs on push, on Linux and Windows: format,
  clippy with warnings denied, the suite in release, the suite again in
  debug so overflow traps are armed, loom over the ring's interleavings,
  MSVC and GCC builds, and ASan/UBSan/TSAN. The badge is that workflow.
- Every checker mutation-tested — the ladder's window strategy, its reciprocal
  rounding, the shift's carry direction and eviction ranges, and the ring's
  orderings were each broken on purpose to confirm a test fails, in both
  languages. Benchmarks that could not discriminate were deleted.

## Not here

No real signal — the strategy is one deterministic rule. No routing, no risk
layer; those live in
[exchange-core](https://github.com/hsdxpro/exchange-core).

MIT.
