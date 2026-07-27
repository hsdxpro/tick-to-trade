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
| Production layout (3 threads, SPSC rings) | 200 ns | **~300 ns** | 600 ns |
| Wire-to-wire (UDP in, TCP out, loopback) | 9.7 µs | **12.5 µs** | 25.1 µs |

C++ matches: ~100 ns compute, ~400 ns staged, **11.9 µs** wire-to-wire through
the same Rust harness. The ~12 µs gap between internal and wire is the two
kernel network stacks — the kernel-bypass motivation, priced.

Wire figures are timestamped at the **counterparty**: T0 before the tick leaves
the harness, T1 when the order's bytes arrive back. The engine's own `send`
times never enter it. Windows clock quantizes near 100 ns; `min 0` in raw
output is the instrument's floor, not sub-nanosecond code.

## Components

| Component | Rust | C++ | Verified by |
|---|---:|---:|---|
| SPSC ring, throughput | 1.8 ns/item | 2.1 ns/item | loom / TSAN |
| SPSC ring, one-way hop | ~50 ns | — | ping-pong ÷ 2 |
| ITCH 5.0 parse | 9.0 ns/msg | 10.0 ns/msg | differential vs generator |
| FIX 4.4 parse (checksum) | 110 ns/msg | 121 ns/msg | + every-prefix truncation |
| JSON parse (schema scanner) | 223 ns/msg | 293 ns/msg | + corruption refusal |
| JSON via serde_json | 393 ns/msg | — | the specialization gap |
| Book maintain (L2+L3) | 51.4 ns/ev | 52.7 ns/ev | differential vs std |

Both languages parse byte-identical streams — generators seeded alike, FNV
fingerprints pinned in both test suites.

## Custom vs standard library

| Structure | Custom | Standard | Gain |
|---|---:|---:|---:|
| Ladder (banded array + occupancy bitmap) | 2.7 ms | BTreeMap 30.7 ms | **11×** |
| Order map (open addressing, backward-shift) | 34.9 ms | HashMap 42.7 ms | 1.2× |
| Book blended, Rust | 51.4 ns/ev | 102.2 ns/ev | 2.0× |
| Book blended, C++ | 52.7 ns/ev | std::map 280.2 ns/ev | 5.3× |

Same 992,670 operations, identical final state. The first ladder was a sorted
array; it lost to `BTreeMap` and was replaced. The module doc keeps that story.

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
rust/feed       ITCH · FIX · JSON         one normalized fixed-point event
rust/book       L2/L3 maintenance         banded bitmap ladder + open addressing
rust/pipeline   engine · harness · rxlat  the measured pipeline
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
cargo bench -p t2t-pipeline             # internal tick-to-trade
cargo run --release --bin rxlat         # transport table
RUSTFLAGS="--cfg loom" cargo test -p t2t-spsc --release   # model checking
```

Wire-to-wire, two terminals:

```bash
cargo run --release -p t2t-pipeline --bin harness
cargo run --release -p t2t-pipeline --bin engine
```

C++:

```bash
cd cpp && cmake -S . -B build && cmake --build build --config Release
ctest --test-dir build -C Release
```

## Verification

Parsers are differential against generators that produce bytes and meaning
independently; every prefix of a valid stream must parse to a clean short-read;
corruption is refused, not reinterpreted. Books are compared against
std-collection references — full ladders, order state, unknown-order counts.
The ring's orderings are loom-model-checked and TSAN-checked, and both were
verified to discriminate: weakening one ordering fails each.

## Not here

No real signal — the strategy is one deterministic rule; the decision belongs
to a deployment, everything around it is what this measures. No routing, no
risk layer: those live in [exchange-core](https://github.com/hsdxpro/exchange-core).

MIT.
