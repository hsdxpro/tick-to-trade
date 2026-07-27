# tick-to-trade

A minimal HFT pipeline, implemented twice — Rust and C++ — with every stage
benchmarked, every latency decomposed, and every custom structure required to
beat the standard library on the same operation stream to justify existing.

```text
UDP ticks → [feed: parse + book + BBO] →ring→ [strategy] →ring→ [gateway] → TCP orders
```

## The number

Tick-to-trade, measured two ways because they answer different questions:

| Path | min | p50 | p99 |
|---|---:|---:|---:|
| **Internal, compute only** (parse + book + decide + encode, one thread) | <100 ns | **~100 ns** | 300 ns |
| **Internal, production layout** (3 threads, SPSC rings, spinning) | 200 ns | **~300 ns** | 600 ns |
| **Wire-to-wire** (UDP in, TCP out, loopback, measured at the counterparty) | 9.7 µs | **12.5 µs** | 25.1 µs |

Both languages hit the same internal figures (C++ ~100 ns compute, ~400 ns
staged) and are clocked wire-to-wire by the **same Rust harness** — the C++
engine measured 11.9 µs p50 through it, within noise of the Rust engine's 12.5.
One harness, one methodology, two engines.

The internal figures come from the identical stage code the deployed engine
runs, handed pre-built datagrams in memory. The staged path carries one ring
hop standing in for the socket read (hop p50: ~50 ns); subtract it to model
the deployed two-hop layout. The ~12 µs between internal and wire-to-wire is
the two kernel network stacks' bill — which is the entire argument for kernel
bypass, priced rather than asserted.

Wire-to-wire is timestamped at the **counterparty**: T0 before the tick's
datagram leaves the harness, T1 when the order's bytes arrive back. The
engine's own `send` return times never enter the measurement, because a send
can complete into a kernel buffer at any relation to the wire; bytes cannot
*arrive* before they were truly sent.

Timing note, stated rather than hidden: these are Windows measurements and the
monotonic clock quantizes near 100 ns — visible as `min 0` in the raw output.
Per-item component costs below dodge that by amortizing over millions of
operations; the latency percentiles above are true within one quantum.

## Component costs, per item, both languages

| Component | Rust | C++ | Checked by |
|---|---:|---:|---|
| SPSC ring, throughput | 1.8 ns/item | 2.1 ns/item | loom (all interleavings) / TSAN |
| SPSC ring, one-way hop latency | ~50 ns p50 | — | ping-pong ÷ 2 |
| ITCH 5.0 parse | 9.0 ns/msg | 10.0 ns/msg | differential vs generator |
| FIX 4.4 parse (checksum verified) | 110 ns/msg | 121 ns/msg | same + every-prefix truncation |
| JSON parse (schema scanner) | 223 ns/msg | 293 ns/msg | same + corruption refusal |
| JSON via serde_json (baseline) | 393 ns/msg | — | the specialization gap, priced |
| Book maintain (L2+L3) | 51.4 ns/event | 52.7 ns/event | differential vs std reference |

The Rust and C++ streams are byte-identical — the generators are seeded alike
and FNV fingerprints are pinned in both test suites — so the columns compare
implementations, not workloads.

## Custom structures, against the library they had to beat

| Structure | Custom | Standard | Verdict |
|---|---:|---:|---|
| Price ladder (banded array + occupancy bitmap) | 2.7 ms | BTreeMap 30.7 ms | **11×** |
| Order map (open addressing, backward-shift delete) | 34.9 ms | HashMap 42.7 ms | 1.2× |
| Blended book, Rust | 51.4 ns/ev | 102.2 ns/ev | 2.0× |
| Blended book, C++ | 52.7 ns/ev | std::map 280.2 ns/ev | 5.3× |

Same 992,670 operations, identical final state in every row. The first ladder
was a sorted array; it lost this table to BTreeMap and was replaced — the
module documentation keeps the story, because "the benchmark killed my design"
is the point of having the benchmark.

## Layout

```text
rust/spsc       wait-free SPSC ring          loom-model-checked, mutation-verified
rust/feed       ITCH · FIX · JSON parsers    one normalized fixed-point event
rust/book       L2/L3 book maintenance       banded bitmap ladder + open addressing
rust/pipeline   engine · harness binaries    the measured pipeline
cpp/            protocol-identical twins     TSAN on Linux, MSVC + GCC warnings-as-errors
```

One event type crosses the whole system; no stage knows which wire format fed
it, and floats never appear — prices are fixed-point 1e8 end to end.

## Run it

Rust (needs rustup):

```bash
cd rust
cargo test --release                         # differential tests throughout
cargo bench -p t2t-spsc                      # ring throughput + hop latency
cargo bench -p t2t-feed                      # parser table
cargo bench -p t2t-book                      # book table, blended + isolated
cargo bench -p t2t-pipeline                  # internal tick-to-trade
RUSTFLAGS="--cfg loom" cargo test -p t2t-spsc --release   # model checking
```

Wire-to-wire (two terminals):

```bash
cargo run --release -p t2t-pipeline --bin harness
cargo run --release -p t2t-pipeline --bin engine
```

C++ (needs CMake 3.24+, C++23):

```bash
cd cpp && cmake -S . -B build && cmake --build build --config Release
ctest --test-dir build -C Release
```

TSAN, on Linux: `g++ -std=c++23 -fsanitize=thread tests/test_spsc.cpp -lpthread`.

## Verification, in one paragraph

Every parser is tested differentially against a generator that produces bytes
and their meaning independently; every prefix of a valid stream must parse to
a clean short-read; corruption must be refused, not reinterpreted. The books
are compared against std-collection references — full ladder contents, order
state, unknown-order counts — every few thousand events. The ring's memory
orderings are model-checked with loom in Rust and race-checked with TSAN in
C++, and both checkers were verified to discriminate: weakening one ordering
fails each of them. The band assertion in the ladder caught this repository's
own harness walking out of range, which is what it is for.

## Receive transports, priced

The ~12 µs network-stack bill is why the receive discipline matters. `rxlat`
measures each on a UDP ping-pong (one-way, round-trip halved), the same way,
so they compare:

| Transport | Windows p50 | Linux p50 | What it is |
|---|---:|---:|---|
| blocking `recv` | 8.2 µs | 18.8 µs | park the thread; the wakeup is the cost |
| busy-poll `recv` | **5.9 µs** | **3.4 µs** | spin a core, skip the wakeup — what the engine does |
| io_uring (polled CQ) | — | 4.6 µs | completion-based; Linux only |

The finding worth stating: **io_uring does not beat busy-poll here**, because
its advantage is amortizing syscalls across a batch and a one-packet probe has
no batch. It pulls ahead when many receives are in flight — which a real feed
handler has and a latency probe does not. Measuring it was the only way to know
that rather than assume it.

Two transports are documented seams behind the same `Receiver` trait, honestly
labelled: **AF_XDP** (kernel bypass on a commodity NIC via the generic XDP hook
— the real "fastest without special hardware", waiting on a host with a spare
interface) and **DPDK** (full bypass, written against the poll-mode-driver API
behind a `dpdk` feature that no build here enables, because a DPDK number from a
machine with no DPDK-bound NIC would be invented — and this repository does not
invent figures).

## Not here, on purpose

No real signal: the strategy is one deterministic rule, because the decision is
a deployment's business and everything around it is what this repository
measures. No multi-venue routing, no risk layer — those live in the
exchange-side project.

## License

MIT.
