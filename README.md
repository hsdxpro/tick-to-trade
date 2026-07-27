# tick-to-trade

A minimal HFT pipeline, under construction: market data in, decision, order
out, measured wire-to-wire. Twin implementations in Rust and C++.

Headline, measured wire-to-wire at the counterparty on loopback: **p50 12.5 us**
tick to trade (min 9.7, p99 25.1), through UDP in, ITCH parse, book update,
strategy decision, two lock-free rings, and TCP order out.

Built so far:

- `rust/spsc`, `cpp/spsc.hpp` — a wait-free SPSC ring, protocol-identical in
  both languages. The Rust side is model-checked with loom (every reachable
  interleaving); the C++ side runs under ThreadSanitizer. Both checkers were
  verified to discriminate: weakening one memory ordering fails each of them.
- Measured, one spinning producer to one spinning consumer, 20M items:
  Rust 1.8 ns/item, C++ 2.1 ns/item.
