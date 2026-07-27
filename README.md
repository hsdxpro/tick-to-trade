# tick-to-trade

A minimal HFT pipeline, under construction: market data in, decision, order
out, measured wire-to-wire. Twin implementations in Rust and C++.

Built so far:

- `rust/spsc`, `cpp/spsc.hpp` — a wait-free SPSC ring, protocol-identical in
  both languages. The Rust side is model-checked with loom (every reachable
  interleaving); the C++ side runs under ThreadSanitizer. Both checkers were
  verified to discriminate: weakening one memory ordering fails each of them.
- Measured, one spinning producer to one spinning consumer, 20M items:
  Rust 1.8 ns/item, C++ 2.1 ns/item.
