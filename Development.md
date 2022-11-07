# Development

Uses standard rust toolchain:

```bash
cargo build
cargo test
cargo publish 
```

Linting:

```bash
cargo fmt
cargo clippy --all-features --all-targets --workspace -- -D warnings
cargo tomlfmt -p ./Cargo.toml
```

Or all in one as following:

```bash
./dev/rust_lint.sh
```
