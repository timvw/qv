# Installation

> [!IMPORTANT]
> qv is currently released **only through [GitHub releases](https://github.com/timvw/qv/releases)**
> (and the Homebrew tap, which installs those binaries).
>
> The [`qv` crate on crates.io](https://crates.io/crates/qv) is **outdated** (last published: 0.9.6, March 2024)
> and does not receive new versions. qv depends on unreleased versions of
> [iceberg-rust](https://github.com/apache/iceberg-rust) and
> [datafusion-iceberg](https://github.com/apache/datafusion-iceberg) (pinned to git revisions),
> and crates.io does not accept crates with git dependencies.
> Publishing to crates.io will resume once those dependencies have compatible releases.
>
> Do not use `cargo install qv`: it installs the old 0.9.6 version.

## As a [Homebrew](https://brew.sh/) package

```bash
brew tap timvw/tap
brew install qv
```

## Download a binary from [GitHub releases](https://github.com/timvw/qv/releases/latest)

Prebuilt binaries are available for Linux (gnu and musl), macOS and Windows, on x86_64 and aarch64.

```bash
curl -fsSLO https://github.com/timvw/qv/releases/latest/download/qv-aarch64-apple-darwin.tar.gz
tar -zxf qv-aarch64-apple-darwin.tar.gz
```

Replace `aarch64-apple-darwin` with your target, for example `x86_64-unknown-linux-gnu`,
`x86_64-unknown-linux-musl` or `x86_64-pc-windows-msvc` (a `.zip` archive).

## Via the Rust toolchain

Install from the git repository (not from crates.io), preferably at a release tag:

```bash
cargo install --locked --git https://github.com/timvw/qv --tag v0.10.0
```

See [the releases page](https://github.com/timvw/qv/releases) for the latest tag.

## As a container image

No container image is published at the moment. You can build one from the [Dockerfile](Dockerfile):

```bash
docker build -t qv .
docker run --rm -it -v $HOME/.aws:/root/.aws -e AWS_PROFILE=default qv s3://datafusion-testing/data/avro/alltypes_plain.avro
```
