# Installation

## As a [Homebrew](https://brew.sh/) package

```bash
brew tap timvw/tap
brew install qv
```

## Download a binary from [Github Release](https://github.com/timvw/qv/releases/latest)
```bash
wget https://github.com/timvw/qv/releases/download/v0.4.0/qv-0.4.0-x86_64-apple-darwin-generic.tar.gz
tar -zxf qv-0.4.0-x86_64-apple-darwin-generic.tar.gz
```

## Run as a [container](https://github.com/timvw/qv/pkgs/container/qv) image

```bash
docker run --rm -it -v $HOME/.aws:/root/.aws -e AWS_PROFILE=icteam ghcr.io/timvw/qv:0.4.0 s3://datafusion-testing/data/avro/alltypes_plain.avro
```

## Via rust toolchain

```bash
cargo install --git https://github.com/timvw/qv --tag v0.4.0
```