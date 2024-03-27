# Development

## Standard rust toolchain: 

Uses the familiar cargo targets: build, test, fmt, clippy

## Testing

```bash
./ci/minio_start.sh
cargo test
./ci/minio_stop.sh
```

## Releasing

Leverages [Release-plz](https://github.com/MarcoIeni/release-plz) to build Release PR's.


