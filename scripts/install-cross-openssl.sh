#!/usr/bin/env bash
set -euo pipefail

TARGET="$1"
OPENSSL_VERSION="3.3.2"
ARCHIVE="openssl-${OPENSSL_VERSION}.tar.gz"
PREFIX="/opt/openssl/${TARGET}"

sudo apt-get update
sudo apt-get install -y curl build-essential perl pkg-config

if [[ "$TARGET" == *"musl"* ]]; then
  sudo apt-get install -y musl-tools
fi

if [[ "$TARGET" == *"aarch64"* ]]; then
  sudo apt-get install -y gcc-aarch64-linux-gnu
fi

sudo rm -rf "$PREFIX" "$ARCHIVE" "openssl-${OPENSSL_VERSION}"
sudo mkdir -p "$PREFIX"
curl -sSLO "https://www.openssl.org/source/${ARCHIVE}"
tar -xf "$ARCHIVE"
cd "openssl-${OPENSSL_VERSION}"

CONFIGURE_TARGET="linux-x86_64"
CC="gcc"
case "$TARGET" in
  aarch64-unknown-linux-gnu)
    CONFIGURE_TARGET="linux-aarch64"
    CC="aarch64-linux-gnu-gcc"
    ;;
  aarch64-unknown-linux-musl)
    CONFIGURE_TARGET="linux-aarch64"
    CC="aarch64-linux-gnu-gcc"
    ;;
  x86_64-unknown-linux-musl)
    CONFIGURE_TARGET="linux-x86_64"
    CC="musl-gcc"
    ;;
  *) ;;
esac

CC="$CC" ./Configure "$CONFIGURE_TARGET" --prefix="$PREFIX" --openssldir="$PREFIX" no-shared
make -j"$(nproc)"
sudo make install_sw

echo "OPENSSL_DIR=$PREFIX" >> "$GITHUB_ENV"
echo "PKG_CONFIG_PATH=$PREFIX/lib/pkgconfig" >> "$GITHUB_ENV"
