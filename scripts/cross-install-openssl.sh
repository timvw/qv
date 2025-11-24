#!/usr/bin/env sh
set -eu

if command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends pkg-config libssl-dev ca-certificates
elif command -v apk >/dev/null 2>&1; then
  apk add --no-cache pkgconf openssl-dev ca-certificates
elif command -v dnf >/dev/null 2>&1; then
  dnf install -y openssl-devel ca-certificates
  dnf install -y pkgconfig || dnf install -y pkgconf-pkg-config
elif command -v yum >/dev/null 2>&1; then
  yum install -y openssl-devel ca-certificates
  yum install -y pkgconfig || yum install -y pkgconf-pkg-config
else
  echo "Unable to install OpenSSL build dependencies: no supported package manager found" >&2
  exit 1
fi
