#!/bin/bash

# One-click installer for uv (Python package/dependency manager)

set -e

echo "Checking if uv is already installed..."
if command -v uv &> /dev/null; then
    echo "uv is already installed at: $(which uv)"
    uv --version
    exit 0
fi

arch=$(uname -m)
os=$(uname -s | tr '[:upper:]' '[:lower:]')

if [ "$arch" = "x86_64" ]; then
    uv_arch="x86_64"
elif [ "$arch" = "aarch64" ]; then
    uv_arch="aarch64"
else
    echo "Unsupported architecture: $arch"
    exit 1
fi

uv_url="https://github.com/astral-sh/uv/releases/latest/download/uv-$os-$uv_arch"

echo "Downloading uv for $os/$arch from $uv_url..."
curl -LsSf "$uv_url" -o uv

echo "Making uv executable and moving to /usr/local/bin (may require sudo)..."
chmod +x uv
sudo mv uv /usr/local/bin/uv

echo "uv installed successfully:"
uv --version
