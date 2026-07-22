#!/usr/bin/env bash
#
# Idempotent bootstrap for a full-history benchmark dev box: an EC2 instance
# with a local NVMe instance store (e.g. m6id.2xlarge) running Ubuntu 24.04.
# Safe to re-run any time — in particular after an instance stop/start, which
# wipes the NVMe instance store (golden packs are re-downloaded by
# campaign.sh).
#
# Usage (on the box):
#   ./scripts/bench-devbox/bootstrap.sh
#
# Overridable: NVME_DEV (default /dev/nvme1n1), REPO (default ~/stellar-rpc),
# BRANCH (default main).
#
set -euo pipefail

NVME_DEV="${NVME_DEV:-/dev/nvme1n1}"
MOUNT=/mnt/nvme
REPO="${REPO:-$HOME/stellar-rpc}"
BRANCH="${BRANCH:-main}"

note() { echo "== $*"; }

# --- NVMe instance store: format if raw, mount if unmounted -----------------
[ -b "$NVME_DEV" ] || { echo "error: $NVME_DEV is not a block device" >&2; exit 1; }
model=$(lsblk -no MODEL "$NVME_DEV" | head -1)
case "$model" in
  *"Instance Storage"*) ;;
  *) echo "error: refusing to touch $NVME_DEV — model '$model' is not the EC2 instance store" >&2; exit 1 ;;
esac
if ! sudo blkid "$NVME_DEV" >/dev/null 2>&1; then
  note "no filesystem on $NVME_DEV (fresh instance store) — formatting"
  sudo mkfs.ext4 -m0 "$NVME_DEV"
fi
if ! mountpoint -q "$MOUNT"; then
  sudo mkdir -p "$MOUNT"
  sudo mount -o noatime "$NVME_DEV" "$MOUNT"
  sudo chown "$USER" "$MOUNT"
fi
mkdir -p "$MOUNT"/bench/{golden,scratch,hot,results}

# --- fsync honesty probe: the whole reason this box exists ------------------
probe=$(dd if=/dev/zero of="$MOUNT/.fsync-probe" bs=4k count=2000 oflag=dsync 2>&1 | tail -1)
rm -f "$MOUNT/.fsync-probe"
note "fsync probe: $probe"
case "$probe" in
  *GB/s*) echo "WARNING: GB/s-scale dsync writes — fsync is being absorbed; hot-commit numbers would be fiction" >&2 ;;
esac

# --- system packages ---------------------------------------------------------
note "apt packages"
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential git jq pkg-config cmake ninja-build \
  tmux libsnappy-dev liblz4-dev zlib1g-dev

# --- Go (>= 1.26; Noble's apt Go is too old) ---------------------------------
if ! /usr/local/go/bin/go version 2>/dev/null | grep -Eq 'go1\.(2[6-9]|[3-9][0-9])'; then
  note "installing Go"
  GOVER=$(curl -fsSL 'https://go.dev/VERSION?m=text' | head -1)
  curl -fsSL "https://go.dev/dl/${GOVER}.linux-amd64.tar.gz" -o /tmp/go.tgz
  # decompress as the user: sudo'd tar cannot always exec gzip
  gunzip -f /tmp/go.tgz
  sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xf /tmp/go.tar
fi

# --- Rust --------------------------------------------------------------------
if [ ! -x "$HOME/.cargo/bin/rustc" ]; then
  note "installing Rust"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi

# --- repo --------------------------------------------------------------------
if [ ! -d "$REPO/.git" ]; then
  note "cloning $BRANCH"
  git clone --branch "$BRANCH" https://github.com/stellar/stellar-rpc.git "$REPO"
fi

# --- native libs, mirroring CI's setup-go action ------------------------------
[ -e "$HOME/.zstd/lib/libzstd.so" ] ||
  (cd "$REPO" && PREFIX="$HOME/.zstd" ./scripts/install-zstd.sh)
[ -e "$HOME/.rocksdb/lib/librocksdb.so" ] ||
  (cd "$REPO" && PREFIX="$HOME/.rocksdb" ZSTD_HOME="$HOME/.zstd" ./scripts/install-rocksdb.sh)

# --- environment: persist for future shells, set for this run ----------------
if ! grep -q '# bench-devbox env' "$HOME/.bashrc"; then
  cat >> "$HOME/.bashrc" <<'EOF'
# bench-devbox env
export PATH=/usr/local/go/bin:$HOME/go/bin:$HOME/.cargo/bin:$PATH
export CGO_CFLAGS="-I$HOME/.zstd/include -I$HOME/.rocksdb/include"
export CGO_LDFLAGS="-L$HOME/.zstd/lib -L$HOME/.rocksdb/lib"
export LD_LIBRARY_PATH="$HOME/.zstd/lib:$HOME/.rocksdb/lib"
EOF
fi
export PATH=/usr/local/go/bin:$HOME/go/bin:$HOME/.cargo/bin:$PATH
export CGO_CFLAGS="-I$HOME/.zstd/include -I$HOME/.rocksdb/include"
export CGO_LDFLAGS="-L$HOME/.zstd/lib -L$HOME/.rocksdb/lib"
export LD_LIBRARY_PATH="$HOME/.zstd/lib:$HOME/.rocksdb/lib"

# --- build + verify -----------------------------------------------------------
note "build (make install → ~/go/bin/stellar-rpc)"
cd "$REPO"
make install
note "bench package tests under -race"
go test -race ./cmd/stellar-rpc/internal/fullhistory/bench/...
stellar-rpc version
note "bootstrap OK"
