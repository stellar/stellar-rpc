# ===========================================================================
# Dockerfile for stellar-rpc
# ===========================================================================
#
# Key tenets:
#
#   1. ALL stages use ubuntu:24.04 (noble) — same glibc everywhere.
#      Shared libraries (.so) built in one stage work in any other stage.
#      Mixed OS versions (e.g. Debian 13 build + Ubuntu 22.04 runtime)
#      cause "GLIBC_2.39 not found" errors due to glibc version mismatch.
#
#   2. zstd and RocksDB are built from source via install scripts,
#      NOT from apt. Ubuntu 24.04 ships zstd 1.5.5 but we need 1.5.7.
#      This keeps Docker and CI consistent — both use the same scripts
#      and the same versions.
#
#   3. RocksDB is built as a SHARED library (.so) — same as CI. Both
#      Docker and CI use ubuntu-24.04 (same glibc), so shared libs work
#      everywhere. Docker additionally builds tools (ldb, sst_dump,
#      db_bench) which CI doesn't need.
#
#   4. Go and Rust are installed from upstream tarballs. There is no
#      golang:1.25-noble image (Go only publishes Debian-based images).
#      This is equivalent to golang:1.25-* but on ubuntu:24.04.
#
#   5. Each native lib has its own build stage for Docker layer caching.
#      Code changes don't trigger zstd/RocksDB rebuilds.
#
#   6. stellar-core is installed from apt.stellar.org noble packages.
#
# ===========================================================================

# ── Stage 1: Build zstd 1.5.7 ──────────────────────────────────────
#
# Built from source via install-zstd.sh (same script as CI).
# Ubuntu 24.04 ships 1.5.5 which is below the 1.5.7 minimum.
#
FROM ubuntu:24.04 AS zstd-build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && apt install -y build-essential cmake ninja-build curl && apt clean

COPY scripts/install-zstd.sh /tmp/install-zstd.sh
RUN PREFIX=/usr/local /tmp/install-zstd.sh

# ── Stage 2: Build RocksDB ──────────────────────────────────────────
#
# Uses zstd from stage 1 (not apt), copied to /usr/local/ below.
# cmake's find_package(zstd) finds it there (default search path).
# Builds shared lib + tools (ldb, sst_dump, db_bench).
#
FROM ubuntu:24.04 AS rocksdb-build
ARG ROCKSDB_VERSION=10.9.1
ARG ROCKSDB_SHA256=e2e2e0254ddcb5338a58ba0723c90e792dbdca10aec520f7186e7b3a3e1c5223

ENV DEBIAN_FRONTEND=noninteractive

# cmake + ninja for the build. Compression dev libs (snappy, lz4, zlib).
# No libzstd-dev from apt — using 1.5.7 from stage 1 instead.
RUN apt update && apt install -y \
    build-essential cmake ninja-build curl \
    libsnappy-dev liblz4-dev zlib1g-dev \
    && apt clean

# Copy zstd 1.5.7 headers + libs from stage 1.
COPY --from=zstd-build /usr/local/include/ /usr/local/include/
COPY --from=zstd-build /usr/local/lib/libzstd* /usr/local/lib/
RUN ldconfig

# Download RocksDB source with SHA256 verification.
RUN curl -sSfL -o /tmp/rocksdb.tar.gz \
    "https://github.com/facebook/rocksdb/archive/refs/tags/v${ROCKSDB_VERSION}.tar.gz" \
    && echo "${ROCKSDB_SHA256}  /tmp/rocksdb.tar.gz" | sha256sum -c \
    && tar xzf /tmp/rocksdb.tar.gz -C /tmp \
    && rm /tmp/rocksdb.tar.gz

# cmake flags:
#   ROCKSDB_BUILD_SHARED=ON  — shared .so (see tenet 3)
#   WITH_ZSTD=ON             — zstd compression (finds 1.5.7 from stage 1)
#   WITH_BZ2=OFF             — bz2 not needed, avoids -lbz2 dep
#   WITH_GFLAGS=OFF          — not installed, not needed
#   WITH_TOOLS=ON            — ldb, sst_dump (built in tools/ subdir)
#   WITH_BENCHMARK_TOOLS=ON  — db_bench (built in build root)
#   WITH_CORE_TOOLS=ON       — needed for ldb and sst_dump
#   PORTABLE=1               — no CPU-specific instructions
#   WITH_TESTS=OFF           — skip test compilation
#
# Note: cmake install does NOT install tool binaries — only the library
# and headers. Tools are manually copied from the build directory.
RUN cmake -S /tmp/rocksdb-${ROCKSDB_VERSION} -B /tmp/rocksdb-build \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    -DROCKSDB_BUILD_SHARED=ON \
    -DWITH_TESTS=OFF \
    -DWITH_ZSTD=ON \
    -DWITH_BZ2=OFF \
    -DWITH_GFLAGS=OFF \
    -DWITH_TOOLS=ON \
    -DWITH_BENCHMARK_TOOLS=ON \
    -DWITH_CORE_TOOLS=ON \
    -DPORTABLE=1 \
    && ninja -C /tmp/rocksdb-build -j"$(nproc)" \
    && ninja -C /tmp/rocksdb-build install \
    && ldconfig \
    && cp /tmp/rocksdb-build/tools/ldb /tmp/rocksdb-build/tools/sst_dump /usr/local/bin/ \
    && cp /tmp/rocksdb-build/db_bench /usr/local/bin/ \
    && rm -rf /tmp/rocksdb-${ROCKSDB_VERSION} /tmp/rocksdb-build

# ── Stage 3: Build stellar-rpc ──────────────────────────────────────
#
# Go + Rust installed from upstream tarballs (see tenet 4).
#
FROM ubuntu:24.04 AS rpc-build
ARG GO_VERSION=1.25.8
ARG RUST_TOOLCHAIN_VERSION=stable
ARG REPOSITORY_VERSION

ENV CARGO_HOME=/rust/.cargo
ENV RUSTUP_HOME=/rust/.rust
ENV PATH="/usr/local/go/bin:$CARGO_HOME/bin:${PATH}"
ENV DEBIAN_FRONTEND=noninteractive

# build-essential, curl, git, jq: needed by the Makefile (git for version
# embedding via git rev-parse/describe, jq for version parsing, curl for
# Go/Rust install). git also needed for safe.directory config below.
#
# libsnappy-dev, liblz4-dev, zlib1g-dev: grocksdb's #cgo LDFLAGS include
# -lsnappy -llz4 -lz (see grocksdb/non_builtin.go). The linker needs
# these dev packages to resolve those flags. zstd comes from stage 1.
RUN apt update && apt install -y \
    build-essential curl git jq \
    libsnappy-dev liblz4-dev zlib1g-dev \
    && apt clean

# Install Go.
RUN curl -sSfL "https://go.dev/dl/go${GO_VERSION}.linux-$(dpkg --print-architecture).tar.gz" \
    | tar -C /usr/local -xzf -

# Install Rust (needed for libpreflight.a and libxdr2json.a).
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain $RUST_TOOLCHAIN_VERSION

# zstd 1.5.7 headers + libs from stage 1.
COPY --from=zstd-build /usr/local/include/ /usr/local/include/
COPY --from=zstd-build /usr/local/lib/libzstd* /usr/local/lib/

# RocksDB headers + shared lib from stage 2.
COPY --from=rocksdb-build /usr/local/include/rocksdb /usr/local/include/rocksdb
COPY --from=rocksdb-build /usr/local/lib/librocksdb* /usr/local/lib/
RUN ldconfig

WORKDIR /go/src/github.com/stellar/stellar-rpc
ADD . ./
RUN git config --global --add safe.directory "/go/src/github.com/stellar/stellar-rpc" || true

RUN make REPOSITORY_VERSION=${REPOSITORY_VERSION} build-stellar-rpc

RUN mv stellar-rpc /bin/stellar-rpc

# ── Stage 4: Runtime image ──────────────────────────────────────────
#
# Minimal image. No compilers, no dev headers.
#
FROM ubuntu:24.04 AS runtime
ARG STELLAR_CORE_VERSION
ENV STELLAR_CORE_VERSION=${STELLAR_CORE_VERSION:-*}
ENV STELLAR_CORE_BINARY_PATH=/usr/bin/stellar-core
ENV DEBIAN_FRONTEND=noninteractive

# Runtime deps: compression shared libs for RocksDB, TLS certs, utilities.
# No libzstd from apt — using 1.5.7 from zstd-build stage.
RUN apt update && apt install -y --no-install-recommends \
    ca-certificates curl wget gnupg apt-utils jq \
    libsnappy1v5 liblz4-1 zlib1g \
    && apt clean

# stellar-core from SDF apt repo (noble packages).
RUN curl -sSL https://apt.stellar.org/SDF.asc | gpg --dearmor >/etc/apt/trusted.gpg.d/SDF.gpg
RUN echo "deb https://apt.stellar.org noble stable" >/etc/apt/sources.list.d/SDF.list
RUN echo "deb https://apt.stellar.org noble unstable" >/etc/apt/sources.list.d/SDF-unstable.list
RUN apt update && apt install -y stellar-core=${STELLAR_CORE_VERSION}
RUN apt clean

# zstd 1.5.7 shared lib from stage 1 (needed by librocksdb.so and stellar-rpc at runtime).
COPY --from=zstd-build /usr/local/lib/libzstd.so* /usr/local/lib/

# RocksDB shared lib + tools from stage 2.
COPY --from=rocksdb-build /usr/local/lib/librocksdb* /usr/local/lib/
COPY --from=rocksdb-build /usr/local/bin/ldb /usr/local/bin/
COPY --from=rocksdb-build /usr/local/bin/sst_dump /usr/local/bin/
COPY --from=rocksdb-build /usr/local/bin/db_bench /usr/local/bin/
RUN ldconfig

# stellar-rpc binary from stage 3.
COPY --from=rpc-build /bin/stellar-rpc /app/stellar-rpc

ENTRYPOINT ["/app/stellar-rpc"]
