# All stages use ubuntu:24.04 — same glibc everywhere, so shared
# libraries built in one stage work in any other. zstd and RocksDB
# are built from source (not apt) to pin versions. Go and Rust come
# from upstream tarballs since there's no golang:1.25-noble image.

# --- Stage 1: zstd 1.5.7 (ubuntu 24.04 ships 1.5.5) ---
FROM ubuntu:24.04 AS zstd-build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && apt install -y build-essential cmake ninja-build curl && apt clean

COPY scripts/install-zstd.sh /tmp/install-zstd.sh
RUN PREFIX=/usr/local /tmp/install-zstd.sh

# --- Stage 2: RocksDB 10.9.1 (shared + tools) ---
FROM ubuntu:24.04 AS rocksdb-build
ARG ROCKSDB_VERSION=10.9.1
ARG ROCKSDB_SHA256=e2e2e0254ddcb5338a58ba0723c90e792dbdca10aec520f7186e7b3a3e1c5223

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y \
    build-essential cmake ninja-build curl \
    libsnappy-dev liblz4-dev zlib1g-dev \
    && apt clean

# zstd 1.5.7 from stage 1 (not apt).
COPY --from=zstd-build /usr/local/include/ /usr/local/include/
COPY --from=zstd-build /usr/local/lib/libzstd* /usr/local/lib/
RUN ldconfig

RUN curl -sSfL -o /tmp/rocksdb.tar.gz \
    "https://github.com/facebook/rocksdb/archive/refs/tags/v${ROCKSDB_VERSION}.tar.gz" \
    && echo "${ROCKSDB_SHA256}  /tmp/rocksdb.tar.gz" | sha256sum -c \
    && tar xzf /tmp/rocksdb.tar.gz -C /tmp \
    && rm /tmp/rocksdb.tar.gz

# Shared lib + tools. cmake install doesn't install tool binaries,
# so they're copied manually from the build directory.
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

# --- Stage 3: Build stellar-rpc ---
FROM ubuntu:24.04 AS rpc-build
ARG GO_VERSION=1.25.8
ARG RUST_TOOLCHAIN_VERSION=stable
ARG REPOSITORY_VERSION

ENV CARGO_HOME=/rust/.cargo
ENV RUSTUP_HOME=/rust/.rust
ENV PATH="/usr/local/go/bin:$CARGO_HOME/bin:${PATH}"
ENV DEBIAN_FRONTEND=noninteractive

# grocksdb cgo flags need -lsnappy -llz4 -lz at link time.
RUN apt update && apt install -y \
    build-essential curl git jq \
    libsnappy-dev liblz4-dev zlib1g-dev \
    && apt clean

RUN curl -sSfL "https://go.dev/dl/go${GO_VERSION}.linux-$(dpkg --print-architecture).tar.gz" \
    | tar -C /usr/local -xzf -

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain $RUST_TOOLCHAIN_VERSION

COPY --from=zstd-build /usr/local/include/ /usr/local/include/
COPY --from=zstd-build /usr/local/lib/libzstd* /usr/local/lib/
COPY --from=rocksdb-build /usr/local/include/rocksdb /usr/local/include/rocksdb
COPY --from=rocksdb-build /usr/local/lib/librocksdb* /usr/local/lib/
RUN ldconfig

WORKDIR /go/src/github.com/stellar/stellar-rpc
ADD . ./
RUN git config --global --add safe.directory "/go/src/github.com/stellar/stellar-rpc" || true

RUN make REPOSITORY_VERSION=${REPOSITORY_VERSION} build-stellar-rpc
RUN mv stellar-rpc /bin/stellar-rpc

# --- Stage 4: Runtime ---
FROM ubuntu:24.04 AS runtime
ARG STELLAR_CORE_VERSION
ENV STELLAR_CORE_VERSION=${STELLAR_CORE_VERSION:-*}
ENV STELLAR_CORE_BINARY_PATH=/usr/bin/stellar-core
ENV DEBIAN_FRONTEND=noninteractive

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

COPY --from=zstd-build /usr/local/lib/libzstd.so* /usr/local/lib/
COPY --from=rocksdb-build /usr/local/lib/librocksdb* /usr/local/lib/
COPY --from=rocksdb-build /usr/local/bin/ldb /usr/local/bin/
COPY --from=rocksdb-build /usr/local/bin/sst_dump /usr/local/bin/
COPY --from=rocksdb-build /usr/local/bin/db_bench /usr/local/bin/
RUN ldconfig

COPY --from=rpc-build /bin/stellar-rpc /app/stellar-rpc
ENTRYPOINT ["/app/stellar-rpc"]
