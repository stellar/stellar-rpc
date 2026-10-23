# Everything is built on ubuntu:22.04 so the binary needs no glibc newer than
# jammy's, and a newer glibc (noble) runs it too: one Debian package for both.
# Go and Rust come from upstream tarballs because there is no golang image on
# ubuntu:22.04. zstd and RocksDB are static archives the binary carries.
#
# The package builder runs `docker build --target build`
# and copies /bin/stellar-rpc out, so that stage name and path stay.

# --- Stage 1: zstd ---
FROM ubuntu:22.04 AS zstd-build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && apt install -y build-essential cmake ninja-build curl && apt clean

COPY scripts/install-zstd.sh /tmp/install-zstd.sh
RUN PREFIX=/usr/local /tmp/install-zstd.sh

# --- Stage 2: RocksDB (static archive + CLI tools), on top of the zstd stage ---
FROM zstd-build AS rocksdb-build
COPY scripts/install-rocksdb.sh /tmp/install-rocksdb.sh
RUN WITH_TOOLS=1 PREFIX=/usr/local ZSTD_HOME=/usr/local /tmp/install-rocksdb.sh

# --- Stage 3: Build stellar-rpc ---
FROM ubuntu:22.04 AS build
ARG GO_VERSION=1.26.9
ARG RUST_TOOLCHAIN_VERSION=stable
ARG REPOSITORY_VERSION

ENV CARGO_HOME=/rust/.cargo
ENV RUSTUP_HOME=/rust/.rust
ENV PATH="/usr/local/go/bin:$CARGO_HOME/bin:${PATH}"
ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y build-essential curl git jq && apt clean

RUN curl -sSfL "https://go.dev/dl/go${GO_VERSION}.linux-$(dpkg --print-architecture).tar.gz" \
    | tar -C /usr/local -xzf -

# Rust builds the preflight and xdr2json libraries the binary links through cgo.
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain $RUST_TOOLCHAIN_VERSION

COPY --from=rocksdb-build /usr/local/include/ /usr/local/include/
COPY --from=rocksdb-build /usr/local/lib/libzstd.a /usr/local/lib/librocksdb.a /usr/local/lib/

WORKDIR /go/src/github.com/stellar/stellar-rpc
ADD . ./
RUN git config --global --add safe.directory "/go/src/github.com/stellar/stellar-rpc" || true

# The archives are already in /usr/local, so make links them instead of
# rebuilding them.
RUN make REPOSITORY_VERSION=${REPOSITORY_VERSION} ZSTD_HOME=/usr/local ROCKSDB_HOME=/usr/local build-stellar-rpc
RUN mv stellar-rpc /bin/stellar-rpc

# --- Stage 4: Runtime ---
FROM ubuntu:24.04 AS runtime
ARG STELLAR_CORE_VERSION
ENV STELLAR_CORE_VERSION=${STELLAR_CORE_VERSION:-*}
ENV STELLAR_CORE_BINARY_PATH=/usr/bin/stellar-core
ENV DEBIAN_FRONTEND=noninteractive

# ca-certificates are required to make tls connections
RUN apt update && apt install -y --no-install-recommends \
    ca-certificates curl wget gnupg apt-utils jq \
    && apt clean

# stellar-core from SDF apt repo (noble packages).
COPY scripts/install-stellar-core.sh /tmp/install-stellar-core.sh
RUN CHANNELS="stable unstable" /tmp/install-stellar-core.sh "${STELLAR_CORE_VERSION}"
RUN apt clean

# Each tool carries its own copy of RocksDB.
COPY --from=rocksdb-build /usr/local/bin/ldb /usr/local/bin/sst_dump /usr/local/bin/

COPY --from=build /bin/stellar-rpc /usr/bin/stellar-rpc
ENTRYPOINT ["/usr/bin/stellar-rpc"]
