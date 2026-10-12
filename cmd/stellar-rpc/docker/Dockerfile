FROM golang:1.26-bookworm AS build
ARG RUST_TOOLCHAIN_VERSION=stable
ARG REPOSITORY_VERSION

WORKDIR /go/src/github.com/stellar/stellar-rpc

ADD . ./

RUN git config --global --add safe.directory "/go/src/github.com/stellar/stellar-rpc"

ENV CARGO_HOME=/rust/.cargo
ENV RUSTUP_HOME=/rust/.rust
ENV PATH="/usr/local/go/bin:$CARGO_HOME/bin:${PATH}"
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update
RUN apt install -y build-essential jq
RUN apt clean

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain $RUST_TOOLCHAIN_VERSION

RUN make REPOSITORY_VERSION=${REPOSITORY_VERSION} build-rpc-v1

# Move the binary to a common location
RUN mv stellar-rpc /bin/stellar-rpc

FROM ubuntu:24.04
ARG STELLAR_CORE_VERSION
ENV STELLAR_CORE_VERSION=${STELLAR_CORE_VERSION:-*}
ENV STELLAR_CORE_BINARY_PATH=/usr/bin/stellar-core
ENV DEBIAN_FRONTEND=noninteractive

# ca-certificates are required to make tls connections
RUN apt update && apt install -y --no-install-recommends ca-certificates curl wget gnupg apt-utils jq
COPY scripts/install-stellar-core.sh /tmp/install-stellar-core.sh
RUN CHANNELS="stable unstable" /tmp/install-stellar-core.sh "${STELLAR_CORE_VERSION}"
RUN apt clean

# Copy the binary from the build stage
COPY --from=build /bin/stellar-rpc /app/stellar-rpc

# Set the entrypoint to the specific binary
ENTRYPOINT ["/app/stellar-rpc"]
