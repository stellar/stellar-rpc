FROM golang:1.24-bookworm AS build
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

RUN make REPOSITORY_VERSION=${REPOSITORY_VERSION} build-stellar-rpc

# Move the binary to a common location
RUN mv stellar-rpc /bin/stellar-rpc

FROM ubuntu:22.04
ARG STELLAR_CORE_VERSION
ENV STELLAR_CORE_VERSION=${STELLAR_CORE_VERSION:-*}
ENV STELLAR_CORE_BINARY_PATH=/usr/bin/stellar-core
ENV DEBIAN_FRONTEND=noninteractive

# ca-certificates are required to make tls connections
RUN apt update && apt install -y --no-install-recommends ca-certificates curl wget gnupg apt-utils jq
RUN curl -sSL https://apt.stellar.org/SDF.asc | gpg --dearmor >/etc/apt/trusted.gpg.d/SDF.gpg
RUN echo "deb https://apt.stellar.org jammy stable" >/etc/apt/sources.list.d/SDF.list
RUN echo "deb https://apt.stellar.org jammy unstable" >/etc/apt/sources.list.d/SDF-unstable.list

# install llvm-19 so that core can be installed
RUN wget -O /etc/apt/trusted.gpg.d/apt.llvm.org.asc https://apt.llvm.org/llvm-snapshot.gpg.key
RUN echo 'deb http://apt.llvm.org/jammy/ llvm-toolchain-jammy-19 main' > /etc/apt/sources.list.d/llvm.list
RUN echo 'deb-src http://apt.llvm.org/jammy/ llvm-toolchain-jammy-19 main' >> /etc/apt/sources.list.d/llvm.list

RUN apt update && apt install -y stellar-core=${STELLAR_CORE_VERSION}
RUN apt clean

# Copy the binary from the build stage
COPY --from=build /bin/stellar-rpc /app/stellar-rpc

# Set the entrypoint to the specific binary
ENTRYPOINT ["/app/stellar-rpc"]
