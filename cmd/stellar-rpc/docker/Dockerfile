FROM golang:1.23-bullseye as build
ARG RUST_TOOLCHAIN_VERSION=stable
ARG REPOSITORY_VERSION

WORKDIR /go/src/github.com/stellar/stellar-rpc

ADD . ./

RUN git config --global --add safe.directory "/go/src/github.com/stellar/stellar-rpc"

ENV CARGO_HOME=/rust/.cargo
ENV RUSTUP_HOME=/rust/.rust
ENV PATH="/usr/local/go/bin:$CARGO_HOME/bin:${PATH}"
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get install -y build-essential
RUN apt-get clean

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain $RUST_TOOLCHAIN_VERSION

RUN make REPOSITORY_VERSION=${REPOSITORY_VERSION} build-stellar-rpc

# Move the binary to a common location
RUN mv stellar-rpc /bin/stellar-rpc

FROM ubuntu:22.04
ARG STELLAR_CORE_VERSION
ENV STELLAR_CORE_VERSION=${STELLAR_CORE_VERSION:-*}
ENV STELLAR_CORE_BINARY_PATH /usr/bin/stellar-core
ENV DEBIAN_FRONTEND=noninteractive

# ca-certificates are required to make tls connections
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl jq wget gnupg apt-utils
RUN wget -qO - https://apt.stellar.org/SDF.asc | APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=true apt-key add -
RUN echo "deb https://apt.stellar.org focal stable" >/etc/apt/sources.list.d/SDF.list
RUN echo "deb https://apt.stellar.org focal unstable" >/etc/apt/sources.list.d/SDF-unstable.list
RUN apt-get update && apt-get install -y stellar-core=${STELLAR_CORE_VERSION}
RUN apt-get clean

# Copy the binary from the build stage
COPY --from=build /bin/stellar-rpc /app/stellar-rpc

# Set the entrypoint to the specific binary
ENTRYPOINT ["/app/stellar-rpc"]
