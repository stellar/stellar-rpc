SUDO := $(shell docker version >/dev/null 2>&1 || echo "sudo")

# https://github.com/opencontainers/image-spec/blob/master/annotations.md
BUILD_DATE := $(shell date -u +%FT%TZ)

# Extract latest release semver from GitHub
STELLAR_RPC_LATEST_RELEASE := $(shell curl -sS https://api.github.com/repos/stellar/stellar-rpc/releases/latest|jq -r ".tag_name"| tr -d "v" )

# If deb version was provided via the STELLAR_RPC_VERSION variable use it.
# If not get latest deb build matching release from GitHub
ifndef STELLAR_RPC_VERSION
        STELLAR_RPC_VERSION_PACKAGE_VERSION := $(shell curl -sS https://apt.stellar.org/dists/focal/unstable/binary-amd64/Packages|grep -A 18 stellar-stellar-rpc|grep Version|grep $(STELLAR_RPC_LATEST_RELEASE)|head -1|cut -d' ' -f2 )
else
        STELLAR_RPC_VERSION_PACKAGE_VERSION := $(STELLAR_RPC_VERSION)
endif

ifndef STELLAR_RPC_VERSION_PACKAGE_VERSION
        $(error Couldn't establish deb build from version $(STELLAR_RPC_LATEST_RELEASE). Has the package been built?)
endif

ifndef STELLAR_CORE_VERSION
        $(error STELLAR_CORE_VERSION environment variable must be set. For example 19.10.1-1310.6649f5173.focal~soroban)
endif

docker-build:
	$(SUDO) docker build --pull --platform linux/amd64 $(DOCKER_OPTS) \
	--label org.opencontainers.image.created="$(BUILD_DATE)" \
	--build-arg STELLAR_CORE_VERSION=$(STELLAR_CORE_VERSION) --build-arg STELLAR_RPC_VERSION=$(STELLAR_RPC_VERSION_PACKAGE_VERSION) \
	-t $(TAG) -f Dockerfile.release .

docker-push:
	$(SUDO) docker push $(TAG)
