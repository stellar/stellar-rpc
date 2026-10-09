#!/bin/bash

set -e

SED=sed
if [ -z "$(sed --version 2>&1 | grep GNU)" ]; then
    SED=gsed
fi

CURL="curl -sL --fail-with-body"

# PROTOS is in ascending order so the last iteration of the PROTOS-based loops
# will end up with the highest protocol value used for recording any state
# variables.
PROTOS=$($SED -n ':pkg; /"soroban-env-host"/ {n; /version/ { s/[^0-9]*\([0-9]\+\).*/\1/ p; b pkg;}}' Cargo.toml | sort -n | tr '\n' ' ')
if [ -z "$PROTOS" ]; then
  echo "Cannot find soroban-env-host dependencies in Cargo.toml"
  exit 1
fi

for PROTO in $PROTOS
do
  if ! CARGO_OUTPUT=$(cargo tree -p soroban-env-host@$PROTO 2>&1); then
    echo "The project depends on multiple versions of the soroban-env-host@$PROTO Rust library, please unify them."
    echo
    echo
    echo "Full error:"
    echo $CARGO_OUTPUT
    exit 1
  fi
done

# revision of the https://github.com/stellar/rs-stellar-xdr library used by the Rust code
RS_STELLAR_XDR_REVISION=""

# revision of https://github.com/stellar/stellar-xdr/ used by the Rust code
STELLAR_XDR_REVISION_FROM_RUST=""

function stellar_xdr_version_from_rust_dep_tree {
  LINE=$(grep stellar-xdr | head -n 1)
  # try to obtain a commit
  COMMIT=$(echo $LINE | $SED -n 's/.*rev=\(.*\)#.*/\1/p')
  if [ -n "$COMMIT" ]; then
    echo "$COMMIT"
    return
  fi
  # obtain a crate version
  echo $LINE | $SED -n  's/.*stellar-xdr \(v\)\{0,1\}\([^ ]*\).*/\2/p'
}

# The stellar-xdr crate's major version doesn't track the protocol (e.g. the
# protocol 29 and 30 hosts both use stellar-xdr 28.x, from different sources),
# so take each host's stellar-xdr from that host's own dependency tree.
for PROTO in $PROTOS
do
  if CARGO_OUTPUT=$(cargo tree -e normal --prefix none -p soroban-env-host@$PROTO 2>&1); then
    RS_STELLAR_XDR_REVISION=$(echo -n "$CARGO_OUTPUT" | stellar_xdr_version_from_rust_dep_tree)
    # Remember it per protocol for the comparison with core below.
    printf -v "RS_STELLAR_XDR_REVISION_P${PROTO}" '%s' "$RS_STELLAR_XDR_REVISION"
    if [ ${#RS_STELLAR_XDR_REVISION} -eq 40 ]; then
      # revision is a git hash. rs-stellar-xdr moved the pinned stellar-xdr
      # commit from xdr/curr-version to the top-level xdr-version file in v27,
      # so read xdr-version first and fall back to xdr/curr-version for older
      # layouts. The || sits outside the command substitution so a
      # --fail-with-body 404 body can't be concatenated into the captured
      # revision; each assignment captures only its own command's stdout.
      STELLAR_XDR_REVISION_FROM_RUST=$($CURL "https://raw.githubusercontent.com/stellar/rs-stellar-xdr/${RS_STELLAR_XDR_REVISION}/xdr-version" 2>/dev/null) \
        || STELLAR_XDR_REVISION_FROM_RUST=$($CURL "https://raw.githubusercontent.com/stellar/rs-stellar-xdr/${RS_STELLAR_XDR_REVISION}/xdr/curr-version" 2>/dev/null)
    else
      # revision is a crate version
      CARGO_SRC_BASE_DIR=$(realpath ${CARGO_HOME:-$HOME/.cargo}/registry/src/index*)
      CRATE_DIR="${CARGO_SRC_BASE_DIR}/stellar-xdr-${RS_STELLAR_XDR_REVISION}"
      # The XDR definitions are a pinned commit of stellar/stellar-xdr. Up to
      # stellar-xdr v26 that commit lived in xdr/curr-version; from v27 the xdr
      # definitions became a git submodule and the commit is recorded in the
      # top-level xdr-version file instead.
      if [ -f "${CRATE_DIR}/xdr-version" ]; then
        STELLAR_XDR_REVISION_FROM_RUST=$(cat "${CRATE_DIR}/xdr-version")
      else
        STELLAR_XDR_REVISION_FROM_RUST=$(cat "${CRATE_DIR}/xdr/curr-version")
      fi
    fi
  else
    echo "Could not determine the rs-stellar-xdr version used by soroban-env-host@$PROTO"
    echo
    echo
    echo
    echo "Full error:"
    echo $CARGO_OUTPUT
  fi
done

# Now, lets compare the Rust and Go XDR revisions

# revision of https://github.com/stellar/stellar-xdr/ used by the Go code, read
# from the go-stellar-sdk module the build actually uses. This honors a replace
# directive (e.g. an SDK fork pinned while a protocol change is in review) and
# works for tagged versions as well as pseudo-versions.
go mod download github.com/stellar/go-stellar-sdk
GO_SDK_DIR=$(go list -m -f '{{.Dir}}' github.com/stellar/go-stellar-sdk)
STELLAR_XDR_REVISION_FROM_GO=$(cat "${GO_SDK_DIR}/xdr/xdr_commit_generated.txt")

if [ "$STELLAR_XDR_REVISION_FROM_GO" != "$STELLAR_XDR_REVISION_FROM_RUST" ]; then
  echo "Go and Rust dependencies are using different revisions of https://github.com/stellar/stellar-xdr"
  echo
  echo "Rust dependencies are using commit $STELLAR_XDR_REVISION_FROM_RUST"
  echo "Go dependencies are using commit $STELLAR_XDR_REVISION_FROM_GO"
  exit 1
fi

# Now, lets make sure that the core and captive core version used in the tests use the same version and that they depend
# on the same XDR revision

# Extract (protocol_version, core_version) pairs from the packaged-core integration jobs in
# stellar-rpc.yml. Each pkg job has a with: block containing protocol_version followed by
# core_version; source jobs have core_git_ref instead and are skipped.
PROTO_VERSION_PAIRS=$(awk "
  /^[[:space:]]*#/ { next }
  /protocol_version:/ { p = \$NF; gsub(/'/, \"\", p) }
  /core_deb_version:/     { c = \$NF; gsub(/'/, \"\", c); if (c != \"\") print p, c }
" .github/workflows/stellar-rpc.yml)

if [ -z "$PROTO_VERSION_PAIRS" ]; then
  echo "Could not find any packaged-core integration jobs in stellar-rpc.yml"
  exit 1
fi

PROTOCOL_VERSIONS=$(echo "$PROTO_VERSION_PAIRS" | awk '{print $1}')
MAX_PROTO=$(echo "$PROTOCOL_VERSIONS" | sort -n | tail -n1)

while IFS=' ' read -r P CORE_VERSION; do
    CORE_CONTAINER_REVISION=$(echo "$CORE_VERSION" | $SED -n 's/.*\.\([a-zA-Z0-9]*\)\..*/\1/p')
    if [ -z "$CORE_CONTAINER_REVISION" ]; then
        echo "Could not extract core commit revision from core_version '$CORE_VERSION' for protocol $P in stellar-rpc.yml"
        exit 1
    fi

    # Revision of https://github.com/stellar/rs-stellar-xdr by Core.
    # We obtain it from src/rust/src/host-dep-tree-curr.txt but Alternatively/in addition we could:
    #  * Check the rs-stellar-xdr revision of host-dep-tree-prev.txt
    #  * Check the stellar-xdr revision

    CORE_HOST_DEP_TREE_CURR=$($CURL https://raw.githubusercontent.com/stellar/stellar-core/${CORE_CONTAINER_REVISION}/src/rust/src/dep-trees/p${P}-expect.txt)
    RS_STELLAR_XDR_REVISION_FROM_CORE=$(echo "$CORE_HOST_DEP_TREE_CURR" | stellar_xdr_version_from_rust_dep_tree)
    # Compare against this repository's host for the same protocol.
    RS_STELLAR_XDR_REVISION_VAR="RS_STELLAR_XDR_REVISION_P${P}"
    RS_STELLAR_XDR_REVISION_FOR_P=${!RS_STELLAR_XDR_REVISION_VAR:-$RS_STELLAR_XDR_REVISION}
    if [ "$RS_STELLAR_XDR_REVISION_FOR_P" != "$RS_STELLAR_XDR_REVISION_FROM_CORE" ]; then
	    echo "The Core revision used in protocol $P integration tests (${CORE_CONTAINER_REVISION}) uses a different revision of https://github.com/stellar/rs-stellar-xdr"
	    echo
	    echo "Current repository's revision $RS_STELLAR_XDR_REVISION_FOR_P"
	    echo "Core's revision $RS_STELLAR_XDR_REVISION_FROM_CORE"
    fi
done <<< "$PROTO_VERSION_PAIRS"
