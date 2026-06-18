#!/usr/bin/env bash
# Monthly synthetic-corpus refresh: regenerate each scenario bundle with
# stellar-core apply-load, verify profiles and cross-bundle tx-hash
# disjointness, and (with UPLOAD=1) replace the bundles in S3.
#
# Requirements: a BUILD_TESTS stellar-core (CORE_BIN), go, zstd, awk, and for
# uploads AWS credentials that can write to $BUCKET.
#
#   CORE_BIN=/path/to/stellar-core ./refresh-bundles.sh           # dry run
#   CORE_BIN=... UPLOAD=1 ./refresh-bundles.sh                    # and upload
#   CORE_BIN=... SCENARIOS="sac" ./refresh-bundles.sh             # one scenario
#
# After uploading, update the repo configs if APPLY_LOAD_NUM_LEDGERS changed,
# run the trimmed local e2e (see README.md), and push to trigger a CI run.
set -euo pipefail

SCENARIOS=(${SCENARIOS:-oz sac soroswap})
CORE_BIN="${CORE_BIN:?set CORE_BIN to a BUILD_TESTS stellar-core with apply-load}"
BUCKET="${BUCKET:-stellar-rpc-ci-load-test}"
PASSPHRASE="Apply Load"
UPLOAD="${UPLOAD:-0}"

cd "$(dirname "$0")"
TESTDATA="$(cd ../testdata && pwd)"
WORK="${WORK:-$PWD/_refresh-work}"

sha256() { shasum -a 256 "$1" 2>/dev/null | awk '{print $1}' || sha256sum "$1" | awk '{print $1}'; }

"$CORE_BIN" help | grep -q apply-load || { echo "FATAL: $CORE_BIN lacks apply-load (need BUILD_TESTS)"; exit 1; }
go build -o refresh-tool .

BUNDLES=()
for s in "${SCENARIOS[@]}"; do
  cfg="$TESTDATA/apply-load-v27-$s.cfg"
  n=$(awk -F' *= *' '/^APPLY_LOAD_NUM_LEDGERS/{print $2}' "$cfg")
  [ -n "$n" ] || { echo "FATAL: no APPLY_LOAD_NUM_LEDGERS in $cfg"; exit 1; }

  wd="$WORK/$s"
  rm -rf "$wd" && mkdir -p "$wd"
  cp "$cfg" "$wd/apply-load.cfg"

  echo ">>> [$s] running apply-load ($n benchmark ledgers; log: $wd/apply-load.log)"
  (cd "$wd" && "$CORE_BIN" apply-load --conf apply-load.cfg > apply-load.log 2>&1) \
    || { echo "FATAL: apply-load failed for $s:"; tail -20 "$wd/apply-load.log"; exit 1; }

  bundle="$WORK/load-test-ledgers-v27-$s.xdr.zstd"
  echo ">>> [$s] trimming last $n frames of meta.xdr -> $(basename "$bundle")"
  ./refresh-tool -trimlast "$n" "$wd/meta.xdr" | zstd -q -19 -T0 -f -o "$bundle"

  ./refresh-tool "$bundle" | tee "$wd/inspect.txt"
  grep -q "^ledgers=$n .*monotonic=true" "$wd/inspect.txt" \
    || { echo "FATAL: $s bundle does not contain exactly $n monotonic ledgers"; exit 1; }
  BUNDLES+=("$bundle")
done

echo ">>> verifying the bundles concatenate into one readable stream"
./refresh-tool -concat "${BUNDLES[@]}" > /dev/null
echo "concat OK"

echo ">>> verifying cross-bundle tx-hash disjointness (duplicates abort CI ingestion)"
./refresh-tool -dupes "$PASSPHRASE" "${BUNDLES[@]}"

if [ "$UPLOAD" = "1" ]; then
  for b in "${BUNDLES[@]}"; do
    key="ledgers/$(basename "$b")"
    sha=$(sha256 "$b")
    echo ">>> uploading $key (sha256-raw=$sha)"
    aws s3 cp "$b" "s3://$BUCKET/$key" --metadata "sha256-raw=$sha" --only-show-errors
    got=$(aws s3api head-object --bucket "$BUCKET" --key "$key" \
      --query 'Metadata."sha256-raw"' --output text)
    [ "$got" = "$sha" ] || { echo "FATAL: $key metadata mismatch after upload"; exit 1; }
  done
  echo "upload complete"
else
  echo "dry run complete; bundles in $WORK. Re-run with UPLOAD=1 to push to s3://$BUCKET/ledgers/"
fi
