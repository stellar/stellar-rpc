#!/usr/bin/env bash
# Smoke test for the full-history v2 service. Curls every served JSON-RPC
# endpoint plus one stub and the admin listener, checking each response
# against the service's pinned wire contracts. Exits non-zero if any check
# fails.
#
# Run against a healthy (post-backfill) daemon for full coverage. Run during
# backfill it instead verifies the gated contracts (gated error, gate-exempt
# getHealth/metrics/stubs) and exits 0, telling you to rerun later.
#
#   ./smoke.sh
#   RPC_URL=http://devbox:8000 ADMIN_URL=http://devbox:8001 ./smoke.sh
set -uo pipefail

RPC_URL="${RPC_URL:-http://localhost:8000}"
ADMIN_URL="${ADMIN_URL:-http://localhost:8001}"
PUBNET_PASSPHRASE="Public Global Stellar Network ; September 2015"
# ContractData ledger key of the XLM Stellar Asset Contract's instance on
# pubnet (exists from protocol 20 on, never archived). Built with the Go SDK:
#   id, _ := xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}.ContractID(PUBNET_PASSPHRASE)
#   key := xdr.LedgerKey{Type: xdr.LedgerEntryTypeContractData,
#     ContractData: &xdr.LedgerKeyContractData{
#       Contract:   xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &id},
#       Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
#       Durability: xdr.ContractDataDurabilityPersistent}}
#   base64(key.MarshalBinary())
XLM_SAC="CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
XLM_SAC_INSTANCE_KEY="AAAABgAAAAEltPzYWa7C+mNIQ4xImzw8EMmLbSG+T9PLMMtolT75dwAAABQAAAAB"

command -v jq >/dev/null || { echo "smoke.sh needs jq"; exit 2; }
command -v curl >/dev/null || { echo "smoke.sh needs curl"; exit 2; }

FAILS=0
ok()   { echo "ok:   $1"; }
fail() { echo "FAIL: $1"; FAILS=$((FAILS + 1)); }

# rpc <method> [params-json] -> raw response body on stdout. The params key is
# omitted entirely for parameterless methods (JSON-RPC 2.0 allows that; null is
# not equivalent for every server).
rpc() {
  local method=$1 body
  if [[ $# -ge 2 ]]; then
    body="{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$method\",\"params\":$2}"
  else
    body="{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$method\"}"
  fi
  curl -sS --max-time 30 "$RPC_URL" -H 'Content-Type: application/json' -d "$body"
}

echo "=== full-history v2 smoke against $RPC_URL (admin $ADMIN_URL) ==="

# --- getHealth decides which phase we're smoking ---
HEALTH=$(rpc getHealth) || { echo "FAIL: getHealth unreachable"; exit 1; }
STATUS=$(jq -r '.result.status // empty' <<<"$HEALTH")

# Transient on earliest_ledger="now" deployments: backfill had nothing to do,
# the gate is already open, but live ingestion hasn't committed its first
# ledger, so there is no served range yet.
if grep -q 'data stores are not initialized' <<<"$HEALTH"; then
  ok "getHealth: gate open, waiting for the first live-ingested ledger"
  echo "=== no served range yet — rerun in a minute once live ingestion commits. ==="
  exit 0
fi

if [[ "$STATUS" == "backfill in progress" ]]; then
  ok "getHealth reports \"backfill in progress\" (success response, gate closed)"

  BODY=$(rpc getLedgers '{"pagination":{"limit":1}}')
  CODE=$(jq -r '.error.code // empty' <<<"$BODY")
  MSG=$(jq -r '.error.message // empty' <<<"$BODY")
  if [[ "$CODE" == "-32603" && "$MSG" == "backfill in progress; query serving not started" ]]; then
    ok "gated method returns the exact gated error"
  else
    fail "gated getLedgers: want -32603 'backfill in progress; query serving not started', got: $BODY"
  fi

  BODY=$(rpc metrics)
  if jq -e '.result.ingestion' >/dev/null <<<"$BODY"; then
    ok "metrics answers during backfill (gate-exempt)"
  else
    fail "metrics during backfill: $BODY"
  fi

  BODY=$(rpc getFeeStats)
  CODE=$(jq -r '.error.code // empty' <<<"$BODY")
  if [[ "$CODE" == "-32601" ]]; then
    ok "stub getFeeStats answers -32601 during backfill (gate-exempt)"
  else
    fail "stub getFeeStats during backfill: $BODY"
  fi

  echo "=== backfill phase: gated contracts verified ($FAILS failures). Rerun once getHealth says \"healthy\" for full endpoint coverage. ==="
  exit $((FAILS > 0 ? 1 : 0))
fi

if [[ "$STATUS" != "healthy" ]]; then
  fail "getHealth: want status healthy (or backfill in progress), got: $HEALTH"
  echo "=== $FAILS failure(s) ==="
  exit 1
fi

LATEST=$(jq -r '.result.latestLedger' <<<"$HEALTH")
OLDEST=$(jq -r '.result.oldestLedger' <<<"$HEALTH")
WINDOW=$(jq -r '.result.ledgerRetentionWindow' <<<"$HEALTH")
if [[ "$LATEST" =~ ^[0-9]+$ && "$OLDEST" =~ ^[0-9]+$ && "$LATEST" -ge "$OLDEST" && "$WINDOW" =~ ^[0-9]+$ ]]; then
  ok "getHealth healthy: oldest=$OLDEST latest=$LATEST window=$WINDOW"
else
  fail "getHealth healthy but range looks wrong: $HEALTH"
  echo "=== cannot continue without a sane range ==="
  exit 1
fi
# Recent 100-ledger window (clamped to the served floor) for tx/event reads.
RECENT=$((LATEST - 99)); ((RECENT < OLDEST)) && RECENT=$OLDEST

# --- metrics ---
BODY=$(rpc metrics)
E2E_COUNT=$(jq -r '.result.ingestion["ingest.e2e"].count // 0' <<<"$BODY")
if [[ "$E2E_COUNT" -gt 0 ]] && jq -e '.result.rpc' >/dev/null <<<"$BODY"; then
  ok "metrics: ingest.e2e count=$E2E_COUNT, rpc section present"
else
  fail "metrics: want ingest.e2e count > 0 and an rpc section, got: $(jq -c '.result | keys' <<<"$BODY" 2>/dev/null || echo "$BODY")"
fi

# --- getLatestLedger ---
BODY=$(rpc getLatestLedger)
SEQ=$(jq -r '.result.sequence // 0' <<<"$BODY")
if [[ "$SEQ" -ge "$LATEST" ]]; then
  ok "getLatestLedger: sequence=$SEQ"
else
  fail "getLatestLedger: want sequence >= $LATEST, got: $BODY"
fi

# --- getNetwork ---
BODY=$(rpc getNetwork)
PASS=$(jq -r '.result.passphrase // empty' <<<"$BODY")
if [[ "$PASS" == "$PUBNET_PASSPHRASE" ]]; then
  ok "getNetwork echoes the pubnet passphrase"
else
  fail "getNetwork: want pubnet passphrase, got: $BODY"
fi

# --- getVersionInfo ---
BODY=$(rpc getVersionInfo)
VER=$(jq -r '.result.version // empty' <<<"$BODY")
if [[ -n "$VER" ]]; then
  ok "getVersionInfo: version=$VER"
else
  fail "getVersionInfo: $BODY"
fi

# --- getLedgers page 1 (from the served floor) + cursor page 2 ---
BODY=$(rpc getLedgers "{\"startLedger\":$OLDEST,\"pagination\":{\"limit\":5}}")
N=$(jq -r '.result.ledgers | length' <<<"$BODY" 2>/dev/null || echo 0)
CURSOR=$(jq -r '.result.cursor // empty' <<<"$BODY")
FIRST=$(jq -r '.result.ledgers[0].sequence // 0' <<<"$BODY")
if [[ "$N" == 5 && "$FIRST" == "$OLDEST" && -n "$CURSOR" ]]; then
  ok "getLedgers: 5 ledgers from oldest=$OLDEST, cursor=$CURSOR"
else
  fail "getLedgers page 1: want 5 ledgers starting at $OLDEST + cursor, got: $(jq -c '{n: (.result.ledgers|length), first: .result.ledgers[0].sequence, cursor: .result.cursor, error: .error}' <<<"$BODY" 2>/dev/null || echo "$BODY")"
fi
if [[ -n "$CURSOR" ]]; then
  BODY=$(rpc getLedgers "{\"pagination\":{\"cursor\":\"$CURSOR\",\"limit\":5}}")
  FIRST2=$(jq -r '.result.ledgers[0].sequence // 0' <<<"$BODY")
  if [[ "$FIRST2" == $((OLDEST + 5)) ]]; then
    ok "getLedgers page 2 via cursor: starts at $FIRST2"
  else
    fail "getLedgers page 2: want first sequence $((OLDEST + 5)), got: $(jq -c '{first: .result.ledgers[0].sequence, error: .error}' <<<"$BODY" 2>/dev/null || echo "$BODY")"
  fi
fi

# --- getTransactions (recent window) + getTransaction (hash from it) ---
BODY=$(rpc getTransactions "{\"startLedger\":$RECENT,\"pagination\":{\"limit\":5}}")
NTX=$(jq -r '.result.transactions | length' <<<"$BODY" 2>/dev/null || echo 0)
TXHASH=$(jq -r '.result.transactions[0].txHash // empty' <<<"$BODY")
if [[ "$NTX" -gt 0 && -n "$TXHASH" ]]; then
  ok "getTransactions: $NTX txs from ledger $RECENT, first hash $TXHASH"
else
  fail "getTransactions: want >0 txs in [$RECENT, $LATEST] on pubnet, got: $(jq -c '{n: (.result.transactions|length), error: .error}' <<<"$BODY" 2>/dev/null || echo "$BODY")"
fi
if [[ -n "$TXHASH" ]]; then
  BODY=$(rpc getTransaction "{\"hash\":\"$TXHASH\"}")
  TXSTATUS=$(jq -r '.result.status // empty' <<<"$BODY")
  if [[ "$TXSTATUS" == "SUCCESS" || "$TXSTATUS" == "FAILED" ]]; then
    ok "getTransaction($TXHASH): status=$TXSTATUS"
  else
    fail "getTransaction: want SUCCESS/FAILED for a hash just served, got: $BODY"
  fi
fi

# --- getEvents: no filter, then a contract filter; the deprecated
# --- inSuccessfulContractCall key must be absent from the raw wire bytes ---
BODY=$(rpc getEvents "{\"startLedger\":$RECENT,\"pagination\":{\"limit\":10}}")
NEV=$(jq -r '.result.events | length' <<<"$BODY" 2>/dev/null || echo 0)
if [[ "$NEV" -gt 0 ]]; then
  ok "getEvents (no filter): $NEV events from ledger $RECENT"
else
  fail "getEvents: want >0 events in [$RECENT, $LATEST] on pubnet, got: $(jq -c '{n: (.result.events|length), error: .error}' <<<"$BODY" 2>/dev/null || echo "$BODY")"
fi
if grep -q 'inSuccessfulContractCall' <<<"$BODY"; then
  fail "getEvents raw body contains the dropped v1 key inSuccessfulContractCall"
else
  ok "getEvents raw body has no inSuccessfulContractCall key"
fi
BODY=$(rpc getEvents "{\"startLedger\":$RECENT,\"filters\":[{\"type\":\"contract\",\"contractIds\":[\"$XLM_SAC\"]}],\"pagination\":{\"limit\":10}}")
if jq -e '.result.events | type == "array"' >/dev/null <<<"$BODY" 2>&1; then
  ok "getEvents (XLM SAC contract filter): $(jq -r '.result.events | length' <<<"$BODY") events"
else
  fail "getEvents with contract filter: $BODY"
fi

# --- getLedgerEntries (XLM SAC instance — exists on pubnet from protocol 20 on) ---
BODY=$(rpc getLedgerEntries "{\"keys\":[\"$XLM_SAC_INSTANCE_KEY\"]}")
NENT=$(jq -r '.result.entries | length' <<<"$BODY" 2>/dev/null || echo 0)
if [[ "$NENT" == 1 ]]; then
  ok "getLedgerEntries: XLM SAC instance entry found (latestLedger=$(jq -r '.result.latestLedger' <<<"$BODY"))"
else
  fail "getLedgerEntries: want the XLM SAC instance entry, got: $BODY"
fi

# --- one stub: getFeeStats must answer the exact unsupported error ---
BODY=$(rpc getFeeStats)
CODE=$(jq -r '.error.code // empty' <<<"$BODY")
MSG=$(jq -r '.error.message // empty' <<<"$BODY")
if [[ "$CODE" == "-32601" && "$MSG" == 'method "getFeeStats" is not supported by the full-history service' ]]; then
  ok "stub getFeeStats: exact -32601 unsupported error"
else
  fail "stub getFeeStats: want -32601 'method \"getFeeStats\" is not supported by the full-history service', got: $BODY"
fi

# --- admin listener ---
if curl -sS --max-time 10 "$ADMIN_URL/metrics" | grep -q '^# HELP'; then
  ok "admin /metrics serves Prometheus text"
else
  fail "admin $ADMIN_URL/metrics"
fi
if curl -sS --max-time 10 "$ADMIN_URL/latency.json" | jq -e 'type == "object"' >/dev/null; then
  ok "admin /latency.json serves JSON"
else
  fail "admin $ADMIN_URL/latency.json"
fi

echo "=== done: $FAILS failure(s) ==="
exit $((FAILS > 0 ? 1 : 0))
