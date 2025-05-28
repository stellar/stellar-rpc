// NOTE: You could use https://michael-f-bryan.github.io/rust-ffi-guide/cbindgen.html to generate
// this header automatically from your Rust code.  But for now, we'll just write it by hand.

#include <stdint.h>
#include <stdbool.h>
#include "shared.h"

typedef struct ledger_info_t {
  uint32_t protocol_version;
  uint32_t sequence_number;
  uint64_t timestamp;
  const char *network_passphrase;
  uint32_t base_reserve;
  uint64_t bucket_list_size;
} ledger_info_t;

typedef struct xdr_vector_t {
    xdr_t  *array;
    size_t len;
} xdr_vector_t;

typedef struct xdr_diff_t {
    xdr_t before;
    xdr_t after;
} xdr_diff_t;

typedef struct xdr_diff_vector_t {
    xdr_diff_t  *array;
    size_t len;
} xdr_diff_vector_t;

typedef struct resource_config_t {
    uint64_t instruction_leeway; // Allow this many extra instructions when budgeting
} resource_config_t;

typedef struct preflight_result_t {
    char             *error; // Error string in case of error, otherwise null
    xdr_vector_t      auth; // array of SorobanAuthorizationEntries
    xdr_t             result; // XDR SCVal
    xdr_t             transaction_data;
    int64_t           min_fee; // Minimum recommended resource fee
    xdr_vector_t      events; // array of XDR DiagnosticEvents
    uint64_t          cpu_instructions;
    uint64_t          memory_bytes;
    xdr_t             pre_restore_transaction_data; // SorobanTransactionData XDR for a prerequired RestoreFootprint operation
    int64_t           pre_restore_min_fee; // Minimum recommended resource fee for a prerequired RestoreFootprint operation
    xdr_diff_vector_t ledger_entry_diff; // Contains the ledger entry changes which would be caused by the transaction execution
} preflight_result_t;

preflight_result_t *preflight_invoke_hf_op(uintptr_t handle, // Go Handle to forward to SnapshotSourceGet
                                           const xdr_t invoke_hf_op, // InvokeHostFunctionOp XDR
                                           const xdr_t source_account, // AccountId XDR
                                           const ledger_info_t ledger_info,
                                           const resource_config_t resource_config,
                                           bool enable_debug,
                                           const uint32_t auth_mode);

preflight_result_t *preflight_footprint_ttl_op(uintptr_t   handle, // Go Handle to forward to SnapshotSourceGet
                                               const xdr_t op_body, // OperationBody XDR
                                               const xdr_t footprint, // LedgerFootprint XDR
                                               const ledger_info_t ledger_info);


// LedgerKey XDR to LedgerEntry XDR AND TTL
typedef struct ledger_entry_and_ttl_t {
    xdr_t entry;
    int64_t ttl; // TTL missing if -1
} ledger_entry_and_ttl_t;
extern ledger_entry_and_ttl_t SnapshotSourceGet(uintptr_t handle, xdr_t ledger_key);

void free_preflight_result(preflight_result_t *result);

extern void FreeGoLedgerEntryAndTTL(ledger_entry_and_ttl_t ledger_entry_and_ttl);
