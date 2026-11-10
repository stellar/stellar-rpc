extern crate anyhow;
extern crate base64;
extern crate ffi;
extern crate libc;
extern crate serde_json;
extern crate sha2;

pub(crate) use anyhow::{anyhow, bail, Result};
pub(crate) use sha2::{Digest, Sha256};

// We really do need everything.
#[allow(clippy::wildcard_imports)]
use ffi::*;
extern crate soroban_env_host_curr;
extern crate soroban_env_host_prev;
extern crate soroban_simulation_curr;
extern crate soroban_simulation_prev;

// We support two different versions of soroban simultaneously, switching on the
// protocol version each supports. This is the exact same mechanism we use in
// stellar-core to switch soroban hosts on protocol boundaries, and allows
// synchronously cutting over between significantly different versions of the
// host (or VM) without having to do fine-grained versioning within the VM.
//
// The way it is _accomplished_ is by mounting the same adaptor code (in
// `shared.rs`) at two different paths in the module tree, and then providing
// each with a different binding for the soroban host and simulation code. Any
// function that mentions a type from the soroban host or simulation code must
// be placed in the `shared.rs` file. Code that is host-version-agnostic can
// continue to live in this file.
//
// This is a bit of a hack, but it works well enough for our purposes and works
// around the absence of parametric modules in the Rust language.

#[path = "."]
mod curr {
    pub(crate) use soroban_env_host_curr as soroban_env_host;
    pub(crate) use soroban_simulation_curr as soroban_simulation;

    #[allow(clippy::duplicate_mod)]
    pub(crate) mod shared;

    pub(crate) const PROTOCOL: u32 = soroban_env_host::meta::INTERFACE_VERSION.protocol;

    // Constructs the recording auth mode for this protocol version. The current
    // protocol's soroban-env can emit either v1 (`Address`) or v2 (`AddressV2`)
    // credentials, selected by `use_upgraded_auth`.
    pub(crate) fn make_recording_auth_mode(
        disable_non_root_auth: bool,
        use_upgraded_auth: bool,
    ) -> soroban_env_host::e2e_invoke::RecordingInvocationAuthMode {
        soroban_env_host::e2e_invoke::RecordingInvocationAuthMode::recording(
            disable_non_root_auth,
            use_upgraded_auth,
        )
    }
}

#[path = "."]
mod prev {
    pub(crate) use soroban_env_host_prev as soroban_env_host;
    pub(crate) use soroban_simulation_prev as soroban_simulation;

    #[allow(clippy::duplicate_mod)]
    pub(crate) mod shared;

    pub(crate) const PROTOCOL: u32 = soroban_env_host::meta::INTERFACE_VERSION.protocol;

    // See the matching `curr::make_recording_auth_mode`. Every `-prev` host
    // since 27.1.0 supports v2 (`AddressV2`) credentials, so `use_upgraded_auth`
    // passes through, matching simulateTransaction on the previous protocol.
    pub(crate) fn make_recording_auth_mode(
        disable_non_root_auth: bool,
        use_upgraded_auth: bool,
    ) -> soroban_env_host::e2e_invoke::RecordingInvocationAuthMode {
        soroban_env_host::e2e_invoke::RecordingInvocationAuthMode::recording(
            disable_non_root_auth,
            use_upgraded_auth,
        )
    }
}

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::panic;
use std::ptr::{null_mut, slice_from_raw_parts_mut};
use std::sync::OnceLock;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct CLedgerInfo {
    pub protocol_version: u32,
    pub sequence_number: u32,
    pub timestamp: u64,
    pub network_passphrase: *const libc::c_char,
    pub base_reserve: u32,
    pub bucket_list_size: u64,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct CXDRVector {
    pub array: *mut CXDR,
    pub len: libc::size_t,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct CXDRDiff {
    pub before: CXDR,
    pub after: CXDR,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct CXDRDiffVector {
    pub array: *mut CXDRDiff,
    pub len: libc::size_t,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct CResourceConfig {
    pub instruction_leeway: u64,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct CPreflightResult {
    // Error string in case of error, otherwise an empty string
    pub error: *mut libc::c_char,
    // XDR ContractAuth array
    pub auth: CXDRVector,
    // XDR SCVal
    pub result: CXDR,
    // SorobanTransactionData XDR
    pub transaction_data: CXDR,
    // Minimum recommended resource fee
    pub min_fee: i64,
    // array of XDR ContractEvents
    pub events: CXDRVector,
    pub cpu_instructions: u64,
    pub memory_bytes: u64,
    // SorobanTransactionData XDR for a prerequired RestoreFootprint operation
    pub pre_restore_transaction_data: CXDR,
    // Minimum recommended resource fee for a prerequired RestoreFootprint operation
    pub pre_restore_min_fee: i64,
    // Contains the ledger entry changes which would be caused by the transaction execution
    pub ledger_entry_diff: CXDRDiffVector,
}

/// The most XDR bytes a preflight result may carry back to Go, summed over all
/// of its fields.
const MAX_RESULT_BYTES: usize = 16 * 1024 * 1024;

/// The XDR of a ledger entry before and after the simulation changed it.
type EntryDiff = (Option<Vec<u8>>, Option<Vec<u8>>);

/// A preflight result while it is still owned by Rust. It is converted to a
/// `CPreflightResult` only once complete, so a failure part-way through
/// building it leaks nothing.
#[derive(Default)]
struct PreflightOutput {
    error: String,
    auth: Vec<Vec<u8>>,
    result: Option<Vec<u8>>,
    transaction_data: Option<Vec<u8>>,
    min_fee: i64,
    events: Vec<Vec<u8>>,
    cpu_instructions: u64,
    memory_bytes: u64,
    pre_restore_transaction_data: Option<Vec<u8>>,
    pre_restore_min_fee: i64,
    ledger_entry_diff: Vec<EntryDiff>,
}

impl PreflightOutput {
    fn error(error: String) -> Self {
        Self {
            error,
            ..Default::default()
        }
    }

    // TODO: We could use something like https://github.com/sonos/ffi-convert-rs
    //       to replace all the free_* , *_to_c and from_c_* functions by implementations of CDrop,
    //       CReprOf and AsRust
    fn into_c(self) -> CPreflightResult {
        let diffs = self
            .ledger_entry_diff
            .into_iter()
            .map(|(before, after)| CXDRDiff {
                before: option_bytes_to_c(before),
                after: option_bytes_to_c(after),
            })
            .collect();
        let (diff_array, diff_len) = vec_to_c_array(diffs);
        CPreflightResult {
            error: string_to_c(self.error),
            auth: bytes_vec_to_c(self.auth),
            result: option_bytes_to_c(self.result),
            transaction_data: option_bytes_to_c(self.transaction_data),
            min_fee: self.min_fee,
            events: bytes_vec_to_c(self.events),
            cpu_instructions: self.cpu_instructions,
            memory_bytes: self.memory_bytes,
            pre_restore_transaction_data: option_bytes_to_c(self.pre_restore_transaction_data),
            pre_restore_min_fee: self.pre_restore_min_fee,
            ledger_entry_diff: CXDRDiffVector {
                array: diff_array,
                len: diff_len,
            },
        }
    }
}

fn bytes_to_c(v: Vec<u8>) -> CXDR {
    let (xdr, len) = vec_to_c_array(v);
    CXDR { xdr, len }
}

fn option_bytes_to_c(v: Option<Vec<u8>>) -> CXDR {
    v.map_or_else(CXDR::default, bytes_to_c)
}

fn bytes_vec_to_c(v: Vec<Vec<u8>>) -> CXDRVector {
    let (array, len) = vec_to_c_array(v.into_iter().map(bytes_to_c).collect());
    CXDRVector { array, len }
}

// Each host runs ledgers on its own protocol only, and fails any other protocol
// with an opaque `Error(Context, InternalError)`. So the dispatch only hands a
// host its own protocol, and rejects any other protocol with an error that says
// why (see https://github.com/stellar/stellar-rpc/issues/1163). The exception is
// a curr host built with `next`, which also runs its base protocol, so curr
// covers every protocol after prev up to its own.
fn unsupported_protocol_message(proto: u32) -> String {
    format!(
        "cannot simulate transaction: the network is on protocol {proto}, but this \
         stellar-rpc only supports protocols {} and {}; run a stellar-rpc release \
         that supports protocol {proto}",
        prev::PROTOCOL,
        curr::PROTOCOL,
    )
}

#[no_mangle]
pub extern "C" fn preflight_invoke_hf_op(
    handle: libc::uintptr_t, // Go Handle to forward to SnapshotSourceGet and SnapshotSourceHas
    invoke_hf_op: CXDR,      // InvokeHostFunctionOp XDR in base64
    source_account: CXDR,    // AccountId XDR in base64
    ledger_info: CLedgerInfo,
    resource_config: CResourceConfig,
    enable_debug: bool,
    auth_mode: u32,
    use_upgraded_auth: bool,
) -> *mut CPreflightResult {
    let proto = ledger_info.protocol_version;
    catch_preflight_panic(&move || {
        if proto == prev::PROTOCOL {
            prev::shared::preflight_invoke_hf_op_or_maybe_panic(
                handle,
                invoke_hf_op,
                source_account,
                ledger_info,
                resource_config,
                enable_debug,
                auth_mode.into(),
                use_upgraded_auth,
            )
        } else if (prev::PROTOCOL + 1..=curr::PROTOCOL).contains(&proto) {
            curr::shared::preflight_invoke_hf_op_or_maybe_panic(
                handle,
                invoke_hf_op,
                source_account,
                ledger_info,
                resource_config,
                enable_debug,
                auth_mode.into(),
                use_upgraded_auth,
            )
        } else {
            bail!(unsupported_protocol_message(proto))
        }
    })
}

#[no_mangle]
pub extern "C" fn preflight_footprint_ttl_op(
    handle: libc::uintptr_t, // Go Handle to forward to SnapshotSourceGet and SnapshotSourceHas
    op_body: CXDR,           // OperationBody XDR
    footprint: CXDR,         // LedgerFootprint XDR
    ledger_info: CLedgerInfo,
) -> *mut CPreflightResult {
    let proto = ledger_info.protocol_version;
    catch_preflight_panic(&move || {
        if proto == prev::PROTOCOL {
            prev::shared::preflight_footprint_ttl_op_or_maybe_panic(
                handle,
                op_body,
                footprint,
                ledger_info,
            )
        } else if (prev::PROTOCOL + 1..=curr::PROTOCOL).contains(&proto) {
            curr::shared::preflight_footprint_ttl_op_or_maybe_panic(
                handle,
                op_body,
                footprint,
                ledger_info,
            )
        } else {
            bail!(unsupported_protocol_message(proto))
        }
    })
}

/// Without a limit of its own, a preflight worker limits the heap of each
/// simulation to this many times the network's per-transaction memory limit.
const MEMORY_LIMIT_FACTOR: u64 = 8;

/// The heap limit a preflight worker asked for, or zero when each simulation
/// sets `MEMORY_LIMIT_FACTOR` times the network's per-transaction memory
/// limit. It is unset outside a worker, where nothing sets a limit.
static WORKER_MEMORY_LIMIT: OnceLock<u64> = OnceLock::new();

/// Starts counting the heap the Rust libraries hold in this process, and
/// limits it: to `bytes` from now on, or when `bytes` is zero, to 8 times the
/// per-transaction memory limit of the ledger each simulation reads. An
/// allocation past the limit ends the process with `exit_code`, so only a
/// preflight worker calls this, before it allocates anything.
#[no_mangle]
pub extern "C" fn preflight_limit_memory(bytes: u64, exit_code: libc::c_int) {
    ffi::memory::start_counting(exit_code);
    if bytes > 0 {
        ffi::memory::set_limit(usize::try_from(bytes).unwrap_or(usize::MAX));
    }
    let _ = WORKER_MEMORY_LIMIT.set(bytes);
}

/// The heap limit in force, or zero when there is none.
#[no_mangle]
pub extern "C" fn preflight_memory_limit() -> u64 {
    ffi::memory::limit().map_or(0, |limit| limit as u64)
}

/// Restarts the measurement `preflight_memory_peak` returns.
#[no_mangle]
pub extern "C" fn preflight_reset_memory_peak() {
    ffi::memory::reset_peak();
}

/// The most heap the Rust libraries held in this process since the last
/// `preflight_reset_memory_peak`.
#[no_mangle]
pub extern "C" fn preflight_memory_peak() -> u64 {
    ffi::memory::peak() as u64
}

/// In a preflight worker without a limit of its own, sets the heap limit of a
/// simulation from the per-transaction memory limit of the ledger it reads.
/// The limit holds until the next simulation sets it, so it also covers the
/// conversion of the result.
fn limit_memory(tx_memory_limit: u32) {
    if WORKER_MEMORY_LIMIT.get() == Some(&0) {
        let limit = MEMORY_LIMIT_FACTOR * u64::from(tx_memory_limit);
        ffi::memory::set_limit(usize::try_from(limit).unwrap_or(usize::MAX));
    }
}

/// A zero-cost wrapper that asserts the contained value is safe to send across
/// thread boundaries.
///
/// # Safety
/// The caller must guarantee that no concurrent accesses to the wrapped value
/// occur and that all raw pointers within remain valid for the thread's
/// lifetime. Both conditions are satisfied here because:
///   1. The `CGo` caller blocks until `catch_preflight_panic` returns.
///   2. `thread::scope` guarantees the worker thread is joined before the
///      scope (and thus `catch_preflight_panic`) exits.
struct AssertSend<T>(T);
unsafe impl<T> Send for AssertSend<T> {}
impl<T> AssertSend<T> {
    /// Unwrap the inner value, consuming `self`.
    ///
    /// Defined as a named method rather than a field access (`.0`) so that
    /// `move` closures which call this capture the whole `AssertSend<T>` --
    /// preserving the `Send` assertion -- rather than just the inner `T`
    /// (which would not be `Send`). Rust 2021 partial-capture rules only
    /// apply to field projections, not to method calls that consume `self`.
    fn into_inner(self) -> T {
        self.0
    }
}

/// Stack size for simulation worker threads: 100 MiB.
/// Soroban contract invocations can produce deep call stacks, so we need
/// substantially more stack space than the OS default (~8 MiB on Linux).
const SIMULATION_THREAD_STACK_SIZE: usize = 100 * 1024 * 1024;

/// Run a preflight operation on a dedicated worker thread with a large stack,
/// catching any panics and converting them to a `CPreflightResult` error.
///
/// We use `thread::scope` rather than `thread::spawn` so that the closure can
/// borrow from the caller's stack frame without requiring a `'static` lifetime.
/// The scope guarantees the thread is joined before we return, which (together
/// with the `CGo` caller blocking) keeps all captured raw pointers valid.
///
/// Go callbacks (`SnapshotSourceGet`, `FreeGoLedgerEntryAndTTL`) invoked from
/// the worker thread are handled by Go's `needm`/`dropm` mechanism, which is
/// the well-tested path for C->Go callbacks originating outside of a goroutine.
fn catch_preflight_panic(op: &dyn Fn() -> Result<PreflightOutput>) -> *mut CPreflightResult {
    // Wrap the reference in AssertSend so it can be moved into the scoped
    // thread. Soundness argument is in the `AssertSend` doc comment above.
    let op = AssertSend(op);
    let output = std::thread::scope(|s| {
        let join_handle = std::thread::Builder::new()
            .name("preflight-worker".into())
            .stack_size(SIMULATION_THREAD_STACK_SIZE)
            .spawn_scoped(s, move || {
                // catch panics before they reach foreign callers (which otherwise would result in
                // undefined behavior)
                let res: std::thread::Result<Result<PreflightOutput>> =
                    panic::catch_unwind(panic::AssertUnwindSafe(op.into_inner()));
                match res {
                    Err(panic) => match panic.downcast::<String>() {
                        Ok(panic_msg) => PreflightOutput::error(format!(
                            "panic during preflight() call: {panic_msg}"
                        )),
                        Err(_) => PreflightOutput::error(
                            "panic during preflight() call: unknown cause".to_string(),
                        ),
                    },
                    // See https://docs.rs/anyhow/latest/anyhow/struct.Error.html#display-representations
                    Ok(r) => r.unwrap_or_else(|e| PreflightOutput::error(format!("{e:#}"))),
                }
            });
        match join_handle {
            Ok(handle) => handle.join().unwrap_or_else(|_| {
                PreflightOutput::error("preflight worker thread panicked".to_string())
            }),
            Err(e) => {
                PreflightOutput::error(format!("failed to spawn preflight worker thread: {e}"))
            }
        }
    });
    // transfer ownership to caller
    // caller needs to invoke free_preflight_result(result) when done
    Box::into_raw(Box::new(output.into_c()))
}

fn vec_to_c_array<T>(v: Vec<T>) -> (*mut T, libc::size_t) {
    // Go throws if a goroutine stack it copies holds Rust's dangling
    // empty-slice pointer, so empty slices cross as null.
    if v.is_empty() {
        return (null_mut(), 0);
    }
    // The free_* functions rebuild the boxed slice from the pointer and length.
    let len = v.len();
    (Box::into_raw(v.into_boxed_slice()).cast::<T>(), len)
}

/// .
///
/// # Safety
///
/// .
#[no_mangle]
pub unsafe extern "C" fn free_preflight_result(result: *mut CPreflightResult) {
    if result.is_null() {
        return;
    }
    let boxed = Box::from_raw(result);
    free_c_string(boxed.error);
    free_c_xdr_array(boxed.auth);
    free_c_xdr(boxed.result);
    free_c_xdr(boxed.transaction_data);
    free_c_xdr_array(boxed.events);
    free_c_xdr(boxed.pre_restore_transaction_data);
    free_c_xdr_diff_array(boxed.ledger_entry_diff);
}

fn free_c_xdr(xdr: CXDR) {
    if xdr.xdr.is_null() {
        return;
    }
    unsafe { drop(Box::from_raw(slice_from_raw_parts_mut(xdr.xdr, xdr.len))) };
}

fn free_c_xdr_array(xdr_array: CXDRVector) {
    if xdr_array.array.is_null() {
        return;
    }
    let v = unsafe { Box::from_raw(slice_from_raw_parts_mut(xdr_array.array, xdr_array.len)) };
    for &xdr in &*v {
        free_c_xdr(xdr);
    }
}

fn free_c_xdr_diff_array(xdr_array: CXDRDiffVector) {
    if xdr_array.array.is_null() {
        return;
    }
    let v = unsafe { Box::from_raw(slice_from_raw_parts_mut(xdr_array.array, xdr_array.len)) };
    for diff in &*v {
        free_c_xdr(diff.before);
        free_c_xdr(diff.after);
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct CLedgerEntryAndTTL {
    pub entry: CXDR,
    pub ttl: i64, // -1 indicates that the TTL is missing
}

// Functions imported from Golang
extern "C" {
    // Free data returned from Go functions
    fn FreeGoLedgerEntryAndTTL(ledger_entry_and_ttl: CLedgerEntryAndTTL);
    // LedgerKey XDR to LedgerEntry XDR and TTL
    fn SnapshotSourceGet(handle: libc::uintptr_t, ledger_key: CXDR) -> CLedgerEntryAndTTL;
}

/// Ledger entries a simulation reads past the network config that are not in
/// its transaction's footprint: none, on either host. Every key the host
/// reads, during or after an invocation, is one it records in the footprint,
/// and the simulation re-reads only written ones. TTLs come back with their
/// entries, the source account is read only when the footprint holds it, and
/// extend, restore and their auto-restore read only the keys they are given.
const READS_OUTSIDE_FOOTPRINT: u32 = 0;

/// A ledger entry's XDR and live-until ledger, or `None` when the ledger has no
/// such entry.
type EntryXdr = Option<(Vec<u8>, Option<u32>)>;

struct GoLedgerStorage {
    golang_handle: libc::uintptr_t,
    internal_error: RefCell<Option<anyhow::Error>>,
    // The entries the simulation has read, missing ones included, by key XDR,
    // so that Go reads each entry once.
    entries: RefCell<HashMap<Vec<u8>, EntryXdr>>,
    // Once `limit_reads` sets it: how many entries had been read then, and the
    // most the simulation may read after.
    max_reads: Cell<Option<(usize, usize)>>,
}

impl GoLedgerStorage {
    fn new(golang_handle: libc::uintptr_t) -> Self {
        Self {
            golang_handle,
            internal_error: RefCell::new(None),
            entries: RefCell::default(),
            max_reads: Cell::new(None),
        }
    }

    /// From now on, fails a read of a new entry once the simulation has read
    /// as many as a transaction's footprint may hold.
    fn limit_reads(&self, tx_max_footprint_entries: u32) {
        let max = tx_max_footprint_entries.saturating_add(READS_OUTSIDE_FOOTPRINT);
        self.max_reads.set(Some((
            self.entries.borrow().len(),
            usize::try_from(max).unwrap_or(usize::MAX),
        )));
    }

    // Gets the entry XDR and TTL, from Go only the first time the simulation
    // reads the entry. Once `limit_reads` has run, it fails a new entry past
    // the limit.
    fn get_xdr_internal(&self, mut key_xdr: Vec<u8>) -> Result<EntryXdr> {
        let mut entries = self.entries.borrow_mut();
        if let Some(entry) = entries.get(&key_xdr) {
            return Ok(entry.clone());
        }
        if let Some((before, max)) = self.max_reads.get() {
            if entries.len() - before >= max {
                bail!("the simulation read more than {max} ledger entries, the most a transaction's footprint may hold");
            }
        }
        let key_c_xdr = CXDR {
            xdr: key_xdr.as_mut_ptr(),
            len: key_xdr.len(),
        };
        let res = unsafe { SnapshotSourceGet(self.golang_handle, key_c_xdr) };
        let entry = if res.entry.xdr.is_null() {
            None
        } else {
            let v = unsafe { from_c_xdr(res.entry) };
            unsafe { FreeGoLedgerEntryAndTTL(res) };
            if res.ttl < 0 {
                Some((v, None))
            } else {
                u32::try_from(res.ttl).ok().map(|ttl| (v, Some(ttl)))
            }
        };
        entries.insert(key_xdr, entry.clone());
        Ok(entry)
    }
}

fn extract_error_string<T>(simulation_result: &Result<T>, go_storage: &GoLedgerStorage) -> String {
    let Err(e) = simulation_result else {
        return String::new();
    };
    // Override any simulation result with a storage error (if any). Simulation does not propagate the storage
    // errors, but these provide more exact information on the root cause.
    let storage_error = go_storage.internal_error.borrow();
    format!("{:#}", storage_error.as_ref().unwrap_or(e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    /// Verify that `catch_preflight_panic` runs on a thread with enough stack
    /// space to survive deep recursion that would overflow a default-sized stack.
    #[test]
    fn deep_recursion_survives_on_simulation_thread() {
        #[inline(never)]
        fn recurse(n: u32) -> u32 {
            let buf = [0u8; 4096];
            let val = std::hint::black_box(&buf)[0] as u32;
            if n == 0 {
                return val;
            }
            val.wrapping_add(recurse(n - 1))
        }

        // 10_000 frames x 4 KiB ~= 40 MiB of stack
        let result = catch_preflight_panic(&|| {
            let _ = recurse(10_000);
            Ok(PreflightOutput::default())
        });

        unsafe {
            let error = CStr::from_ptr((*result).error);
            assert!(
                error.to_bytes().is_empty(),
                "expected no error, got: {:?}",
                error
            );
            free_preflight_result(result);
        }
    }

    #[test]
    fn empty_arrays_cross_as_null() {
        let result = PreflightOutput::default().into_c();
        assert!(result.auth.array.is_null());
        assert!(result.events.array.is_null());
        assert!(result.ledger_entry_diff.array.is_null());
        unsafe { free_preflight_result(Box::into_raw(Box::new(result))) };
    }
}
