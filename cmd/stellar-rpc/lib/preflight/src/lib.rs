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
}

#[path = "."]
mod prev {
    pub(crate) use soroban_env_host_prev as soroban_env_host;
    pub(crate) use soroban_simulation_prev as soroban_simulation;

    #[allow(clippy::duplicate_mod)]
    pub(crate) mod shared;

    pub(crate) const PROTOCOL: u32 = soroban_env_host::meta::INTERFACE_VERSION.protocol;
}

use std::cell::RefCell;
use std::mem;
use std::panic;
use std::ptr::null_mut;

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

impl Default for CXDRVector {
    fn default() -> Self {
        CXDRVector {
            array: null_mut(),
            len: 0,
        }
    }
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

impl Default for CXDRDiffVector {
    fn default() -> Self {
        CXDRDiffVector {
            array: null_mut(),
            len: 0,
        }
    }
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

impl Default for CPreflightResult {
    fn default() -> Self {
        Self {
            error: safe_cstring(String::new()).into_raw(),
            auth: CXDRVector::default(),
            result: CXDR::default(),
            transaction_data: CXDR::default(),
            min_fee: 0,
            events: CXDRVector::default(),
            cpu_instructions: 0,
            memory_bytes: 0,
            pre_restore_transaction_data: CXDR::default(),
            pre_restore_min_fee: 0,
            ledger_entry_diff: CXDRDiffVector::default(),
        }
    }
}

impl CPreflightResult {
    /// Safely sets the error on the internal structure. By default, error
    /// is an empty string, so setting it with
    ///
    ///     { error: message, ..Default::default() }
    ///
    /// syntax causes a memory leak. This method ensures memory is managed correctly.
    fn set_error(&mut self, error: String) {
        unsafe { free_c_string(self.error) };
        self.error = safe_cstring(error).into_raw();
    }
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
) -> *mut CPreflightResult {
    let proto = ledger_info.protocol_version;
    catch_preflight_panic(&move || {
        if proto <= prev::PROTOCOL {
            prev::shared::preflight_invoke_hf_op_or_maybe_panic(
                handle,
                invoke_hf_op,
                source_account,
                ledger_info,
                resource_config,
                enable_debug,
                auth_mode.into(),
            )
        } else if proto == curr::PROTOCOL {
            curr::shared::preflight_invoke_hf_op_or_maybe_panic(
                handle,
                invoke_hf_op,
                source_account,
                ledger_info,
                resource_config,
                enable_debug,
                auth_mode.into(),
            )
        } else {
            bail!("unsupported protocol version: {proto}")
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
        if proto <= prev::PROTOCOL {
            prev::shared::preflight_footprint_ttl_op_or_maybe_panic(
                handle,
                op_body,
                footprint,
                ledger_info,
            )
        } else if proto == curr::PROTOCOL {
            curr::shared::preflight_footprint_ttl_op_or_maybe_panic(
                handle,
                op_body,
                footprint,
                ledger_info,
            )
        } else {
            bail!("unsupported protocol version: {proto}")
        }
    })
}

fn preflight_error(str: String) -> CPreflightResult {
    let mut result = CPreflightResult::default();
    result.set_error(str);
    result
}

// Safety: CPreflightResult is constructed on the worker thread and returned
// via join(). All raw pointers it contains are Rust-allocated and ownership
// is transferred to the caller. No concurrent access occurs.
unsafe impl Send for CPreflightResult {}

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
fn catch_preflight_panic(op: &dyn Fn() -> Result<CPreflightResult>) -> *mut CPreflightResult {
    // Wrap the reference in AssertSend so it can be moved into the scoped
    // thread. Soundness argument is in the `AssertSend` doc comment above.
    let op = AssertSend(op);
    let c_preflight_result = std::thread::scope(|s| {
        let join_handle = std::thread::Builder::new()
            .name("preflight-worker".into())
            .stack_size(SIMULATION_THREAD_STACK_SIZE)
            .spawn_scoped(s, move || {
                // catch panics before they reach foreign callers (which otherwise would result in
                // undefined behavior)
                let res: std::thread::Result<Result<CPreflightResult>> =
                    panic::catch_unwind(panic::AssertUnwindSafe(op.into_inner()));
                match res {
                    Err(panic) => match panic.downcast::<String>() {
                        Ok(panic_msg) => {
                            preflight_error(format!("panic during preflight() call: {panic_msg}"))
                        }
                        Err(_) => preflight_error(
                            "panic during preflight() call: unknown cause".to_string(),
                        ),
                    },
                    // See https://docs.rs/anyhow/latest/anyhow/struct.Error.html#display-representations
                    Ok(r) => r.unwrap_or_else(|e| preflight_error(format!("{e:?}"))),
                }
            });
        match join_handle {
            Ok(handle) => handle.join().unwrap_or_else(|_| {
                preflight_error("preflight worker thread panicked".to_string())
            }),
            Err(e) => preflight_error(format!("failed to spawn preflight worker thread: {e}")),
        }
    });
    // transfer ownership to caller
    // caller needs to invoke free_preflight_result(result) when done
    Box::into_raw(Box::new(c_preflight_result))
}

fn vec_to_c_array<T>(mut v: Vec<T>) -> (*mut T, libc::size_t) {
    // Make sure length and capacity are the same
    // (this allows using the length as the capacity when deallocating the vector)
    v.shrink_to_fit();
    let len = v.len();
    assert_eq!(len, v.capacity());

    // Get the pointer to our vector, we will deallocate it in free_c_null_terminated_char_array()
    // TODO: replace by `out_vec.into_raw_parts()` once the API stabilizes
    let ptr = v.as_mut_ptr();
    mem::forget(v);

    (ptr, len)
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
    unsafe {
        // We intentionally use len for both length and capacity because the
        // allocation was shrunk to fit (see vec_to_c_array).
        #[allow(clippy::same_length_and_capacity)]
        drop(Vec::from_raw_parts(xdr.xdr, xdr.len, xdr.len));
    }
}

fn free_c_xdr_array(xdr_array: CXDRVector) {
    if xdr_array.array.is_null() {
        return;
    }
    unsafe {
        #[allow(clippy::same_length_and_capacity)]
        let v = Vec::from_raw_parts(xdr_array.array, xdr_array.len, xdr_array.len);
        for xdr in v {
            free_c_xdr(xdr);
        }
    }
}

fn free_c_xdr_diff_array(xdr_array: CXDRDiffVector) {
    if xdr_array.array.is_null() {
        return;
    }
    unsafe {
        #[allow(clippy::same_length_and_capacity)]
        let v = Vec::from_raw_parts(xdr_array.array, xdr_array.len, xdr_array.len);
        for diff in v {
            free_c_xdr(diff.before);
            free_c_xdr(diff.after);
        }
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

struct GoLedgerStorage {
    golang_handle: libc::uintptr_t,
    internal_error: RefCell<Option<anyhow::Error>>,
}

impl GoLedgerStorage {
    fn new(golang_handle: libc::uintptr_t) -> Self {
        Self {
            golang_handle,
            internal_error: RefCell::new(None),
        }
    }

    // Get the entry XDR and TTL
    fn get_xdr_internal(&self, key_xdr: &mut Vec<u8>) -> Option<(Vec<u8>, Option<u32>)> {
        let key_c_xdr = CXDR {
            xdr: key_xdr.as_mut_ptr(),
            len: key_xdr.len(),
        };
        let res = unsafe { SnapshotSourceGet(self.golang_handle, key_c_xdr) };
        if res.entry.xdr.is_null() {
            return None;
        }
        let v = unsafe { from_c_xdr(res.entry) };
        unsafe { FreeGoLedgerEntryAndTTL(res) };
        if res.ttl < 0 {
            Some((v, None))
        } else {
            let ttl = u32::try_from(res.ttl).ok()?;
            Some((v, Some(ttl)))
        }
    }
}

fn extract_error_string<T>(simulation_result: &Result<T>, go_storage: &GoLedgerStorage) -> String {
    match simulation_result {
        Ok(_) => String::new(),
        Err(e) => {
            // Override any simulation result with a storage error (if any). Simulation does not propagate the storage
            // errors, but these provide more exact information on the root cause.
            if let Some(e) = go_storage.internal_error.borrow().as_ref() {
                format!("{e:?}")
            } else {
                format!("{e:?}")
            }
        }
    }
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
            Ok(CPreflightResult::default())
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
}
