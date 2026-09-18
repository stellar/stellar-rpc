extern crate anyhow;
extern crate ffi;
extern crate stellar_xdr;

use std::{panic, str::FromStr};
use stellar_xdr as xdr;
use stellar_xdr::WriteXdr;

use anyhow::{anyhow, Result};

// We really do need everything.
#[allow(clippy::wildcard_imports)]
use ffi::*;

// This is the same limit as the soroban serialization limit
// but we redefine it here for two reasons:
//
//   1. To depend only on the XDR crate, not the soroban host.
//   2. To allow customizing it here, since this function may
//      serialize many XDR types that are larger than the types
//      soroban allows serializing (eg. transaction sets or ledger
//      entries or whatever). Soroban is conservative and stops
//      at 32MiB.

const DEFAULT_XDR_RW_LIMITS: xdr::Limits = xdr::Limits {
    depth: 500,
    len: 32 * 1024 * 1024,
};

#[repr(C)]
pub struct ConversionResult {
    json: *mut libc::c_char,
    error: *mut libc::c_char,
}

#[repr(C)]
pub struct JsonToXdrResult {
    xdr: CXDR,
    error: *mut libc::c_char,
}

/// Takes in a string name of an XDR type in the Stellar Protocol (i.e. from the
/// `stellar_xdr` crate) as well as a raw byte structure and returns a structure
/// containing the JSON-ified string of the given structure.
///
/// # Errors
///
/// On error, the struct's `error` field will be filled out with the appropriate
/// message that caused the function to panic.
///
/// # Panics
///
/// This should never panic due to `catch_conversion_panic` catching and
/// unwinding all panics to stringified error messages.
///
/// # Safety
///
/// This relies on the function parameters to be valid structures. The
/// `typename` must be a null-terminated C string. The `xdr` structure should
/// have a valid pointer to an aligned byte array and have a matching size. If
/// these aren't true there may be segfaults when trying to manage their memory.
#[no_mangle]
pub unsafe extern "C" fn xdr_to_json(
    typename: *mut libc::c_char,
    xdr: CXDR,
) -> *mut ConversionResult {
    let result = catch_conversion_panic("xdr_to_json()", move || {
        let type_str = unsafe { from_c_string(typename) };
        let the_type = match xdr::TypeVariant::from_str(&type_str) {
            Ok(t) => t,
            Err(e) => panic!("couldn't match type {type_str}: {e}"),
        };

        let xdr_bytearray = unsafe { from_c_xdr(xdr) };
        let mut buffer = xdr::Limited::new(xdr_bytearray.as_slice(), DEFAULT_XDR_RW_LIMITS.clone());

        let t = match xdr::Type::read_xdr_to_end(the_type, &mut buffer) {
            Ok(t) => t,
            Err(e) => panic!("couldn't read {type_str}: {e}"),
        };

        Ok(serde_json::to_string(&t).unwrap())
    });

    let (json, error) = match result {
        Ok(json) => (json, String::new()),
        Err(error) => ("{}".to_string(), error),
    };

    // Caller is responsible for calling free_conversion_result.
    Box::into_raw(Box::new(ConversionResult {
        json: string_to_c(json),
        error: string_to_c(error),
    }))
}

/// Frees memory allocated for the corresponding conversion result.
///
/// # Safety
///
/// You should *only* use this to free the return value of `xdr_to_json`.
#[no_mangle]
pub unsafe extern "C" fn free_conversion_result(ptr: *mut ConversionResult) {
    if ptr.is_null() {
        return;
    }

    unsafe {
        free_c_string((*ptr).json);
        free_c_string((*ptr).error);
        drop(Box::from_raw(ptr));
    }
}

/// The inverse of `xdr_to_json`: takes in a string name of an XDR type in the
/// Stellar Protocol (i.e. from the `stellar_xdr` crate) as well as the JSON
/// serialization of a value of that type (the encoding `xdr_to_json` emits)
/// and returns a structure containing the value's XDR byte encoding.
///
/// # Errors
///
/// On error, the struct's `error` field will be filled out with the failure
/// message, and its `xdr` field is empty. Failures return errors rather than
/// panicking because this function parses user-supplied values. Inputs over
/// the 32 MiB limit are rejected before parsing (the Go wrapper enforces the
/// same bound first; this check covers direct FFI callers), and
/// `serde_json`'s recursion limit caps container nesting well below the 500
/// levels the XDR read direction allows (the Go tests pin that boundary).
///
/// # Safety
///
/// This relies on the function parameters to be valid structures. The
/// `typename` must be a null-terminated C string. The `json` structure should
/// have a valid pointer to an aligned byte array and have a matching size. If
/// these aren't true there may be segfaults when trying to manage their memory.
#[no_mangle]
pub unsafe extern "C" fn json_to_xdr(
    typename: *mut libc::c_char,
    json: CXDR,
) -> *mut JsonToXdrResult {
    let result = catch_conversion_panic("json_to_xdr()", move || {
        // The read direction bounds memory during decoding via Limited; the
        // parse direction can only bound it up front, before serde_json
        // materializes the whole value.
        if json.len > DEFAULT_XDR_RW_LIMITS.len {
            return Err(anyhow!(
                "JSON input is {} bytes, over the {}-byte limit",
                json.len,
                DEFAULT_XDR_RW_LIMITS.len
            ));
        }

        let type_str = unsafe { from_c_string(typename) };
        let the_type = xdr::TypeVariant::from_str(&type_str)
            .map_err(|e| anyhow!("couldn't match type {type_str}: {e}"))?;

        let json_bytearray = unsafe { from_c_xdr(json) };
        // Unknown fields inside nested structures are dropped, not rejected:
        // the crate's collecting variant allocates a path string per ignored
        // field (a large input-to-heap amplifier on adversarial input) and
        // never sees fields inside the untagged numeric arms anyway. Strict
        // rejection is the request handler's job, where value sizes are
        // tightly bounded.
        let t = xdr::Type::from_json(the_type, json_bytearray.as_slice())
            .map_err(|e| anyhow!("couldn't parse {type_str}: {e}"))?;

        t.to_xdr(DEFAULT_XDR_RW_LIMITS.clone())
            .map_err(|e| anyhow!("couldn't serialize {type_str}: {e}"))
    });

    let (xdr, error) = match result {
        Ok(bytes) => (vec_to_c_xdr(bytes), String::new()),
        Err(error) => (CXDR::default(), error),
    };

    // Caller is responsible for calling free_json_to_xdr_result.
    Box::into_raw(Box::new(JsonToXdrResult {
        xdr,
        error: string_to_c(error),
    }))
}

/// Frees memory allocated for the corresponding conversion result.
///
/// # Safety
///
/// You should *only* use this to free the return value of `json_to_xdr`.
#[no_mangle]
pub unsafe extern "C" fn free_json_to_xdr_result(ptr: *mut JsonToXdrResult) {
    if ptr.is_null() {
        return;
    }

    unsafe {
        let result = Box::from_raw(ptr);
        free_c_xdr(result.xdr);
        free_c_string(result.error);
    }
}

/// Converts an owned byte vector into an FFI-compatible raw XDR structure,
/// to be freed by `free_c_xdr`. Mirrors preflight's private `vec_to_c_array`,
/// like the Go side mirrors preflight's cgo helpers.
fn vec_to_c_xdr(v: Vec<u8>) -> CXDR {
    let len = v.len();
    let xdr = Box::into_raw(v.into_boxed_slice()).cast::<libc::c_uchar>();
    CXDR { xdr, len }
}

/// Frees the memory previously allocated by `vec_to_c_xdr`.
unsafe fn free_c_xdr(xdr: CXDR) {
    if xdr.xdr.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
            xdr.xdr, xdr.len,
        )));
    }
}

/// Runs a conversion operation and unwinds panics into an error string.
///
/// It is modeled after `catch_preflight_panic()`. Only panic-derived messages
/// get the `label` prefix; an ordinary `Err` from `op` passes through
/// unprefixed.
fn catch_conversion_panic<T>(label: &str, op: impl FnOnce() -> Result<T>) -> Result<T, String> {
    // catch panics before they reach foreign callers (which otherwise would result in
    // undefined behavior)
    let res: std::thread::Result<Result<T>> = panic::catch_unwind(panic::AssertUnwindSafe(op));

    match res {
        Err(panic) => {
            // Payloads are String from format-style panics and &str from
            // literal ones (e.g. assert! in dependencies).
            let msg = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&'static str>().copied())
                .unwrap_or("unknown cause");
            Err(format!("{label} failed: {msg}"))
        }
        // See https://docs.rs/anyhow/latest/anyhow/struct.Error.html#display-representations
        Ok(r) => r.map_err(|e| format!("{e:#}")),
    }
}
