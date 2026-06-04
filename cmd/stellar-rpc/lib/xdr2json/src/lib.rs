extern crate anyhow;
extern crate ffi;
extern crate stellar_xdr;

use std::{panic, str::FromStr};
use stellar_xdr as xdr;

use anyhow::Result;

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

struct RustConversionResult {
    json: String,
    error: String,
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
/// This should never panic due to `catch_json_to_xdr_panic` catching and
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
    let result = catch_json_to_xdr_panic(Box::new(move || {
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

        Ok(RustConversionResult {
            json: serde_json::to_string(&t).unwrap(),
            error: String::new(),
        })
    }));

    // Caller is responsible for calling free_conversion_result.
    Box::into_raw(Box::new(ConversionResult {
        json: string_to_c(result.json),
        error: string_to_c(result.error),
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

/// Runs a JSON conversion operation and unwinds panics.
///
/// It is modeled after `catch_preflight_panic()` and will always return valid
/// JSON in the result's `json` field and an error string in `error` if a panic
/// occurs.
fn catch_json_to_xdr_panic(
    op: Box<dyn Fn() -> Result<RustConversionResult>>,
) -> RustConversionResult {
    // catch panics before they reach foreign callers (which otherwise would result in
    // undefined behavior)
    let res: std::thread::Result<Result<RustConversionResult>> =
        panic::catch_unwind(panic::AssertUnwindSafe(op));

    match res {
        Err(panic) => match panic.downcast::<String>() {
            Ok(panic_msg) => RustConversionResult {
                json: "{}".to_string(),
                error: format!("xdr_to_json() failed: {panic_msg}"),
            },
            Err(_) => RustConversionResult {
                json: "{}".to_string(),
                error: "xdr_to_json() failed: unknown cause".to_string(),
            },
        },
        // See https://docs.rs/anyhow/latest/anyhow/struct.Error.html#display-representations
        Ok(r) => r.unwrap_or_else(|e| RustConversionResult {
            json: "{}".to_string(),
            error: format!("{e:?}"),
        }),
    }
}
