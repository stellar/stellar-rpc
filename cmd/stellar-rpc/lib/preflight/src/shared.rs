// This file is included into the module graph as two different modules:
//
//   - crate::prev::shared for the previous protocol
//   - crate::curr::shared for the current protocol
//
// This file is the `shared` part of that path, and there is a different binding
// for `soroban_env_host` and `soroban_simulation` in each of the two parent
// modules `crate::prev` and `crate::curr`, corresponding to two different
// releases of soroban.
//
// We therefore import the different bindings for anything we use from
// `soroban_env_host` or `soroban_simulation` from `super::` rather than
// `crate::`.
use super::soroban_env_host::e2e_invoke::RecordingInvocationAuthMode;
use super::soroban_env_host::xdr::{
    AccountId, ExtendFootprintTtlOp, HostFunction, InvokeHostFunctionOp, LedgerEntry,
    LedgerFootprint, LedgerKey, OperationBody, ReadXdr, ScErrorCode, ScErrorType,
    SorobanTransactionData, WriteXdr,
};
use super::soroban_env_host::{LedgerInfo, DEFAULT_XDR_RW_LIMITS};
use super::soroban_simulation::simulation::{
    simulate_extend_ttl_op, simulate_invoke_host_function_op, simulate_restore_op,
    InvokeHostFunctionSimulationResult, LedgerEntryDiff, RestoreOpSimulationResult,
    SimulationAdjustmentConfig,
};
use super::soroban_simulation::{AutoRestoringSnapshotSource, NetworkConfig};

// Any definition that doesn't mention a soroban type in its signature can be
// stored in the common grandparent module `crate` a.k.a. `lib.rs`. Both copies
// of the `shared` module import the same definitions for these.

use crate::{
    anyhow, extract_error_string, from_c_string, from_c_xdr, string_to_c, vec_to_c_array,
    CLedgerInfo, CPreflightResult, CResourceConfig, CXDRDiff, CXDRDiffVector, CXDRVector, Digest,
    GoLedgerStorage, Result, Sha256, CXDR,
};
use std::convert::TryFrom;
use std::ptr::null_mut;
use std::rc::Rc;

#[derive(Clone, Copy)]
pub(crate) enum AuthMode {
    Enforce = 0,
    Record = 1,
    RecordAllowNonroot = 2,
}

fn fill_ledger_info(c_ledger_info: CLedgerInfo, network_config: &NetworkConfig) -> LedgerInfo {
    let network_passphrase = unsafe { from_c_string(c_ledger_info.network_passphrase) };
    let mut ledger_info = LedgerInfo {
        protocol_version: c_ledger_info.protocol_version,
        sequence_number: c_ledger_info.sequence_number,
        timestamp: c_ledger_info.timestamp,
        network_id: Sha256::digest(network_passphrase).into(),
        base_reserve: c_ledger_info.base_reserve,
        ..Default::default()
    };
    network_config.fill_config_fields_in_ledger_info(&mut ledger_info);
    ledger_info
}

// This has to be a free function rather than a method on an impl because there
// are two copies of this file mounted in the module tree and we can't define a
// same-named method on a single Self-type twice.
fn new_cpreflight_result_from_invoke_host_function(
    invoke_hf_result: InvokeHostFunctionSimulationResult,
    restore_preamble: Option<RestoreOpSimulationResult>,
    error: String,
) -> CPreflightResult {
    let mut result = CPreflightResult {
        error: string_to_c(error),
        auth: xdr_vec_to_c(&invoke_hf_result.auth),
        result: option_xdr_to_c(invoke_hf_result.invoke_result.ok().as_ref()),
        min_fee: invoke_hf_result
            .transaction_data
            .as_ref()
            .map_or_else(|| 0, |r| r.resource_fee),
        transaction_data: option_xdr_to_c(invoke_hf_result.transaction_data.as_ref()),
        // TODO: Diagnostic and contract events should be separated in the response
        events: xdr_vec_to_c(&invoke_hf_result.diagnostic_events),
        cpu_instructions: u64::from(invoke_hf_result.simulated_instructions),
        memory_bytes: u64::from(invoke_hf_result.simulated_memory),
        ledger_entry_diff: ledger_entry_diff_vec_to_c(&invoke_hf_result.modified_entries),
        ..Default::default()
    };
    if let Some(p) = restore_preamble {
        result.pre_restore_min_fee = p.transaction_data.resource_fee;
        result.pre_restore_transaction_data = xdr_to_c(&p.transaction_data);
    }
    result
}

// This has to be a free function rather than a method on an impl because there
// are two copies of this file mounted in the module tree and we can't define a
// same-named method on a single Self-type twice.
fn new_cpreflight_result_from_transaction_data(
    transaction_data: Option<&SorobanTransactionData>,
    restore_preamble: Option<&RestoreOpSimulationResult>,
    error: String,
) -> CPreflightResult {
    let min_fee = transaction_data.map_or(0, |d| d.resource_fee);
    let mut result = CPreflightResult {
        error: string_to_c(error),
        transaction_data: option_xdr_to_c(transaction_data),
        min_fee,
        ..Default::default()
    };
    if let Some(p) = restore_preamble {
        result.pre_restore_min_fee = p.transaction_data.resource_fee;
        result.pre_restore_transaction_data = xdr_to_c(&p.transaction_data);
    }
    result
}

pub(crate) fn preflight_invoke_hf_op_or_maybe_panic(
    handle: libc::uintptr_t,
    invoke_hf_op: CXDR,   // InvokeHostFunctionOp XDR in base64
    source_account: CXDR, // AccountId XDR in base64
    c_ledger_info: CLedgerInfo,
    resource_config: CResourceConfig,
    enable_debug: bool,
    auth_mode: AuthMode,
) -> Result<CPreflightResult> {
    let invoke_hf_op =
        InvokeHostFunctionOp::from_xdr(unsafe { from_c_xdr(invoke_hf_op) }, DEFAULT_XDR_RW_LIMITS)
            .unwrap();
    let source_account =
        AccountId::from_xdr(unsafe { from_c_xdr(source_account) }, DEFAULT_XDR_RW_LIMITS).unwrap();

    let go_storage = Rc::new(GoLedgerStorage::new(handle));
    let network_config =
        NetworkConfig::load_from_snapshot(go_storage.as_ref(), c_ledger_info.bucket_list_size)?;
    let ledger_info = fill_ledger_info(c_ledger_info, &network_config);

    let mut adjustment_config = SimulationAdjustmentConfig::default_adjustment();
    // It would be reasonable to extend `resource_config` to be compatible with `adjustment_config`
    // in order to let the users customize the resource/fee adjustments in a more granular fashion.

    let instruction_leeway = u32::try_from(resource_config.instruction_leeway)?;
    adjustment_config.instructions.additive_factor = adjustment_config
        .instructions
        .additive_factor
        .max(instruction_leeway);

    let auth_entries = invoke_hf_op.auth.to_vec();

    // Behavior differs based on user-supplied `auth_mode`: if chosen,
    // enforcement is done even without entries, while the recording modes
    // ignore the list entirely even if it's present.
    let auth_mode = match auth_mode {
        AuthMode::Enforce => RecordingInvocationAuthMode::Enforcing(auth_entries),
        AuthMode::Record => RecordingInvocationAuthMode::Recording(true),
        AuthMode::RecordAllowNonroot => RecordingInvocationAuthMode::Recording(false),
    };

    if ledger_info.protocol_version < 23 {
        // Protocols lower than 23 don't support autorestore,
        // we always use the restore preamble instead
        preflight_invoke_hf_op_pre_autorestore_or_maybe_panic(
            &go_storage,
            &network_config,
            &adjustment_config,
            &ledger_info,
            invoke_hf_op.host_function,
            auth_mode,
            &source_account,
            enable_debug,
        )
    } else {
        preflight_invoke_hf_op_post_autorestore_or_maybe_panic(
            &go_storage,
            &network_config,
            &adjustment_config,
            &ledger_info,
            invoke_hf_op.host_function,
            auth_mode,
            &source_account,
            enable_debug,
        )
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn preflight_invoke_hf_op_post_autorestore_or_maybe_panic(
    go_storage: &Rc<GoLedgerStorage>,
    network_config: &NetworkConfig,
    adjustment_config: &SimulationAdjustmentConfig,
    ledger_info: &LedgerInfo,
    hf: HostFunction,
    auth_mode: RecordingInvocationAuthMode,
    source_account: &AccountId,
    enable_debug: bool,
) -> Result<CPreflightResult> {
    // TODO: A restore preamble should be generated when network limits are surpassed.
    // The algorithm (suggested by Dima) would work as follows:
    // 1. run simulation using regular (non-auto-restore) snapshot source. the result will contain
    //    auto-restored entries (if any) in SorobanTransactionData . the result will also have proper
    //    fees and limits for autorestoration.
    // 2. verify the output SorobanTransactionData against the network limits.
    //    2a. if it is below limits, return the result from step 1
    //    2b. if it is above limits, re-run simulation using AutoRestoringSnapshotSource and
    //        populate the restore preamble. now you'll have an invoke transaction without any
    //        auto-restored entries, and a restore transaction.
    //
    // note, that step 2b doesn't guarantee that the original transaction without autorestore, or
    // the restore transaction don't exceed the network limits. which is why I suggest to postpone
    // that until after we have a more coherent design for handling the network limits in
    // simulation. thus the algorithm can be shortcut to just step 1.
    let invoke_hf_result: InvokeHostFunctionSimulationResult = simulate_invoke_host_function_op(
        go_storage.clone(),
        network_config,
        adjustment_config,
        ledger_info,
        hf,
        auth_mode,
        source_account,
        rand::Rng::gen(&mut rand::thread_rng()),
        enable_debug,
    )?;
    Ok(new_cpreflight_result_from_invoke_host_function(
        invoke_hf_result,
        None,
        String::new(),
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn preflight_invoke_hf_op_pre_autorestore_or_maybe_panic(
    go_storage: &Rc<GoLedgerStorage>,
    network_config: &NetworkConfig,
    adjustment_config: &SimulationAdjustmentConfig,
    ledger_info: &LedgerInfo,
    hf: HostFunction,
    auth_mode: RecordingInvocationAuthMode,
    source_account: &AccountId,
    enable_debug: bool,
) -> Result<CPreflightResult> {
    // Use an autorestore wrapper to build the restore preamble
    let auto_restore_snapshot = Rc::new(AutoRestoringSnapshotSource::new(
        go_storage.clone(),
        ledger_info,
    )?);

    // Invoke the host function. The user errors should normally be captured in
    // `invoke_hf_result.invoke_result` and this should return Err result for
    // misconfigured ledger.
    let invoke_hf_result: InvokeHostFunctionSimulationResult = simulate_invoke_host_function_op(
        auto_restore_snapshot.clone(),
        network_config,
        adjustment_config,
        ledger_info,
        hf,
        auth_mode,
        source_account,
        rand::Rng::gen(&mut rand::thread_rng()),
        enable_debug,
    )?;
    let maybe_restore_result = match &invoke_hf_result.invoke_result {
        Ok(_) => auto_restore_snapshot.simulate_restore_keys_op(
            network_config,
            &SimulationAdjustmentConfig::default_adjustment(),
            ledger_info,
        ),
        Err(e) => Err(e.clone().into()),
    };
    let error_str = extract_error_string(&maybe_restore_result, go_storage.as_ref());
    Ok(new_cpreflight_result_from_invoke_host_function(
        invoke_hf_result,
        maybe_restore_result.unwrap_or(None),
        error_str,
    ))
}

pub(crate) fn preflight_footprint_ttl_op_or_maybe_panic(
    handle: libc::uintptr_t,
    op_body: CXDR,
    footprint: CXDR,
    c_ledger_info: CLedgerInfo,
) -> Result<CPreflightResult> {
    let op_body = OperationBody::from_xdr(unsafe { from_c_xdr(op_body) }, DEFAULT_XDR_RW_LIMITS)?;
    let footprint =
        LedgerFootprint::from_xdr(unsafe { from_c_xdr(footprint) }, DEFAULT_XDR_RW_LIMITS)?;
    let go_storage = Rc::new(GoLedgerStorage::new(handle));
    let network_config =
        NetworkConfig::load_from_snapshot(go_storage.as_ref(), c_ledger_info.bucket_list_size)?;
    let ledger_info = fill_ledger_info(c_ledger_info, &network_config);
    // TODO: It would make for a better UX if the user passed only the necessary fields for every operation.
    // That would remove a possibility of providing bad operation body, or a possibility of filling wrong footprint
    // field.
    match op_body {
        OperationBody::ExtendFootprintTtl(extend_op) => {
            preflight_extend_ttl_op(&extend_op, footprint.read_only.as_slice(), &go_storage, &network_config, &ledger_info)
        }
        OperationBody::RestoreFootprint(_) => {
            Ok(preflight_restore_op(footprint.read_write.as_slice(), &go_storage, &network_config, &ledger_info))
        }
        _ => Err(anyhow!("encountered unsupported operation type: '{:?}', instead of 'ExtendFootprintTtl' or 'RestoreFootprint' operations.",
            op_body.discriminant()))
    }
}

fn preflight_extend_ttl_op(
    extend_op: &ExtendFootprintTtlOp,
    keys_to_extend: &[LedgerKey],
    go_storage: &Rc<GoLedgerStorage>,
    network_config: &NetworkConfig,
    ledger_info: &LedgerInfo,
) -> Result<CPreflightResult> {
    let auto_restore_snapshot = AutoRestoringSnapshotSource::new(go_storage.clone(), ledger_info)?;
    let simulation_result = simulate_extend_ttl_op(
        &auto_restore_snapshot,
        network_config,
        &SimulationAdjustmentConfig::default_adjustment(),
        ledger_info,
        keys_to_extend,
        extend_op.extend_to,
    );
    let (maybe_transaction_data, maybe_restore_result) = match simulation_result {
        Ok(r) => (
            Some(r.transaction_data),
            auto_restore_snapshot.simulate_restore_keys_op(
                network_config,
                &SimulationAdjustmentConfig::default_adjustment(),
                ledger_info,
            ),
        ),
        Err(e) => (None, Err(e)),
    };

    let error_str = extract_error_string(&maybe_restore_result, go_storage);
    Ok(new_cpreflight_result_from_transaction_data(
        maybe_transaction_data.as_ref(),
        maybe_restore_result.ok().flatten().as_ref(),
        error_str,
    ))
}

fn preflight_restore_op(
    keys_to_restore: &[LedgerKey],
    go_storage: &Rc<GoLedgerStorage>,
    network_config: &NetworkConfig,
    ledger_info: &LedgerInfo,
) -> CPreflightResult {
    let simulation_result = simulate_restore_op(
        go_storage.as_ref(),
        network_config,
        &SimulationAdjustmentConfig::default_adjustment(),
        ledger_info,
        keys_to_restore,
    );
    let error_str = extract_error_string(&simulation_result, go_storage.as_ref());
    new_cpreflight_result_from_transaction_data(
        simulation_result.ok().map(|r| r.transaction_data).as_ref(),
        None,
        error_str,
    )
}

// TODO: We could use something like https://github.com/sonos/ffi-convert-rs
//       to replace all the free_* , *_to_c and from_c_* functions by implementations of CDrop,
//       CReprOf and AsRust
fn xdr_to_c(v: &impl WriteXdr) -> CXDR {
    let (xdr, len) = vec_to_c_array(v.to_xdr(DEFAULT_XDR_RW_LIMITS).unwrap());
    CXDR { xdr, len }
}

fn option_xdr_to_c(v: Option<&impl WriteXdr>) -> CXDR {
    v.map_or(
        CXDR {
            xdr: null_mut(),
            len: 0,
        },
        xdr_to_c,
    )
}

fn ledger_entry_diff_to_c(v: &LedgerEntryDiff) -> CXDRDiff {
    CXDRDiff {
        before: option_xdr_to_c(v.state_before.as_ref()),
        after: option_xdr_to_c(v.state_after.as_ref()),
    }
}

fn xdr_vec_to_c(v: &[impl WriteXdr]) -> CXDRVector {
    let c_v = v.iter().map(xdr_to_c).collect();
    let (array, len) = vec_to_c_array(c_v);
    CXDRVector { array, len }
}

fn ledger_entry_diff_vec_to_c(modified_entries: &[LedgerEntryDiff]) -> CXDRDiffVector {
    let c_diffs = modified_entries
        .iter()
        .map(ledger_entry_diff_to_c)
        .collect();
    let (array, len) = vec_to_c_array(c_diffs);
    CXDRDiffVector { array, len }
}

impl From<u32> for AuthMode {
    fn from(x: u32) -> AuthMode {
        match x {
            0 => AuthMode::Enforce,
            1 => AuthMode::Record,
            2 => AuthMode::RecordAllowNonroot,
            _ => panic!("invalid AuthMode value"),
        }
    }
}

// Gets a ledger entry by key, including the archived/removed entries.
// The failures of this function are not recoverable and should only happen when
// the underlying storage is somehow corrupted.
//
// This has to be a free function rather than a method on an impl because there
// are two copies of this file mounted in the module tree and we can't define a
// same-named method on a single Self-type twice.
pub(crate) fn get_fallible_from_go_ledger_storage(
    storage: &GoLedgerStorage,
    key: &LedgerKey,
) -> Result<
    Option<super::soroban_env_host::storage::EntryWithLiveUntil>,
    super::soroban_env_host::HostError,
> {
    let mut key_xdr = match key.to_xdr(DEFAULT_XDR_RW_LIMITS) {
        Ok(res) => res,
        Err(e) => {
            // Store the internal error in the storage as the info won't
            // be propagated from simulation.
            if let Ok(mut err) = storage.internal_error.try_borrow_mut() {
                *err = Some(e.into());
            }
            // Errors that occur in storage are not recoverable, so we
            // force host to halt by passing it an internal error.
            return Err((ScErrorType::Storage, ScErrorCode::InternalError).into());
        }
    };

    let Some((xdr, live_until_ledger_seq)) = storage.get_xdr_internal(&mut key_xdr) else {
        return Ok(None);
    };

    let entry = match LedgerEntry::from_xdr(xdr, DEFAULT_XDR_RW_LIMITS) {
        Ok(res) => res,
        Err(e) => {
            // Same error handling as above
            if let Ok(mut err) = storage.internal_error.try_borrow_mut() {
                *err = Some(e.into());
            }
            return Err((ScErrorType::Storage, ScErrorCode::InternalError).into());
        }
    };

    Ok(Some((Rc::new(entry), live_until_ledger_seq)))
}
