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
    AccountId, Error as XdrError, ExtendFootprintTtlOp, HostFunction, InvokeHostFunctionOp,
    LedgerEntry, LedgerFootprint, LedgerKey, Limits, OperationBody, ReadXdr, ScErrorCode,
    ScErrorType, SorobanTransactionData, WriteXdr,
};
use super::soroban_env_host::{LedgerInfo, DEFAULT_XDR_RW_LIMITS};
use super::soroban_simulation::simulation::{
    simulate_extend_ttl_op, simulate_invoke_host_function_op, simulate_restore_op,
    InvokeHostFunctionSimulationResult, RestoreOpSimulationResult, SimulationAdjustmentConfig,
};
use super::soroban_simulation::{AutoRestoringSnapshotSource, NetworkConfig};

// Any definition that doesn't mention a soroban type in its signature can be
// stored in the common grandparent module `crate` a.k.a. `lib.rs`. Both copies
// of the `shared` module import the same definitions for these.

use crate::{
    anyhow, extract_error_string, from_c_string, from_c_xdr, CLedgerInfo, CResourceConfig, Digest,
    GoLedgerStorage, PreflightOutput, Result, Sha256, CXDR, MAX_RESULT_BYTES,
};
use std::convert::TryFrom;
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

fn output_from_invoke_host_function(
    invoke_hf_result: &InvokeHostFunctionSimulationResult,
    error: String,
) -> Result<PreflightOutput> {
    let mut remaining = MAX_RESULT_BYTES;
    Ok(PreflightOutput {
        error,
        auth: encode_all(&invoke_hf_result.auth, &mut remaining)?,
        result: encode_option(invoke_hf_result.invoke_result.as_ref().ok(), &mut remaining)?,
        min_fee: invoke_hf_result
            .transaction_data
            .as_ref()
            .map_or(0, |r| r.resource_fee),
        transaction_data: encode_option(
            invoke_hf_result.transaction_data.as_ref(),
            &mut remaining,
        )?,
        // TODO: Diagnostic and contract events should be separated in the response
        events: encode_all(&invoke_hf_result.diagnostic_events, &mut remaining)?,
        cpu_instructions: u64::from(invoke_hf_result.simulated_instructions),
        memory_bytes: u64::from(invoke_hf_result.simulated_memory),
        ledger_entry_diff: invoke_hf_result
            .modified_entries
            .iter()
            .map(|diff| {
                Ok((
                    encode_option(diff.state_before.as_ref(), &mut remaining)?,
                    encode_option(diff.state_after.as_ref(), &mut remaining)?,
                ))
            })
            .collect::<Result<_>>()?,
        ..Default::default()
    })
}

fn output_from_transaction_data(
    transaction_data: Option<&SorobanTransactionData>,
    restore_preamble: Option<&RestoreOpSimulationResult>,
    error: String,
) -> Result<PreflightOutput> {
    let mut remaining = MAX_RESULT_BYTES;
    Ok(PreflightOutput {
        error,
        transaction_data: encode_option(transaction_data, &mut remaining)?,
        min_fee: transaction_data.map_or(0, |d| d.resource_fee),
        pre_restore_transaction_data: encode_option(
            restore_preamble.map(|p| &p.transaction_data),
            &mut remaining,
        )?,
        pre_restore_min_fee: restore_preamble.map_or(0, |p| p.transaction_data.resource_fee),
        ..Default::default()
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn preflight_invoke_hf_op_or_maybe_panic(
    handle: libc::uintptr_t,
    invoke_hf_op: CXDR,   // InvokeHostFunctionOp XDR in base64
    source_account: CXDR, // AccountId XDR in base64
    c_ledger_info: CLedgerInfo,
    resource_config: CResourceConfig,
    enable_debug: bool,
    auth_mode: AuthMode,
    use_upgraded_auth: bool,
) -> Result<PreflightOutput> {
    let invoke_hf_op =
        InvokeHostFunctionOp::from_xdr(unsafe { from_c_xdr(invoke_hf_op) }, DEFAULT_XDR_RW_LIMITS)
            .unwrap();
    let source_account =
        AccountId::from_xdr(unsafe { from_c_xdr(source_account) }, DEFAULT_XDR_RW_LIMITS).unwrap();

    let go_storage = Rc::new(GoLedgerStorage::new(handle));
    let network_config = NetworkConfig::load_from_snapshot(go_storage.as_ref())?;
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
        AuthMode::Record => super::make_recording_auth_mode(true, use_upgraded_auth),
        AuthMode::RecordAllowNonroot => super::make_recording_auth_mode(false, use_upgraded_auth),
    };

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
) -> Result<PreflightOutput> {
    // TODO: A restore preamble should be generated when network limits are surpassed.
    //       See https://github.com/stellar/stellar-rpc/issues/464
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
    let invoke_result = invoke_hf_result
        .invoke_result
        .as_ref()
        .map_err(|e| anyhow!("HostError: {:?}", e.error));
    let error_str = extract_error_string(&invoke_result, go_storage.as_ref());
    output_from_invoke_host_function(&invoke_hf_result, error_str)
}

pub(crate) fn preflight_footprint_ttl_op_or_maybe_panic(
    handle: libc::uintptr_t,
    op_body: CXDR,
    footprint: CXDR,
    c_ledger_info: CLedgerInfo,
) -> Result<PreflightOutput> {
    let op_body = OperationBody::from_xdr(unsafe { from_c_xdr(op_body) }, DEFAULT_XDR_RW_LIMITS)?;
    let footprint =
        LedgerFootprint::from_xdr(unsafe { from_c_xdr(footprint) }, DEFAULT_XDR_RW_LIMITS)?;
    let go_storage = Rc::new(GoLedgerStorage::new(handle));
    let network_config = NetworkConfig::load_from_snapshot(go_storage.as_ref())?;

    let ledger_info = fill_ledger_info(c_ledger_info, &network_config);
    // TODO: It would make for a better UX if the user passed only the necessary fields for every operation.
    // That would remove a possibility of providing bad operation body, or a possibility of filling wrong footprint
    // field.
    match op_body {
        OperationBody::ExtendFootprintTtl(extend_op) => {
            preflight_extend_ttl_op(&extend_op, footprint.read_only.as_slice(), &go_storage, &network_config, &ledger_info)
        }
        OperationBody::RestoreFootprint(_) => {
            preflight_restore_op(footprint.read_write.as_slice(), &go_storage, &network_config, &ledger_info)
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
) -> Result<PreflightOutput> {
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
    output_from_transaction_data(
        maybe_transaction_data.as_ref(),
        maybe_restore_result.ok().flatten().as_ref(),
        error_str,
    )
}

fn preflight_restore_op(
    keys_to_restore: &[LedgerKey],
    go_storage: &Rc<GoLedgerStorage>,
    network_config: &NetworkConfig,
    ledger_info: &LedgerInfo,
) -> Result<PreflightOutput> {
    let simulation_result = simulate_restore_op(
        go_storage.as_ref(),
        network_config,
        &SimulationAdjustmentConfig::default_adjustment(),
        ledger_info,
        keys_to_restore,
    );
    let error_str = extract_error_string(&simulation_result, go_storage.as_ref());
    output_from_transaction_data(
        simulation_result.ok().map(|r| r.transaction_data).as_ref(),
        None,
        error_str,
    )
}

// Encodes `v` within the `remaining` byte budget shared by every field of a
// preflight result.
fn encode(v: &impl WriteXdr, remaining: &mut usize) -> Result<Vec<u8>> {
    let limits = Limits {
        depth: DEFAULT_XDR_RW_LIMITS.depth,
        len: *remaining,
    };
    let xdr = v.to_xdr(limits).map_err(|e| match e {
        XdrError::LengthLimitExceeded => {
            anyhow!("simulation result exceeds {MAX_RESULT_BYTES} bytes")
        }
        e => e.into(),
    })?;
    *remaining -= xdr.len();
    Ok(xdr)
}

fn encode_option(v: Option<&impl WriteXdr>, remaining: &mut usize) -> Result<Option<Vec<u8>>> {
    v.map(|v| encode(v, remaining)).transpose()
}

fn encode_all(v: &[impl WriteXdr], remaining: &mut usize) -> Result<Vec<Vec<u8>>> {
    v.iter().map(|v| encode(v, remaining)).collect()
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

impl super::soroban_env_host::storage::SnapshotSource for crate::GoLedgerStorage {
    fn get(
        &self,
        key: &Rc<LedgerKey>,
    ) -> Result<
        Option<super::soroban_env_host::storage::EntryWithLiveUntil>,
        super::soroban_env_host::HostError,
    > {
        get_fallible_from_go_ledger_storage(self, key.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::super::soroban_env_host::xdr::{
        ContractEvent, ContractEventBody, ContractEventType, ContractEventV0, DiagnosticEvent,
        ExtensionPoint, ScBytes, ScVal, VecM,
    };
    use super::*;

    fn bytes(len: usize) -> ScVal {
        ScVal::Bytes(ScBytes(vec![0; len].try_into().unwrap()))
    }

    fn event(data: ScVal) -> DiagnosticEvent {
        DiagnosticEvent {
            in_successful_contract_call: true,
            event: ContractEvent {
                ext: ExtensionPoint::V0,
                contract_id: None,
                type_: ContractEventType::Diagnostic,
                body: ContractEventBody::V0(ContractEventV0 {
                    topics: VecM::default(),
                    data,
                }),
            },
        }
    }

    #[test]
    fn events_count_toward_the_result_limit() {
        let half = bytes(MAX_RESULT_BYTES / 2);
        let simulation = InvokeHostFunctionSimulationResult {
            invoke_result: Ok(half.clone()),
            auth: vec![],
            contract_events: vec![],
            diagnostic_events: vec![event(half)],
            transaction_data: None,
            simulated_instructions: 0,
            simulated_memory: 0,
            modified_entries: vec![],
        };
        let err = output_from_invoke_host_function(&simulation, String::new())
            .err()
            .unwrap();
        assert!(err.to_string().contains("exceeds"), "{err}");
    }

    #[test]
    fn fields_share_one_byte_budget() {
        let half = bytes(MAX_RESULT_BYTES / 2);
        let mut remaining = MAX_RESULT_BYTES;
        assert!(encode_all(std::slice::from_ref(&half), &mut remaining).is_ok());
        let err = encode_all(&[half], &mut remaining).unwrap_err();
        assert!(err.to_string().contains("exceeds"), "{err}");
    }
}
