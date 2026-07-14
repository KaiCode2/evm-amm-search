use std::{cell::RefCell, error::Error, io};

use alloy_primitives::{Address, Bytes, U256};
use evm_amm_state::adapters::{
    AdapterCache, CacheError, CallOutcome, PurgeScope, SlotChange, StateDiff, StateUpdate,
    StateView,
};
use evm_fork_cache::cache::EvmOverlay;
use revm::database_interface::Database;

pub(crate) struct OverlayAdapterCache {
    overlay: RefCell<EvmOverlay>,
}

impl OverlayAdapterCache {
    pub(crate) fn new(overlay: EvmOverlay) -> Self {
        Self {
            overlay: RefCell::new(overlay),
        }
    }
}

impl StateView for OverlayAdapterCache {
    fn storage(&self, address: Address, slot: U256) -> Option<U256> {
        Database::storage(&mut *self.overlay.borrow_mut(), address, slot).ok()
    }
}

impl AdapterCache for OverlayAdapterCache {
    fn cached_storage(&self, address: Address, slot: U256) -> Option<U256> {
        StateView::storage(self, address, slot)
    }

    fn apply_updates(&mut self, updates: &[StateUpdate]) -> StateDiff {
        let diff = StateDiff::default();

        for update in updates {
            match update {
                StateUpdate::Slot {
                    address,
                    slot,
                    value,
                } => self.override_slot(*address, *slot, *value),
                StateUpdate::SlotDelta {
                    address,
                    slot,
                    delta,
                } => {
                    if let Ok(current) = Database::storage(self.overlay.get_mut(), *address, *slot)
                    {
                        self.override_slot(*address, *slot, delta.apply(current));
                    }
                }
                StateUpdate::SlotMasked {
                    address,
                    slot,
                    mask,
                    value,
                } => {
                    if let Ok(current) = Database::storage(self.overlay.get_mut(), *address, *slot)
                    {
                        let next = (current & !*mask) | (*value & *mask);
                        self.override_slot(*address, *slot, next);
                    }
                }
                StateUpdate::Purge { address, scope } => match scope {
                    PurgeScope::AllStorage => {
                        let _ = address;
                    }
                    PurgeScope::Slots(slots) => {
                        for slot in slots {
                            let _ = slot;
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }

        diff
    }

    fn verify_slots(&mut self, slots: &[(Address, U256)]) -> Result<Vec<SlotChange>, CacheError> {
        for (address, slot) in slots {
            let _ = Database::storage(self.overlay.get_mut(), *address, *slot)
                .map_err(cache_error_from)?;
        }
        Ok(Vec::new())
    }

    fn purge_storage(&mut self, _address: Address) -> StateDiff {
        StateDiff::default()
    }

    fn purge_slots(&mut self, _address: Address, _slots: &[U256]) -> StateDiff {
        StateDiff::default()
    }

    fn read_storage_slot(&mut self, address: Address, slot: U256) -> Result<U256, CacheError> {
        Database::storage(self.overlay.get_mut(), address, slot).map_err(cache_error_from)
    }

    fn read_storage_slots(&mut self, slots: &[(Address, U256)]) -> Result<Vec<U256>, CacheError> {
        slots
            .iter()
            .map(|(address, slot)| self.read_storage_slot(*address, *slot))
            .collect()
    }

    fn call_raw(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
        commit: bool,
    ) -> Result<CallOutcome, CacheError> {
        if commit {
            return Err(cache_error_message(
                "overlay-backed search cache does not support committing raw calls",
            ));
        }

        self.overlay
            .get_mut()
            .call_raw(from, to, calldata)
            .map(CallOutcome::from)
            .map_err(cache_error_from)
    }

    fn call_raw_with_code_overrides(
        &mut self,
        from: Address,
        to: Address,
        calldata: Bytes,
        code_overrides: &[(Address, Bytes)],
        commit: bool,
    ) -> Result<CallOutcome, CacheError> {
        if commit {
            return Err(cache_error_message(
                "overlay-backed search cache does not support committing raw calls",
            ));
        }

        self.overlay
            .get_mut()
            .call_raw_with_code_overrides(from, to, calldata, code_overrides)
            .map(CallOutcome::from)
            .map_err(cache_error_from)
    }
}

impl OverlayAdapterCache {
    fn override_slot(&mut self, address: Address, slot: U256, value: U256) {
        self.overlay.get_mut().override_slot(address, slot, value);
    }
}

fn cache_error_from<E>(error: E) -> CacheError
where
    E: Error + Send + Sync + 'static,
{
    CacheError::Backend(Box::new(error))
}

fn cache_error_message(message: &'static str) -> CacheError {
    CacheError::Backend(Box::new(io::Error::other(message)))
}
