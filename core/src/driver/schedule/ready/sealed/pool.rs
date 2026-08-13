use std::{cell, io, process};

use crate::driver::{route, schedule::ready};

#[repr(transparent)]
pub(in crate::driver::schedule::ready) struct Reservation<'d>(ready::DynamicKey<'d>);

impl<'d> Reservation<'d> {
    fn new(key: ready::DynamicKey<'d>) -> Self {
        Self(key)
    }

    fn key(&self) -> ready::DynamicKey<'d> {
        self.0
    }
}

pub(in crate::driver::schedule::ready) struct Pool {
    free: cell::Cell<ready::FreeLink>,
    free_len: cell::Cell<usize>,
}

impl Pool {
    pub(super) fn new(free: ready::FreeLink, available: usize) -> Self {
        Self {
            free: cell::Cell::new(free),
            free_len: cell::Cell::new(available),
        }
    }

    pub(in crate::driver::schedule::ready) fn available(&self) -> usize {
        self.free_len.get()
    }

    fn reserve<'d>(
        &'d self,
        slots: &'d ready::Slots,
        reserve: usize,
    ) -> io::Result<Reservation<'d>> {
        let available = self.available();
        if available <= reserve {
            return Err(Self::capacity_error(1, available));
        }
        let index = self.free.get().index();
        let dynamic = slots.dynamic(index);
        debug_assert!(dynamic.kind.get() == ready::Kind::Free);
        let next = unsafe { dynamic.payload.get().into_free() };
        dynamic.kind.set(ready::Kind::Reserved);
        let epoch = dynamic.epoch.get();
        self.free.set(next);
        self.free_len.set(available - 1);
        Ok(Reservation::new(ready::DynamicKey::new(index, epoch)))
    }

    pub(in crate::driver::schedule::ready) fn reserve_dispatch<'d>(
        &'d self,
        slots: &'d ready::Slots,
        reserve: usize,
        target: route::Token,
    ) -> io::Result<ready::DynamicKey<'d>> {
        let reservation = self.reserve(slots, reserve)?;
        Ok(self.install_dispatch(slots, reservation, target))
    }

    pub(in crate::driver::schedule::ready) fn reserve_task<'d>(
        &'d self,
        slots: &'d ready::Slots,
    ) -> io::Result<Reservation<'d>> {
        self.reserve(slots, 0)
    }

    pub(in crate::driver::schedule::ready) fn lease_tasks(
        &self,
        requested: usize,
    ) -> Result<(), usize> {
        let available = self.available();
        if requested > available {
            return Err(available);
        }
        self.free_len.set(available - requested);
        Ok(())
    }

    pub(in crate::driver::schedule::ready) fn claim_leased_task<'d>(
        &'d self,
        slots: &'d ready::Slots,
    ) -> Option<Reservation<'d>> {
        let free = self.free.get();
        if free.is_empty() {
            return None;
        }
        let index = free.index();
        let dynamic = slots.dynamic(index);
        if dynamic.kind.get() != ready::Kind::Free {
            process::abort();
        }
        let next = unsafe { dynamic.payload.get().into_free() };
        dynamic.kind.set(ready::Kind::Reserved);
        let epoch = dynamic.epoch.get();
        self.free.set(next);
        Some(Reservation::new(ready::DynamicKey::new(index, epoch)))
    }

    pub(in crate::driver::schedule::ready) fn release_task_lease(&self, remaining: usize) {
        if remaining == 0 {
            return;
        }
        let available = self.available();
        let Some(available) = available.checked_add(remaining) else {
            process::abort();
        };
        self.free_len.set(available);
    }

    pub(in crate::driver::schedule::ready) fn return_leased_task<'d>(
        &'d self,
        slots: &'d ready::Slots,
        reservation: &Reservation<'d>,
    ) {
        let key = reservation.key();
        let dynamic = slots.dynamic(key.index());
        debug_assert!(dynamic.epoch.get() == key.epoch());
        debug_assert!(dynamic.kind.get() == ready::Kind::Reserved);
        dynamic
            .payload
            .set(ready::raw::Payload::free(self.free.get()));
        dynamic.kind.set(ready::Kind::Free);
        self.free.set(ready::FreeLink::from_index(dynamic.index));
    }

    pub(in crate::driver::schedule::ready) fn capacity_error(
        requested: usize,
        available: usize,
    ) -> io::Error {
        io::Error::new(
            io::ErrorKind::WouldBlock,
            format!(
                "dope: dynamic ready capacity exhausted: requested {requested}, available {available}"
            ),
        )
    }

    fn install_dispatch<'d>(
        &'d self,
        slots: &'d ready::Slots,
        reservation: Reservation<'d>,
        target: route::Token,
    ) -> ready::DynamicKey<'d> {
        let key = reservation.key();
        let dynamic = slots.dynamic(key.index());
        debug_assert!(dynamic.epoch.get() == key.epoch());
        debug_assert!(dynamic.kind.get() == ready::Kind::Reserved);
        dynamic.payload.set(ready::raw::Payload::dispatch(target));
        dynamic.kind.set(ready::Kind::Dispatch);
        key
    }

    pub(in crate::driver::schedule::ready) fn install_task<'d>(
        &'d self,
        slots: &'d ready::Slots,
        reservation: Reservation<'d>,
        task: ready::raw::Task<'d>,
    ) -> ready::DynamicKey<'d> {
        let key = reservation.key();
        let dynamic = slots.dynamic(key.index());
        debug_assert!(dynamic.epoch.get() == key.epoch());
        debug_assert!(dynamic.kind.get() == ready::Kind::Reserved);
        dynamic.payload.set(task.into_payload());
        dynamic.kind.set(ready::Kind::Task);
        key
    }

    pub(in crate::driver::schedule::ready) fn release<'d>(
        &'d self,
        slots: &'d ready::Slots,
        key: ready::DynamicKey<'d>,
        remove_ready: impl FnOnce(usize) -> bool,
    ) -> bool {
        let dynamic = slots.dynamic(key.index());
        debug_assert!(dynamic.epoch.get() == key.epoch());
        debug_assert!(matches!(
            dynamic.kind.get(),
            ready::Kind::Dispatch | ready::Kind::Task
        ));
        dynamic.kind.set(ready::Kind::Reserved);
        let was_ready = remove_ready(dynamic.index.get());
        self.recycle(key, dynamic);
        was_ready
    }

    pub(in crate::driver::schedule::ready) fn reclaim_task<'d>(
        &'d self,
        slots: &'d ready::Slots,
        key: ready::DynamicKey<'d>,
        remove_ready: impl FnOnce(usize) -> bool,
    ) -> (Option<Reservation<'d>>, bool) {
        let dynamic = slots.dynamic(key.index());
        debug_assert!(dynamic.epoch.get() == key.epoch());
        debug_assert!(dynamic.kind.get() == ready::Kind::Task);
        dynamic.kind.set(ready::Kind::Reserved);
        let was_ready = remove_ready(dynamic.index.get());
        let Some(epoch) = key.epoch().checked_add(1) else {
            dynamic.kind.set(ready::Kind::Retired);
            return (None, was_ready);
        };
        dynamic.epoch.set(epoch);
        (
            Some(Reservation::new(ready::DynamicKey::new(
                dynamic.index,
                epoch,
            ))),
            was_ready,
        )
    }

    pub(in crate::driver::schedule::ready) fn release_reserved<'d>(
        &'d self,
        slots: &'d ready::Slots,
        reservation: &Reservation<'d>,
    ) {
        let key = reservation.key();
        let dynamic = slots.dynamic(key.index());
        debug_assert!(dynamic.epoch.get() == key.epoch());
        debug_assert!(dynamic.kind.get() == ready::Kind::Reserved);
        self.recycle(key, dynamic);
    }

    fn recycle<'d>(&'d self, key: ready::DynamicKey<'d>, dynamic: ready::Dynamic<'d>) {
        let Some(epoch) = key.epoch().checked_add(1) else {
            dynamic.kind.set(ready::Kind::Retired);
            return;
        };
        dynamic.epoch.set(epoch);
        dynamic
            .payload
            .set(ready::raw::Payload::free(self.free.get()));
        dynamic.kind.set(ready::Kind::Free);
        self.free.set(ready::FreeLink::from_index(dynamic.index));
        self.free_len.set(self.free_len.get() + 1);
    }
}

const _: () = {
    assert!(std::mem::size_of::<Pool>() == 2 * std::mem::size_of::<usize>());
    assert!(
        std::mem::size_of::<Reservation<'static>>()
            == std::mem::size_of::<ready::DynamicKey<'static>>()
    );
};
