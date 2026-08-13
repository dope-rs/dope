use std::mem;

use o3::cell;

use crate::driver::{
    self,
    route::{self, kind},
    schedule::ready,
};

pub(in crate::driver) struct Credit<'a>(&'a ready::Arena);

struct Held<'a> {
    index: usize,
    dispatch: ready::Dispatch<'a>,
    refs: &'a cell::LocalRefCount,
}

impl<'a> Credit<'a> {
    pub(in crate::driver) fn new(arena: &'a ready::Arena) -> Self {
        Self(arena)
    }

    fn resolve_held(&self, key: ready::FixedKey<'_>, target: route::Token) -> Option<Held<'_>> {
        let resolved = self.0.entries.slots.resolve(key.key())?;
        let dispatch = resolved.dispatch()?;
        if dispatch.get() != target.with_kind(kind::RECV_CREDIT_HELD) {
            return None;
        }
        Some(Held {
            index: resolved.index(),
            dispatch,
            refs: &self.0.recv_credit_refs[resolved.index()],
        })
    }

    pub(in crate::driver) fn arm(self, key: ready::FixedKey<'_>, target: route::Token) -> bool {
        let Some(resolved) = self.0.entries.slots.resolve(key.key()) else {
            return false;
        };
        let Some(dispatch) = resolved.dispatch() else {
            return false;
        };
        let refs = &self.0.recv_credit_refs[resolved.index()];
        if dispatch.get() != target || !refs.try_activate() {
            return false;
        }
        dispatch.set(target.with_kind(kind::RECV_CREDIT_HELD));
        true
    }

    pub(in crate::driver) fn retain(self, key: ready::FixedKey<'_>, target: route::Token) -> bool {
        let Some(held) = self.resolve_held(key, target) else {
            return false;
        };
        held.refs.try_retain()
    }

    pub(in crate::driver) fn release(
        self,
        key: ready::FixedKey<'_>,
        target: route::Token,
        wake: driver::RecvCreditWake,
    ) {
        let Some(held) = self.resolve_held(key, target) else {
            return;
        };
        let Some(last) = held.refs.try_release() else {
            return;
        };
        if !last {
            return;
        }
        held.refs.deactivate();
        held.dispatch.set(target.with_kind(wake as u8));
        self.0.ready.insert(held.index);
    }

    pub(in crate::driver) fn wake(
        self,
        key: ready::FixedKey<'_>,
        target: route::Token,
        wake: driver::RecvCreditWake,
    ) {
        let Some(held) = self.resolve_held(key, target) else {
            return;
        };
        if !held.refs.try_deactivate() {
            return;
        }
        held.dispatch.set(target.with_kind(wake as u8));
        self.0.ready.insert(held.index);
    }

    pub(in crate::driver) fn cancel(self, key: ready::FixedKey<'_>, target: route::Token) -> bool {
        let Some(held) = self.resolve_held(key, target) else {
            return false;
        };
        if !held.refs.try_deactivate() {
            return false;
        }
        held.dispatch.set(target);
        true
    }

    pub(in crate::driver) fn held(self, key: ready::FixedKey<'_>, target: route::Token) -> bool {
        let Some(dispatch) = self
            .0
            .entries
            .slots
            .resolve(key.key())
            .and_then(ready::Resolved::dispatch)
        else {
            return false;
        };
        let current = dispatch.get();
        current == target.with_kind(kind::RECV_CREDIT_HELD)
            || current == target.with_kind(driver::RecvCreditWake::ResourceReturned as u8)
            || current == target.with_kind(driver::RecvCreditWake::WaiterRetry as u8)
    }

    pub(in crate::driver) fn take(
        self,
        key: ready::FixedKey<'_>,
        target: route::Token,
    ) -> Option<driver::RecvCreditWake> {
        let resolved = self.0.entries.slots.resolve(key.key())?;
        let dispatch = resolved.dispatch()?;
        let current = dispatch.get();
        let wake = if current == target.with_kind(driver::RecvCreditWake::ResourceReturned as u8) {
            driver::RecvCreditWake::ResourceReturned
        } else if current == target.with_kind(driver::RecvCreditWake::WaiterRetry as u8) {
            driver::RecvCreditWake::WaiterRetry
        } else {
            return None;
        };
        debug_assert!(self.0.recv_credit_refs[resolved.index()].is_empty());
        dispatch.set(target);
        Some(wake)
    }

    pub(in crate::driver) fn arm_buffer(
        self,
        key: ready::FixedKey<'_>,
        target: route::Token,
    ) -> bool {
        let Some(resolved) = self.0.entries.slots.resolve(key.key()) else {
            return false;
        };
        let Some(dispatch) = resolved.dispatch() else {
            return false;
        };
        if dispatch.get() != target || !self.0.buffer_waiters.link(key) {
            return false;
        }
        dispatch.set(target.with_kind(kind::RECV_BUFFER_WAITING));
        true
    }

    pub(in crate::driver) fn cancel_buffer(
        self,
        key: ready::FixedKey<'_>,
        target: route::Token,
    ) -> bool {
        let Some(resolved) = self.0.entries.slots.resolve(key.key()) else {
            return false;
        };
        let Some(dispatch) = resolved.dispatch() else {
            return false;
        };
        let current = dispatch.get();
        if current == target.with_kind(kind::RECV_BUFFER_WAITING) {
            self.0.buffer_waiters.unlink(key);
        } else if current != target.with_kind(kind::RECV_BUFFER_GRANTED) {
            return false;
        }
        if current.kind() == kind::RECV_BUFFER_GRANTED {
            self.0.ready.remove(resolved.index());
        }
        dispatch.set(target);
        if current.kind() == kind::RECV_BUFFER_GRANTED {
            self.0.buffer_waiters.wake(self.0);
        }
        true
    }

    pub(in crate::driver) fn take_buffer(
        self,
        key: ready::FixedKey<'_>,
        target: route::Token,
    ) -> bool {
        let Some(dispatch) = self
            .0
            .entries
            .slots
            .resolve(key.key())
            .and_then(|entry| entry.dispatch())
        else {
            return false;
        };
        if dispatch.get() != target.with_kind(kind::RECV_BUFFER_GRANTED) {
            return false;
        }
        dispatch.set(target);
        true
    }

    pub(in crate::driver) fn release_buffers(self, count: usize) {
        for _ in 0..count {
            if !self.0.buffer_waiters.wake(self.0) {
                break;
            }
        }
    }

    pub(in crate::driver) fn release_buffer(self) {
        self.0.buffer_waiters.wake(self.0);
    }
}

const _: () = assert!(mem::size_of::<Credit<'static>>() == mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Held<'static>>() == 3 * mem::size_of::<usize>());
