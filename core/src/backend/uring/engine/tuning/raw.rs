use std::{array, marker, mem, pin, ptr, slice};

use io_uring::{opcode, squeue, types};

use crate::{
    backend::{
        self,
        uring::{engine::controls, ring},
    },
    driver::{self, route},
    io::{fd::handles, socket::option},
};

const _: () = assert!(mem::size_of::<libc::c_int>() == 4);

pub(in crate::backend::uring::engine) struct State {
    target: Option<route::Token>,
    values: [libc::c_int; option::MAX_STREAM_OPTIONS],
    _pin: marker::PhantomPinned,
}

pub(super) struct Chain<'state> {
    entries: [squeue::Entry; option::MAX_STREAM_OPTIONS],
    len: usize,
    state: marker::PhantomData<&'state State>,
}

pub(super) struct TunedConnectChain<'owner, 'state> {
    entries: backend::Captured<'owner, [squeue::Entry; option::MAX_STREAM_OPTIONS + 1]>,
    len: usize,
    state: marker::PhantomData<&'state State>,
}

pub(super) struct Cancel {
    entry: squeue::Entry,
}

const _: () = {
    assert!(mem::size_of::<State>() == 48);
    assert!(mem::size_of::<Cancel>() == mem::size_of::<squeue::Entry>());
    assert!(
        mem::size_of::<Chain<'static>>()
            == mem::size_of::<([squeue::Entry; option::MAX_STREAM_OPTIONS], usize)>()
    );
    assert!(
        mem::size_of::<TunedConnectChain<'static, 'static>>()
            == mem::size_of::<([squeue::Entry; option::MAX_STREAM_OPTIONS + 1], usize)>()
    );
};

impl State {
    pub(in crate::backend::uring::engine) const fn vacant() -> Self {
        Self {
            target: None,
            values: [0; option::MAX_STREAM_OPTIONS],
            _pin: marker::PhantomPinned,
        }
    }

    fn raw_mut(self: pin::Pin<&mut Self>) -> &mut Self {
        unsafe { pin::Pin::get_unchecked_mut(self) }
    }

    pub(super) fn reserve(
        mut self: pin::Pin<&mut Self>,
        options: option::StreamOptions,
        target: route::Token,
    ) -> bool {
        if !self.as_ref().is_vacant() {
            return false;
        }
        let state = self.as_mut().raw_mut();
        for (value, option) in state.values.iter_mut().zip(options.iter()) {
            *value = *option.value();
        }
        state.target = Some(target);
        true
    }

    pub(super) fn is_vacant(self: pin::Pin<&Self>) -> bool {
        self.target.is_none()
    }

    pub(super) fn complete(self: pin::Pin<&mut Self>, result: i32) -> Option<(route::Token, i32)> {
        self.raw_mut().target.take().map(|target| (target, result))
    }

    pub(super) fn reset(self: pin::Pin<&mut Self>) {
        self.raw_mut().target = None;
    }

    pub(super) fn chain(
        self: pin::Pin<&Self>,
        slot: handles::FixedSlot,
        options: option::StreamOptions,
        transaction: controls::Tuning,
    ) -> Chain<'_> {
        Chain::new(slot, options, transaction, self)
    }

    pub(super) fn tuned_connect<'owner>(
        self: pin::Pin<&Self>,
        slot: handles::FixedSlot,
        options: option::StreamOptions,
        transaction: controls::Tuning,
        terminal: backend::Captured<'owner, squeue::Entry>,
    ) -> TunedConnectChain<'owner, '_> {
        TunedConnectChain::new(slot, options, transaction, terminal, self)
    }

    pub(super) fn cancel(
        self: pin::Pin<&Self>,
        transaction: controls::Tuning,
        ring: &mut ring::Ready,
    ) -> Result<(), driver::SubmitError> {
        debug_assert!(!self.is_vacant());
        Cancel::new(transaction).submit(ring)
    }
}

unsafe fn submit(
    entries: &[squeue::Entry],
    ring: &mut ring::Ready,
) -> Result<(), driver::SubmitError> {
    unsafe {
        use crate::backend::uring::engine::submit::raw::Batch;
        Batch::new(entries)
    }
    .submit(ring)
}

fn lower_options<const N: usize>(
    slot: handles::FixedSlot,
    options: option::StreamOptions,
    transaction: controls::Tuning,
    state: pin::Pin<&State>,
) -> ([squeue::Entry; N], usize) {
    const { assert!(N >= option::MAX_STREAM_OPTIONS) };
    let state = state.get_ref();
    let token = transaction.token().framework_raw();
    let mut entries = array::from_fn(|_| opcode::Nop::new().build());
    let mut len = 0;
    for (option, value) in options.iter().zip(&state.values) {
        entries[len] = opcode::SetSockOpt::new(
            types::Fixed(slot.raw()),
            u32::from_ne_bytes(option.level().to_ne_bytes()),
            u32::from_ne_bytes(option.name().to_ne_bytes()),
            ptr::from_ref(value).cast(),
            4,
        )
        .build()
        .user_data(token)
        .flags(squeue::Flags::IO_LINK | squeue::Flags::SKIP_SUCCESS);
        len += 1;
    }
    (entries, len)
}

impl<'state> Chain<'state> {
    pub(super) fn new(
        slot: handles::FixedSlot,
        options: option::StreamOptions,
        transaction: controls::Tuning,
        state: pin::Pin<&'state State>,
    ) -> Self {
        let (mut entries, len) = lower_options(slot, options, transaction, state);
        debug_assert_ne!(len, 0);
        entries[len - 1] = entries[len - 1]
            .clone()
            .clear_flags()
            .flags(squeue::Flags::FIXED_FILE)
            .user_data(transaction.token().framework_raw());
        Self {
            entries,
            len,
            state: marker::PhantomData,
        }
    }

    /// The owning tuning table retains `state` until the chain's sole CQE: the
    /// final SetSockOpt, the first failing skipped step, or explicit cancel.
    pub(super) unsafe fn submit(self, ring: &mut ring::Ready) -> Result<(), driver::SubmitError> {
        unsafe { submit(&self.entries[..self.len], ring) }
    }
}

impl<'owner, 'state> TunedConnectChain<'owner, 'state> {
    pub(super) fn new(
        slot: handles::FixedSlot,
        options: option::StreamOptions,
        transaction: controls::Tuning,
        terminal: backend::Captured<'owner, squeue::Entry>,
        state: pin::Pin<&'state State>,
    ) -> Self {
        let (entries, mut len) = lower_options(slot, options, transaction, state);
        let entries = terminal.map(|terminal| {
            let mut entries = entries;
            entries[len] = terminal.user_data(transaction.token().framework_raw());
            len += 1;
            entries
        });
        Self {
            entries,
            len,
            state: marker::PhantomData,
        }
    }

    /// Retains `state` until the sole terminal/failure/cancel CQE and retains
    /// the terminal owner until the chain crosses the submission boundary.
    pub(super) unsafe fn submit(self, ring: &mut ring::Ready) -> Result<(), driver::SubmitError> {
        unsafe { submit(&self.entries.as_raw()[..self.len], ring) }
    }
}

impl Cancel {
    pub(super) fn new(transaction: controls::Tuning) -> Self {
        Self {
            entry: opcode::AsyncCancel::new(transaction.token().framework_raw())
                .build()
                .flags(squeue::Flags::SKIP_SUCCESS)
                .user_data(0),
        }
    }

    pub(super) fn submit(self, ring: &mut ring::Ready) -> Result<(), driver::SubmitError> {
        unsafe { submit(slice::from_ref(&self.entry), ring) }
    }
}
