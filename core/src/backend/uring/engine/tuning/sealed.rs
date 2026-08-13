use std::{io, pin};

use io_uring::squeue;
use o3::collections::fixed::pinned;

use crate::{
    backend::{
        self,
        uring::{
            engine::{controls, tuning},
            ring,
        },
    },
    driver::{self, route, route::table},
    io::{fd::handles, socket::option},
};

/// Address-stable tuning states indexed by affine physical fixed-file slots.
/// A slot becomes vacant only after its linked chain's sole terminal CQE.
pub(in crate::backend::uring) struct Table {
    states: States,
}

#[repr(transparent)]
struct States(pinned::Slice<tuning::raw::State>);

struct Binding<'state> {
    transaction: controls::Tuning,
    state: pin::Pin<&'state mut tuning::raw::State>,
}

#[repr(transparent)]
struct Reserved<'state>(Binding<'state>);

#[repr(transparent)]
struct Occupied<'state>(Binding<'state>);

impl Table {
    pub(in crate::backend::uring) fn new(capacity: table::Capacity) -> io::Result<Self> {
        use o3::collections::BoxSliceExt;

        let states: Box<[tuning::raw::State]> =
            BoxSliceExt::try_box_with(capacity.get(), |_| tuning::raw::State::vacant())?;
        Ok(Self {
            states: States(states.into()),
        })
    }

    pub(in crate::backend::uring) fn submit_tuning<'d>(
        &mut self,
        ring: &mut ring::Ready,
        fd: &handles::Descriptor<'d>,
        options: option::StreamOptions,
        target: route::Token,
    ) -> Result<(), driver::SubmitError> {
        let Some(reserved) = self.states.reserve(fd.token_index(), options, target) else {
            return Err(driver::SubmitError);
        };
        let transaction = reserved.transaction();
        let chain = reserved.state().chain(fd.slot(), options, transaction);
        let result = unsafe { chain.submit(ring) };
        reserved.finish_submission(result)
    }

    pub(in crate::backend::uring) fn submit_tuned_connect<'owner, 'd>(
        &mut self,
        ring: &mut ring::Ready,
        fd: &handles::Descriptor<'d>,
        options: option::StreamOptions,
        target: route::Token,
        terminal: backend::Captured<'owner, squeue::Entry>,
    ) -> Result<(), driver::SubmitError> {
        let Some(reserved) = self.states.reserve(fd.token_index(), options, target) else {
            return Err(driver::SubmitError);
        };
        let transaction = reserved.transaction();
        let chain = reserved
            .state()
            .tuned_connect(fd.slot(), options, transaction, terminal);
        let result = unsafe { chain.submit(ring) };
        reserved.finish_submission(result)
    }

    pub(in crate::backend::uring) fn cancel(
        &mut self,
        ring: &mut ring::Ready,
        slot: route::SlotIndex,
    ) -> Result<(), driver::SubmitError> {
        let transaction = controls::Tuning::new(slot);
        let Some(binding) = self.states.bind(transaction) else {
            use std::process::abort;
            abort();
        };
        let Some(occupied) = binding.occupied() else {
            return Ok(());
        };
        occupied.cancel(transaction, ring)
    }

    pub(in crate::backend::uring) fn complete(
        &mut self,
        transaction: controls::Tuning,
        result: i32,
    ) -> Option<(route::Token, i32)> {
        self.states.occupied(transaction)?.complete(result)
    }

    pub(in crate::backend::uring) fn is_quiescent(&self) -> bool {
        self.states.is_quiescent()
    }
}

impl States {
    fn reserve(
        &mut self,
        slot: route::SlotIndex,
        options: option::StreamOptions,
        target: route::Token,
    ) -> Option<Reserved<'_>> {
        let mut binding = self.bind(controls::Tuning::new(slot))?;
        if !binding.state.as_mut().reserve(options, target) {
            return None;
        }
        Some(Reserved(binding))
    }

    fn occupied(&mut self, transaction: controls::Tuning) -> Option<Occupied<'_>> {
        self.bind(transaction)?.occupied()
    }

    fn bind(&mut self, transaction: controls::Tuning) -> Option<Binding<'_>> {
        let state = self.0.get_mut(transaction.slot().raw() as usize)?;
        Some(Binding { transaction, state })
    }

    fn is_quiescent(&self) -> bool {
        self.0.iter().all(|state| state.is_vacant())
    }
}

impl<'state> Binding<'state> {
    fn occupied(self) -> Option<Occupied<'state>> {
        (!self.state.as_ref().is_vacant()).then_some(Occupied(self))
    }
}

impl Reserved<'_> {
    fn transaction(&self) -> controls::Tuning {
        self.0.transaction
    }

    fn state(&self) -> pin::Pin<&tuning::raw::State> {
        self.0.state.as_ref()
    }

    fn finish_submission(
        mut self,
        result: Result<(), driver::SubmitError>,
    ) -> Result<(), driver::SubmitError> {
        if result.is_err() {
            self.0.state.as_mut().reset();
        }
        result
    }
}

impl Occupied<'_> {
    fn cancel(
        self,
        transaction: controls::Tuning,
        ring: &mut ring::Ready,
    ) -> Result<(), driver::SubmitError> {
        self.0.state.as_ref().cancel(transaction, ring)
    }

    fn complete(mut self, result: i32) -> Option<(route::Token, i32)> {
        self.0.state.as_mut().complete(result)
    }
}
