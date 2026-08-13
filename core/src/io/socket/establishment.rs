use std::{marker, mem};

use crate::{
    driver::{
        flight,
        route::{self, kind},
    },
    io::{
        event::{connect, tuning},
        fd::handles,
    },
};

pub(crate) enum CancelTarget<'a, 'd> {
    Tuning(&'a mut handles::Descriptor<'d>),
    Connect(&'a mut flight::Flight<'d>),
}

#[doc(hidden)]
pub struct Live;

#[doc(hidden)]
pub struct Cancelled;

#[doc(hidden)]
#[must_use = "a pending establishment owns its descriptor and completion authority"]
#[repr(C)]
pub struct TuningPending<'d, State = Live> {
    pub(crate) fd: handles::Descriptor<'d>,
    pub(crate) target: route::Erased<'d>,
    pub(crate) state: marker::PhantomData<State>,
}

#[doc(hidden)]
#[must_use = "a pending establishment owns its descriptor and completion authority"]
#[repr(C)]
pub struct ConnectionPending<'d, State = Live> {
    pub(crate) fd: handles::Descriptor<'d>,
    pub(crate) target: route::Erased<'d>,
    pub(crate) flight: Option<flight::Flight<'d>>,
    pub(crate) state: marker::PhantomData<State>,
}

impl<'d, State> TuningPending<'d, State> {
    #[doc(hidden)]
    pub const fn fd(&self) -> &handles::Descriptor<'d> {
        &self.fd
    }
}

impl<'d> TuningPending<'d> {
    pub(crate) fn tuning<Tag: route::Tag>(
        fd: handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
    ) -> Self {
        Self {
            fd,
            target: target.operation(kind::TUNING).erase(),
            state: marker::PhantomData,
        }
    }
}

impl<'d, State> ConnectionPending<'d, State> {
    #[doc(hidden)]
    pub const fn fd(&self) -> &handles::Descriptor<'d> {
        &self.fd
    }
}

impl<'d> ConnectionPending<'d> {
    pub(crate) fn connect(fd: handles::Descriptor<'d>, flight: flight::Flight<'d>) -> Self {
        Self {
            fd,
            target: flight.target_erased(),
            flight: Some(flight),
            state: marker::PhantomData,
        }
    }

    pub(crate) fn tuned_connect<Tag: route::Tag>(
        fd: handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
    ) -> Self {
        Self {
            fd,
            target: target.operation(kind::CONNECT).erase(),
            flight: None,
            state: marker::PhantomData,
        }
    }
}

impl<'d, State> TuningPending<'d, State> {
    #[doc(hidden)]
    pub fn complete_tuning(
        self,
        completion: tuning::Completion,
    ) -> Result<(handles::Descriptor<'d>, tuning::Outcome), (Self, tuning::Completion)> {
        if !self.target.matches(completion.token()) {
            return Err((self, completion));
        }
        let (_, outcome) = completion.into_parts();
        Ok((self.fd, outcome))
    }
}

impl<'d, State> ConnectionPending<'d, State> {
    #[doc(hidden)]
    pub fn complete_connection(
        mut self,
        completion: connect::Completion,
    ) -> Result<(handles::Descriptor<'d>, connect::Outcome), (Self, connect::Completion)> {
        if !self.target.matches(completion.token()) {
            return Err((self, completion));
        }
        if let Some(flight) = self.flight.take() {
            let _ = flight.complete();
        }
        let (_, outcome) = completion.into_parts();
        Ok((self.fd, outcome))
    }
}

const _: () = {
    assert!(mem::size_of::<Live>() == 0);
    assert!(mem::size_of::<Cancelled>() == 0);
    assert!(
        mem::size_of::<TuningPending<'static>>()
            == mem::size_of::<(handles::Descriptor<'static>, route::Erased<'static>)>()
    );
    assert!(
        mem::size_of::<ConnectionPending<'static>>()
            == mem::size_of::<(
                handles::Descriptor<'static>,
                route::Erased<'static>,
                Option<flight::Flight<'static>>,
            )>()
    );
    assert!(
        mem::size_of::<TuningPending<'static, Cancelled>>()
            == mem::size_of::<TuningPending<'static>>()
    );
    assert!(
        mem::size_of::<ConnectionPending<'static, Cancelled>>()
            == mem::size_of::<ConnectionPending<'static>>()
    );
};
