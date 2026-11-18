use std::marker;

use dope_core::{
    driver::{self, result, retained::flight, route, route::Identity, route::kind},
    io::{fd::handles, transfer},
};
use o3::{
    depot::{index::Initialized, raw::Proof},
    mem::{raw::Vacant, slot::Storage},
};

use crate::{
    link::{
        duplex::{ops, state},
        event::SendSource,
        slot::types,
    },
    wire::{
        self,
        send::{self, reclaim},
    },
};

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(in crate::link) enum Source {
    Direct,
    Queued,
    Terminal,
}

impl From<Option<Source>> for SendSource {
    fn from(source: Option<Source>) -> Self {
        match source {
            Some(Source::Direct) => Self::Direct,
            Some(Source::Queued) => Self::Queued,
            Some(Source::Terminal) => Self::Terminal,
            None => Self::WriteShutdown,
        }
    }
}

pub(in crate::link) enum Invalid {
    Excess {
        sent: transfer::Len,
        limit: transfer::Len,
        origin: SendSource,
    },
}

pub(in crate::link) enum Shutdown {
    Armed,
    Busy,
    Rejected(result::SubmitError),
}

pub(in crate::link) struct Data {
    bytes: transfer::NonZero,
    source: Source,
}

pub(in crate::link) enum Inflight {
    Data(Data),
    Shutdown,
}

impl Inflight {
    pub(in crate::link) fn source(&self) -> Option<Source> {
        match self {
            Self::Data(data) => Some(data.source),
            Self::Shutdown => None,
        }
    }

    fn validate(&self, sent: transfer::Len) -> Result<(), Invalid> {
        let limit = match self {
            Self::Data(data) => transfer::Len::from_nonzero(data.bytes),
            Self::Shutdown => transfer::Len::ZERO,
        };
        if sent > limit {
            Err(Invalid::Excess {
                sent,
                limit,
                origin: self.source().into(),
            })
        } else {
            Ok(())
        }
    }

    pub(in crate::link) fn try_complete(
        self,
        life: &mut state::State,
        sent: transfer::Len,
    ) -> Result<send::Sent, Invalid> {
        self.validate(sent)?;
        if let Self::Shutdown = self {
            life.close.finish_write();
        }
        Ok(send::Sent::new(sent))
    }
}

struct Active<'d> {
    inflight: Inflight,
    flight: flight::Flight<'d, kind::Send>,
}

pub(in crate::link) struct Sending<'d, P: reclaim::Policy = reclaim::OnComplete> {
    active: Storage<Active<'d>>,
    policy: marker::PhantomData<P>,
}

pub(in crate::link) struct Submission<'a, 'd, P: reclaim::Policy> {
    sending: &'a mut Sending<'d, P>,
    lifecycle: &'a mut state::State,
    source: Source,
}

#[must_use]
pub(in crate::link) enum Pending<'a, 'd, P: reclaim::Policy> {
    Ready(Seal<'a, 'd, P>),
    Inflight,
    Missing,
    Duplicate,
}

#[must_use]
pub(in crate::link) struct Seal<'a, 'd, P: reclaim::Policy>(Ready<'a, 'd, P>);

/// Exclusive idle-send admission retained until submission.
pub(in crate::link) struct Ready<'a, 'd, P: reclaim::Policy> {
    active: Vacant<'a, Active<'d>>,
    lifecycle: &'a mut state::State,
    source: Source,
    policy: marker::PhantomData<P>,
}

impl<'a, 'd, P: reclaim::Policy> Submission<'a, 'd, P> {
    pub(super) fn new(
        state: &'a mut Sending<'d, P>,
        life: &'a mut state::State,
        source: Source,
    ) -> Self {
        Self {
            sending: state,
            lifecycle: life,
            source,
        }
    }

    pub(in crate::link) fn pending(self) -> Pending<'a, 'd, P> {
        let Some(ready) = self.try_ready() else {
            return Pending::Inflight;
        };
        match ready.lifecycle.close.check_seal() {
            state::Seal::Ready => Pending::Ready(Seal(ready)),
            state::Seal::Missing => Pending::Missing,
            state::Seal::Duplicate => Pending::Duplicate,
        }
    }

    pub(in crate::link) fn try_ready(self) -> Option<Ready<'a, 'd, P>> {
        Some(Ready {
            active: self.sending.active.vacant()?,
            lifecycle: self.lifecycle,
            source: self.source,
            policy: marker::PhantomData,
        })
    }

    pub(in crate::link) fn submit<
        'input,
        Tag: route::Tag + route::Select<kind::Send>,
        B: send::Payload<'input>,
    >(
        self,
        fd: &handles::Descriptor<'d>,
        slots: &flight::Slots<'d, Tag>,
        driver: &mut driver::Context<'_, 'd>,
        target: route::Target<'d, Tag>,
        ready: send::Prepared<'input, P, B>,
    ) -> send::Attempt<P>
    where
        'd: 'input,
    {
        match self.try_ready() {
            Some(submit) => submit.submit(fd, slots, driver, target, ready),
            None => send::Attempt::idle(ready.into_parts().1),
        }
    }
}

impl<'a, 'd, P: reclaim::Policy> Ready<'a, 'd, P> {
    pub(in crate::link) fn submit<
        'input,
        Tag: route::Tag + route::Select<kind::Send>,
        B: send::Payload<'input>,
    >(
        self,
        fd: &handles::Descriptor<'d>,
        slots: &flight::Slots<'d, Tag>,
        driver: &mut driver::Context<'_, 'd>,
        target: route::Target<'d, Tag>,
        ready: send::Prepared<'input, P, B>,
    ) -> send::Attempt<P>
    where
        'd: 'input,
    {
        let waiting = ready.is_waiting();
        self.lifecycle.wait_send(waiting);
        let (data, credit, close) = ready.into_parts();
        if waiting {
            return send::Attempt::idle(credit);
        }
        let selected = send::Payload::select(data);
        let ready = match selected {
            None => {
                match close {
                    send::End::Keep => {}
                    send::End::Close => self.lifecycle.drain(),
                }
                return send::Attempt::idle(credit);
            }
            Some(ready) => ready,
        };
        let send::Submitted { bytes, flight } =
            match send::raw::submit(ready, fd, target, slots, driver.reborrow()) {
                Ok(sent) => sent,
                Err(error) => return send::Attempt::rejected(credit, error),
            };
        match close {
            send::End::Keep => {}
            send::End::Close => self.lifecycle.drain(),
        };
        let active = Inflight::Data(Data {
            bytes,
            source: self.source,
        });
        self.active.insert(Active {
            inflight: active,
            flight,
        });
        send::Attempt::submitted(credit)
    }
}

impl<'a, 'd, P: reclaim::Policy> Seal<'a, 'd, P> {
    pub(in crate::link) fn seal(self, waiting: bool) -> Ready<'a, 'd, P> {
        let submit = self.0;
        if !waiting {
            submit.lifecycle.close.seal();
        }
        submit
    }
}

impl<'w, 'd: 'w, ID: Identity + 'w, W: wire::Spec + 'w, S>
    types::Connection<'w, 'd, ID, W, S>
{
    pub(in crate::link) fn observe_send(
        &mut self,
        target: route::Erased<'d, kind::Send>,
        before: impl FnOnce(&mut Self),
    ) -> Option<Inflight> {
        let active = &self.control.engine.operations.sending.active;
        if !active.get()?.flight.matches(target) {
            return None;
        }
        before(self);
        // SAFETY: the callback cannot retire this private matched flight. The
        // exclusive pool loan and route claim prevent replacing its connection.
        let active = unsafe {
            Proof::<_, Initialized<Active<'d>>>::new((
                &mut self.control.engine.operations.sending.active,
                (),
            ))
        }
        .into();
        let (active, _) = active.take();
        drop(active.flight);
        Some(active.inflight)
    }
}

impl<'d, P: reclaim::Policy> Sending<'d, P> {
    pub(super) fn new() -> Self {
        Self {
            active: Storage::new(()),
            policy: marker::PhantomData,
        }
    }

    pub(in crate::link) fn is_inflight(&self) -> bool {
        self.active.get().is_some()
    }

    pub(in crate::link) fn origin(&self) -> Option<SendSource> {
        self.active.get().map(|active| active.inflight.source().into())
    }

    pub(super) fn cancel(&mut self) -> ops::Cancel<'_, 'd, kind::Send> {
        match self.active.get_mut() {
            Some(active) => ops::Cancel::Armed(&mut active.flight),
            None => ops::Cancel::Idle,
        }
    }

    pub(in crate::link) fn shutdown<Tag: route::Tag + route::Select<kind::Send>>(
        &mut self,
        driver: &mut driver::Context<'_, 'd>,
        slots: &flight::Slots<'d, Tag>,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
    ) -> Shutdown {
        let Some(active) = self.active.vacant() else {
            return Shutdown::Busy;
        };
        let flight = match slots.shutdown(driver.reborrow(), fd, target) {
            Ok(flight) => flight,
            Err(error) => return Shutdown::Rejected(error),
        };
        active.insert(Active {
            inflight: Inflight::Shutdown,
            flight,
        });
        Shutdown::Armed
    }
}

const _: () = {
    use std::mem;

    assert!(mem::size_of::<Inflight>() == mem::size_of::<u64>());
    assert!(
        mem::size_of::<Ready<'static, 'static, reclaim::OnComplete>>()
            == mem::size_of::<Submission<'static, 'static, reclaim::OnComplete>>()
    );
    assert!(mem::size_of::<Sending<'static>>() == mem::size_of::<Active<'static>>());
    assert!(
        mem::size_of::<Sending<'static, reclaim::OnSubmit>>() == mem::size_of::<Sending<'static>>()
    );
    assert!(
        mem::size_of::<Pending<'static, 'static, reclaim::OnComplete>>()
            == mem::size_of::<Submission<'static, 'static, reclaim::OnComplete>>()
    );
    assert!(
        mem::size_of::<Seal<'static, 'static, reclaim::OnComplete>>()
            == mem::size_of::<Submission<'static, 'static, reclaim::OnComplete>>()
    );
};
