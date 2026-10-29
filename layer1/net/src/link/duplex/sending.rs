use std::marker;

use dope_core::{
    driver::{result, retained, retained::flight, route, route::kind},
    io::{fd::handles, transfer},
};

use crate::{
    link::{
        duplex::{ops, state},
        event::SendSource,
    },
    wire::send::{self, reclaim},
};

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(in crate::link) enum Source {
    Direct,
    Queued,
    Terminal,
}

impl From<Source> for SendSource {
    fn from(source: Source) -> Self {
        match source {
            Source::Direct => Self::Direct,
            Source::Queued => Self::Queued,
            Source::Terminal => Self::Terminal,
        }
    }
}

pub(in crate::link) struct Completion {
    sent: send::Sent,
    origin: SendSource,
}

impl Completion {
    pub(in crate::link) fn into_parts(self) -> (send::Sent, SendSource) {
        (self.sent, self.origin)
    }
}

pub(in crate::link) enum Invalid {
    Stale {
        sent: transfer::Len,
    },
    Excess {
        sent: transfer::Len,
        limit: transfer::Len,
        origin: SendSource,
    },
}

pub(in crate::link) enum Done {
    Idle,
    Active(SendSource),
}

pub(in crate::link) enum Shutdown {
    Armed,
    Busy,
    Rejected(result::SubmitError),
}

struct Data {
    bytes: transfer::NonZero,
    source: Source,
}

enum Inflight {
    Data(Data),
    Shutdown,
}

impl Inflight {
    fn origin(&self) -> SendSource {
        match self {
            Self::Data(data) => data.source.into(),
            Self::Shutdown => SendSource::WriteShutdown,
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
                origin: self.origin(),
            })
        } else {
            Ok(())
        }
    }

    fn complete(self, life: &mut state::State, sent: transfer::Len) -> Completion {
        if let Self::Shutdown = self {
            life.close.finish_write();
        }
        Completion {
            sent: send::Sent::new(sent),
            origin: self.origin(),
        }
    }
}

struct Active<'d> {
    inflight: Inflight,
    flight: flight::Flight<'d, kind::Send>,
}

pub(in crate::link) struct Sending<'d, P: reclaim::Policy = reclaim::OnComplete> {
    active: Option<Active<'d>>,
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
#[repr(transparent)]
pub(in crate::link) struct Ready<'a, 'd, P: reclaim::Policy>(Submission<'a, 'd, P>);

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
        match ready.0.lifecycle.close.check_seal() {
            state::Seal::Ready => Pending::Ready(Seal(ready)),
            state::Seal::Missing => Pending::Missing,
            state::Seal::Duplicate => Pending::Duplicate,
        }
    }

    pub(in crate::link) fn try_ready(self) -> Option<Ready<'a, 'd, P>> {
        (!self.sending.is_inflight()).then_some(Ready(self))
    }

    pub(in crate::link) fn submit<
        'input,
        'owner,
        Tag: route::Tag + route::Select<kind::Send>,
        B: send::Payload<'input>,
    >(
        self,
        fd: &handles::Descriptor<'d>,
        slots: &flight::Slots<'d, Tag>,
        driver: &mut retained::Context<'_, 'owner, 'd>,
        target: route::Target<'d, Tag>,
        ready: send::Prepared<'input, P, B>,
    ) -> send::Attempt<P>
    where
        'd: 'owner,
        'owner: 'input,
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
        'owner,
        Tag: route::Tag + route::Select<kind::Send>,
        B: send::Payload<'input>,
    >(
        self,
        fd: &handles::Descriptor<'d>,
        slots: &flight::Slots<'d, Tag>,
        driver: &mut retained::Context<'_, 'owner, 'd>,
        target: route::Target<'d, Tag>,
        ready: send::Prepared<'input, P, B>,
    ) -> send::Attempt<P>
    where
        'd: 'owner,
        'owner: 'input,
    {
        let Self(submit) = self;
        let waiting = ready.is_waiting();
        submit.lifecycle.wait_send(waiting);
        let (data, credit, close) = ready.into_parts();
        if waiting {
            return send::Attempt::idle(credit);
        }
        let selected = send::Payload::select(&data);
        let ready = match selected {
            None => {
                match close {
                    send::End::Keep => {}
                    send::End::Close => submit.lifecycle.drain(),
                }
                return send::Attempt::idle(credit);
            }
            Some(ready) => ready,
        };
        let send::Submitted { bytes, flight } =
            match send::raw::Submission(ready).submit(fd, target, slots, driver.reborrow()) {
                Ok(sent) => sent,
                Err(error) => return send::Attempt::rejected(credit, error),
            };
        match close {
            send::End::Keep => {}
            send::End::Close => submit.lifecycle.drain(),
        };
        let active = Inflight::Data(Data {
            bytes,
            source: submit.source,
        });
        submit.sending.active = Some(Active {
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
            submit.0.lifecycle.close.seal();
        }
        submit
    }
}

impl<'d, P: reclaim::Policy> ops::State<'d, P> {
    /// Retires the terminal native flight before validating its byte count.
    pub(in crate::link) fn try_complete_send(
        &mut self,
        sent: transfer::Len,
    ) -> Result<Completion, Invalid> {
        let Some(active) = self.sending.active.take() else {
            return Err(Invalid::Stale { sent });
        };
        drop(active.flight);
        active.inflight.validate(sent)?;
        Ok(active.inflight.complete(&mut self.lifecycle, sent))
    }
}

impl<'d, P: reclaim::Policy> Sending<'d, P> {
    pub(super) fn new() -> Self {
        Self {
            active: None,
            policy: marker::PhantomData,
        }
    }

    pub(in crate::link) fn is_inflight(&self) -> bool {
        self.active.is_some()
    }

    pub(in crate::link) fn origin(&self) -> Option<SendSource> {
        self.active.as_ref().map(|active| active.inflight.origin())
    }

    pub(super) fn cancel(&mut self) -> ops::Cancel<'_, 'd, kind::Send> {
        match &mut self.active {
            Some(active) => ops::Cancel::Armed(&mut active.flight),
            None => ops::Cancel::Idle,
        }
    }

    pub(in crate::link) fn shutdown<Tag: route::Tag + route::Select<kind::Send>>(
        &mut self,
        driver: &mut retained::Context<'_, '_, 'd>,
        slots: &flight::Slots<'d, Tag>,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
    ) -> Shutdown {
        if self.is_inflight() {
            return Shutdown::Busy;
        }
        let flight = match slots.shutdown(driver.driver().reborrow(), fd, target) {
            Ok(flight) => flight,
            Err(error) => return Shutdown::Rejected(error),
        };
        self.active = Some(Active {
            inflight: Inflight::Shutdown,
            flight,
        });
        Shutdown::Armed
    }

    pub(super) fn done(&mut self) -> Done {
        match self.active.take() {
            Some(active) => {
                drop(active.flight);
                Done::Active(active.inflight.origin())
            }
            None => Done::Idle,
        }
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
    assert!(mem::size_of::<Completion>() == mem::size_of::<(send::Sent, SendSource)>());
};
