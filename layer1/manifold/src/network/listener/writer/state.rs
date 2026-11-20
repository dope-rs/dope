use std::{mem, pin::Pin};

use dope_core::driver::route::{Identity, KeyTag};
use dope_net::link::{Consumed, egress::data};
use memory::access;
use o3::{buffer::resident, mem::slot};

use crate::network::listener::{conn, handler::{Application, Disposition}, writer::memory};

pub(in crate::network::listener) struct Send<
    'p,
    's,
    'd,
    ID: Identity,
    B: data::raw::Payload<'d> = resident::Shared<'d>,
> {
    state: State<'p, 's, 'd, ID, B>,
}

enum State<'p, 's, 'd, ID: Identity, B: data::raw::Payload<'d>> {
    Queued,
    Fixed(&'p memory::raw::Home<memory::Frame<'d, B>>),
    Direct(access::Direct<'p, 's, 'd, ID, B>),
    Failed(
        conn::Transmit,
        Option<&'p memory::raw::Home<memory::Frame<'d, B>>>,
    ),
}

impl<'d, ID: Identity, B: data::raw::Payload<'d>> State<'_, '_, 'd, ID, B> {
    fn retire<'out>(self, place: slot::Vacant<'out, B>) -> Option<slot::Occupied<'out, B>> {
        match self {
            Self::Direct(direct) => direct.retire(place),
            _ => None,
        }
    }
}

#[repr(u8)]
pub(in crate::network::listener::writer) enum Mode {
    Queued,
    Direct,
}

impl<'p, 's, 'd, ID: Identity, B: data::raw::Payload<'d>> Send<'p, 's, 'd, ID, B> {
    pub(in crate::network::listener) fn new(
        home: Option<&'p memory::raw::Home<memory::Frame<'d, B>>>,
    ) -> Self {
        Self {
            state: home.map_or(State::Queued, State::Fixed),
        }
    }

    pub(in crate::network::listener) fn home(
        &self,
    ) -> Option<&'p memory::raw::Home<memory::Frame<'d, B>>> {
        match self.state {
            State::Fixed(home) => Some(home),
            State::Failed(_, home) => home,
            State::Queued | State::Direct(_) => None,
        }
    }

    pub(in crate::network::listener) fn begin(
        &mut self,
        direct: access::Direct<'p, 's, 'd, ID, B>,
    ) {
        self.state = State::Direct(direct);
    }

    pub(in crate::network::listener::writer) fn direct(
        &mut self,
    ) -> Option<&mut access::Direct<'p, 's, 'd, ID, B>> {
        match &mut self.state {
            State::Direct(direct) => Some(direct),
            State::Queued | State::Fixed(_) | State::Failed(..) => None,
        }
    }

    pub(in crate::network::listener) fn error(&self) -> Option<conn::Transmit> {
        match &self.state {
            State::Queued | State::Fixed(_) => None,
            State::Direct(state) => state.progress.error(),
            State::Failed(error, _) => Some(*error),
        }
    }

    pub(in crate::network::listener) fn finish(&mut self) -> (Option<B>, Option<conn::Transmit>) {
        let error = self.error();
        let mut place = mem::MaybeUninit::uninit();
        let body =
            mem::replace(&mut self.state, State::Queued).retire(slot::Vacant::new(&mut place));
        (body.map(slot::Occupied::into_inner), error)
    }

    pub(in crate::network::listener::writer) fn settle<'out>(
        &mut self,
        consumed: Option<Consumed>,
        place: slot::Vacant<'out, B>,
    ) -> Option<(Disposition, Option<slot::Occupied<'out, B>>)> {
        let State::Direct(direct) = &mut self.state else {
            return Some((Disposition::Closed, None));
        };
        match direct.progress.complete(consumed)? {
            Disposition::Sent => {
                let next = Self::new(direct.lease.home());
                Some((Disposition::Sent, mem::replace(self, next).state.retire(place)))
            }
            Disposition::Closed => Some((Disposition::Closed, self.take(None, place))),
        }
    }

    pub(in crate::network::listener::writer) fn take<'out>(
        &mut self,
        failure: Option<conn::Transmit>,
        place: slot::Vacant<'out, B>,
    ) -> Option<slot::Occupied<'out, B>> {
        let home = match &self.state {
            State::Direct(direct) => direct.lease.home(),
            _ => self.home(),
        };
        let next = match self.error().or(failure) {
            Some(error) => State::Failed(error, home),
            None => Self::new(home).state,
        };
        mem::replace(&mut self.state, next).retire(place)
    }

    pub(in crate::network::listener::writer) fn release<A: Application<'d, ID, Egress = B>>(
        &mut self,
        app: Pin<&mut A>,
        id: conn::Id<'d, ID>,
        failure: Option<conn::Transmit>,
        disposition: Disposition,
    ) {
        let mut place = mem::MaybeUninit::uninit();
        if let Some(body) = self.take(failure, slot::Vacant::new(&mut place)) {
            app.released(id, body.into_inner(), disposition);
        }
    }

    pub(in crate::network::listener::writer) fn motion(&self) -> access::Motion {
        match &self.state {
            State::Queued | State::Fixed(_) | State::Failed(..) => access::Motion::Empty,
            State::Direct(state) => state.progress.motion(),
        }
    }

    pub(in crate::network::listener::writer) const fn mode(&self) -> Mode {
        match self.state {
            State::Queued | State::Fixed(_) => Mode::Queued,
            State::Direct(_) | State::Failed(..) => Mode::Direct,
        }
    }
}

const _: () = assert!(
    mem::size_of::<Send<'static, 'static, 'static, KeyTag<0>>>()
        == mem::size_of::<
            access::Direct<'static, 'static, 'static, KeyTag<0>, resident::Shared<'static>>,
        >()
);
const _: () = {
    assert!(mem::size_of::<Mode>() == mem::size_of::<bool>());
    assert!(mem::align_of::<Mode>() == mem::align_of::<bool>());
};
