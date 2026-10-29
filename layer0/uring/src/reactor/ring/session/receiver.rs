use std::{marker, os::fd};

use dope_sys::{
    io::request::Lower as _,
    memory, probe,
    probe::{
        Context as _, Stopped,
        cleanup::{Cleanup as _, Rejection, Release as _},
        receive,
        receive::Context as _,
        session::{Empty as _, Occupied as _, Pending as _, Session as _},
    },
    table::token,
};

use crate::{
    reactor::{
        buffers::Payload,
        register,
        ring::{
            Recover, Released, Session, session,
            session::{Fault, GROUP, Identity, Inspect, Inspected, Update},
        },
        sq,
    },
    sockets::probe::Pair,
};

type Occupied<T, L> = Session<T, L, register::raw::Occupied>;
type Pending<T, L> = session::Pending<T, L, register::raw::Occupied>;
type Stage<T, L> = probe::State<Occupied<T, L>, Pending<T, L>>;
type Failure<T, L, P, R> =
    Rejection<Result<P, Recover<T, L, P>>, Result<(R, sq::Fixed), Released<T, L, R>>>;

const PAYLOADS: [&[u8]; 3] = [b"a", b"b", b"c"];

/// The installed reader and its peer live through every receive observation.
pub(super) struct Receiver<T, L: memory::raw::Canary, P> {
    state: Occupied<T, L>,
    pair: Pair,
    mode: receive::Mode,
    marker: marker::PhantomData<fn() -> P>,
}

impl<T, L, P> Receiver<T, L, P>
where
    T: token::Codec<Context: receive::Context<Payload>>,
    L: memory::raw::Canary,
    P: Copy
        + From<Fault<T>>
        + From<Inspected<T, L, Inspect<T>>>
        + From<probe::Syscall<Identity<T>, dope_sys::Errno>>
        + From<probe::Syscall<Identity<T>, probe::Creation>>
        + From<receive::Install<Update>>
        + From<(Identity<T>, receive::Send)>,
{
    pub(super) fn new(
        state: Session<T, L>,
        mode: receive::Mode,
    ) -> Result<Self, Result<P, Recover<T, L, P>>> {
        let pair = match Pair::new() {
            Ok(pair) => pair,
            Err(error) => {
                let primary = probe::Syscall {
                    probe: Self::probe(mode),
                    error,
                };
                return Err(Recover::session(state, primary.into()));
            }
        };
        let stopped = match state.install(fd::AsFd::as_fd(&pair)) {
            Ok(state) => {
                return Ok(Self {
                    state,
                    pair,
                    mode,
                    marker: marker::PhantomData,
                });
            }
            Err(stopped) => stopped,
        };
        Err(Recover::session(
            stopped.state,
            receive::Install {
                mode,
                error: stopped.cause,
            }
            .into(),
        ))
    }

    pub(super) fn run<R: Copy>(
        self,
        release: &impl Fn(receive::Mode, Update) -> R,
    ) -> Result<Session<T, L>, Failure<T, L, P, R>> {
        let Self {
            state,
            pair,
            mode,
            marker: marker::PhantomData,
        } = self;
        let cleanup = |stopped: Stopped<Stage<T, L>, P>| {
            Rejection::Cleanup(Recover::receive(stopped.state, stopped.cause))
        };
        let probe = Self::probe(mode);
        let sent = pair.send(PAYLOADS[0]).map_err(|cause| (probe, cause));
        let (state, ()) = Stopped::attach(state, sent)
            .map_err(Stopped::idle)
            .map_err(Stopped::convert)
            .map_err(cleanup)?;
        let entry = match mode {
            receive::Mode::Data => sq::Entry::recv::<GROUP>(state.slot()),
            receive::Mode::Message => sq::Entry::recv_msg::<GROUP>(state.slot()),
        };
        let entry = entry.user_data(T::probe(probe));
        let active = state
            .submit(&entry, probe)
            .map_err(Stopped::idle)
            .map_err(Stopped::convert)
            .map_err(cleanup)?;
        let mut active = Occupied::next(active, |bytes| {
            T::Context::inspect(mode, bytes, PAYLOADS[0])
        })
        .map_err(Stopped::convert)
        .map_err(cleanup)?;
        for bytes in &PAYLOADS[1..] {
            let sent = pair.send(bytes).map_err(|cause| (probe, cause));
            let (state, ()) = Stopped::attach(active, sent)
                .map_err(Stopped::active)
                .map_err(Stopped::convert)
                .map_err(cleanup)?;
            active = state;
            active = Occupied::next(active, |data| T::Context::inspect(mode, data, bytes))
                .map_err(Stopped::convert)
                .map_err(cleanup)?;
        }
        let state = active.cancel().map_err(Stopped::convert).map_err(cleanup)?;
        let stopped = match state.try_remove() {
            Ok(state) => return Ok(state),
            Err(stopped) => stopped,
        };
        Err(Rejection::Release(Released::session(
            stopped.state,
            release(mode, stopped.cause),
        )))
    }

    fn probe(mode: receive::Mode) -> Identity<T> {
        match mode {
            receive::Mode::Data => T::Context::RECEIVE,
            receive::Mode::Message => T::Context::MESSAGE,
        }
    }
}
