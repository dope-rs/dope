use std::{num, ops::ControlFlow, os::fd, task};

use dope_sys::{
    engine,
    engine::{life, life::Error, ring, shutdown::Shutdown},
    io::{
        Close,
        reactor::{
            Full, complete,
            complete::packet::record,
            submit,
            submit::{Control, Poll as _, Queue as _, Ready as _},
        },
        request,
        socket::option::tuning::Quiesce as _,
    },
    memory,
    memory::buffer::provided,
    probe::Stopped,
    ring::{
        Source, enter, ready,
        ready::{Complete as _, Ready as _},
    },
    table,
    table::{
        closed,
        reclaim::{Item, Kind, View},
        token::{Close as _, Codec, Tuning, host},
    },
};
use engine::admit::{Admit, Backend, Context};

use crate::{
    Uring,
    driver::{Closing, Key, guards::Cancellation, shutdown, tuning, tuning::Progress as _},
    reactor::{ring::Ready, sq},
};

pub(super) mod turn;

type Identity<T> = <T as Codec>::Context;

/// The registered ring keeps every pinned option value alive until teardown.
pub struct Engine<C: ring::Context> {
    pub(in crate::driver) ring: Ready<C::Token, C::Layout>,
    pub(in crate::driver) tuning: C::Tuning,
}

/// The exact resolver after every request and descriptor reaches its terminal state.
pub struct Quiesced<R>(R);

impl<R: life::Reclaim> life::Quiesced for Quiesced<R> {
    type Resolver = R;

    fn into_inner(self) -> R {
        self.0
    }
}

impl<C> engine::Complete for Engine<C>
where
    C: ring::Context,
    C::Tuning: tuning::Progress<C::Token>,
    C::Map: record::Map<
            Parts = complete::Parts<
                C::Token,
                <<Uring as Source>::Entry<C::Token> as complete::Entry>::Outcome,
                <<Uring as Source>::Entry<C::Token> as complete::Entry>::Flags,
            >,
            Fault: From<(<Uring as provided::Source>::Index, C::Layout)>,
        >,
{
    type Map = C::Map;
    type Pending =
        <Uring as provided::Source>::Pending<<C::Layout as memory::raw::Receive>::Extent>;

    fn next(&mut self) -> Option<Option<<C::Map as complete::Map>::Entry<Self::Pending>>> {
        use complete::{
            Entry as _,
            packet::record::{Map as _, Selection},
        };
        match self.tuning.next(self.ring.submission()) {
            Some(Some(parts)) => return Some(Some(C::Map::new(parts, Selection::None))),
            Some(None) => return Some(None),
            None => {}
        }
        let (entry, buffer) = self.ring.pop()?;
        Some(self.tuning.observe(entry.into_parts()).map(|parts| {
            let buffer = match buffer {
                None => Selection::None,
                Some(Ok(selected)) => Selection::Selected(selected),
                Some(Err(index)) => Selection::Invalid((index, self.ring.layout()).into()),
            };
            C::Map::new(parts, buffer)
        }))
    }
}

impl<C: ring::Context> engine::Buffer for Engine<C> {
    type Layout = C::Layout;
    type Token = <Uring as provided::Source>::Complete<<C::Layout as memory::raw::Receive>::Extent>;
    type Context = <Uring as provided::Source>::Context<C::Layout>;
    type Selection = C::Selection;
}

impl<C, O> engine::Install<O> for Engine<C>
where
    C: ring::Context + Context<Native = Uring>,
    O: Into<<C::Inputs as request::Context>::Target>,
{
    type Handle = fd::OwnedFd;
    type Error = <Ready<C::Token, C::Layout> as ready::Ready>::InstallError;

    fn install(&mut self, slot: O, handle: Self::Handle) -> Result<(), Self::Error> {
        self.ring.install(
            <Uring as request::Source<C::Inputs>>::target(slot.into()),
            fd::AsFd::as_fd(&handle),
        )
    }
}

impl<C: ring::Context> ring::Backend<C> for Engine<C> {
    type Ring = Ready<C::Token, C::Layout>;

    fn new(ring: Self::Ring, tuning: C::Tuning) -> Self {
        Self { ring, tuning }
    }

    fn ring(&mut self) -> &mut Self::Ring {
        &mut self.ring
    }

    fn tuning(&mut self) -> &mut C::Tuning {
        &mut self.tuning
    }

    fn split(&mut self) -> (&mut Self::Ring, &mut C::Tuning) {
        (&mut self.ring, &mut self.tuning)
    }
}

impl<C> Admit<C> for Engine<C>
where
    C: ring::Context + Context<Native = Uring, Key = Key<C::Token>>,
{
    type Admission<'a>
        = sq::raw::Vacancy<'a, C::Token>
    where
        Self: 'a;

    fn try_admit(&mut self) -> Result<Self::Admission<'_>, Full> {
        self.ring.submission().try_one()
    }
}

impl<C> Backend<C> for Engine<C>
where
    C: ring::Context + Context<Native = Uring, Key = Key<C::Token>>,
    C::Tuning: tuning::Progress<C::Token>,
{
    type Pending<'a, const FAMILY: u8>
        = Cancellation<'a, C::Token>
    where
        Self: 'a;
    type Immediate<'a>
        = Cancellation<'a, C::Token>
    where
        Self: 'a;
}

impl<C, S> engine::Cancel<S> for Engine<C>
where
    C: ring::Context,
    C::Tuning: tuning::Progress<C::Token>,
    S: Into<num::NonZero<u32>>,
    <C::Token as Codec>::Tuning: Tuning<Slot = S>,
{
    type Permit = ();

    fn cancel(&mut self, _: &Self::Permit, slot: S) -> Result<(), Full> {
        let vacancy = self.ring.submission().try_one()?;
        let target = <C::Token as Codec>::Tuning::new(slot);
        let index: num::NonZero<u32> = target.slot().into();
        let bytes = (index.get() - 1).to_le_bytes();
        let fixed = sq::Fixed::new(sq::Index::from_le([bytes[0], bytes[1], bytes[2]]));
        if let Some(stopped) = self.tuning.cancel_at(fixed, None) {
            *stopped = true;
        }
        let control = tuning::cancel::<C::Token>(target);
        submit::raw::Vacancy::prepare(vacancy, &control).push();
        Ok(())
    }
}

/// A queued retirement remains owned until the tuning terminal clears its state.
/// Cancellation replies may permit retries; only that retirement can request
/// them. Native close cannot recycle the slot before the terminal clears it.
impl<'d, C, V, const N: usize> table::Retire<'d, V> for Engine<C>
where
    C: ring::Context<Tuning = tuning::Table<Key<<C as ring::Context>::Token>, N>>
        + Context<Native = Uring>,
    V: View<
            Slot = <C::Inputs as request::Context>::Target,
            Close = <Identity<C::Token> as host::Context>::Close<'d>,
            Retire = <Identity<C::Token> as host::Context>::Retire<'d>,
        >,
    <Identity<C::Token> as host::Context>::Close<'d>: Item<Slot = V::Slot, Flight = V::Flight>,
    <Identity<C::Token> as host::Context>::Retire<'d>: Item<Slot = V::Slot, Flight = V::Flight>,
{
    type Output = closed::Submitted;

    fn retire<'a>(
        &'a mut self,
        pending: Kind<V::Close, V::Retire>,
        view: V,
    ) -> task::Poll<Self::Output>
    where
        'd: 'a,
    {
        use crate::driver::tuning::Retirement;
        let slot = match &pending {
            Kind::Close(item) => item.slot(),
            Kind::Retire(item) => item.slot(),
        };
        let target = <Uring as request::Source<C::Inputs>>::target(slot);
        match self.tuning.retirement(target) {
            Retirement::Ready => {}
            Retirement::Pending => return task::Poll::Pending,
            Retirement::Cancel(chain) => {
                return match chain.try_submit(self.ring.submission()) {
                    Ok(()) | Err(Full) => task::Poll::Pending,
                };
            }
        }
        match pending {
            Kind::Close(item) => {
                let token = C::Token::close(<C::Token as Codec>::Close::new(&item));
                let entries = [
                    Uring::shutdown(target, C::Token::prepare()),
                    Uring::close(target, token),
                ];
                let ready = match self.ring.submission().prepare(&entries) {
                    Ok(ready) => ready,
                    Err(Full) => return task::Poll::Pending,
                };
                ready.push();
                view.submitted(item.submit());
            }
            Kind::Retire(item) => {
                let token = C::Token::close(<C::Token as Codec>::Close::retire(&item));
                let entry = Uring::close(target, token);
                let ready = match self.ring.submission().try_one() {
                    Ok(ready) => ready,
                    Err(Full) => return task::Poll::Pending,
                };
                submit::raw::Vacancy::prepare(ready, &entry).push();
                view.submitted(item.submit());
            }
        }
        task::Poll::Ready(closed::Submitted)
    }
}

impl<C: ring::Context> complete::Observe for Engine<C> {
    type Observation = C::Observation;
}

impl<C: ring::Context> Close for Engine<C> {
    type State = Closing<C>;

    fn try_close(self) -> Result<(), Self::State> {
        let Self { ring, tuning } = self;
        match ring.try_close() {
            Ok(()) => Ok(()),
            Err(ring) => Err(Closing::new(ring, tuning)),
        }
    }
}

impl<C: ring::Context> Shutdown for Engine<C> {
    type State = shutdown::Shutdown<C>;

    fn try_shutdown(self, context: <Self as engine::Buffer>::Context) -> Result<(), Self::State> {
        use dope_sys::probe::Stopped;

        use crate::driver::shutdown::Shutdown;

        let (context, ()) = Stopped::attach(context, self.try_close())?;
        context.try_close().map_err(Shutdown::Context)
    }
}

mod raw;

enum Resume {
    Wait,
    Retry,
    Done,
}

impl<C: ring::Context> Engine<C> {
    /// Accepted slots belong to the kernel; no local availability proof exists.
    pub const fn has_local_accept_capacity(&self) -> bool {
        false
    }

    pub(crate) fn try_finish<'a, R>(mut resolver: R) -> Retirement<R, Quiesced<R>>
    where
        C: 'a,
        C::Tuning: tuning::Progress<C::Token>,
        R: life::Reclaim<Backend = Self> + 'a,
    {
        loop {
            let result = Self::try_retire(&mut resolver).map_err(Error::File);
            (resolver, ()) = Stopped::attach(resolver, result)?;
            resolver = Self::drain(resolver)?;
            if resolver.has_inflight() {
                let result = resolver.backend().ring.try_wait(None).map_err(Error::File);
                (resolver, ()) = Stopped::attach(resolver, result)?;
                continue;
            }
            if !resolver.has_pending() {
                break;
            }
        }
        resolver.backend().ring.flush();
        if resolver.backend().tuning.is_quiescent() {
            Ok(Quiesced(resolver))
        } else {
            Err(Stopped {
                state: resolver,
                cause: Error::Tuning,
            })
        }
    }

    /// # Safety
    /// Retain the terminal source through the next quiescence or destruction,
    /// including when this attempt returns an error.
    pub(crate) unsafe fn try_resume<'a, R>(mut resolver: R) -> Retirement<R>
    where
        C: 'a,
        C::Tuning: tuning::Progress<C::Token>,
        R: life::Resume<Backend = Self> + 'a,
    {
        use life::Terminal as _;

        loop {
            let result = Self::try_retire(&mut resolver).map_err(Error::File);
            (resolver, ()) = Stopped::attach(resolver, result)?;
            let (current, progress) = Self::drain_with(resolver, |resolver| {
                let (backend, terminal) = resolver.terminal();
                let Self { ring, tuning } = backend;
                let Some(vacancy) = ring.try_idle().map_err(Error::File)? else {
                    return Ok(ControlFlow::Continue(enter::Progress::Entered));
                };
                if terminal.has_flights() {
                    return Ok(ControlFlow::Continue(enter::Progress::Idle));
                }
                if terminal.has_inflight() {
                    return Ok(ControlFlow::Break(Resume::Wait));
                }
                if terminal.has_pending() {
                    return Ok(ControlFlow::Break(Resume::Retry));
                }
                if !tuning.is_quiescent() {
                    return Err(Error::Tuning);
                }
                if let Some(source) = terminal.source() {
                    let entry = <Uring as Control>::Poll::new(source, C::Token::shutdown());
                    // SAFETY: the caller retains this generation's source;
                    // the same idle queue still owns its admitted vacancy.
                    unsafe { Self::arm(vacancy, &entry) };
                }
                Ok(ControlFlow::Break(Resume::Done))
            })?;
            resolver = current;
            match progress {
                Resume::Wait => {
                    let result = resolver.backend().ring.try_wait(None).map_err(Error::File);
                    (resolver, ()) = Stopped::attach(resolver, result)?;
                }
                Resume::Retry => {}
                Resume::Done => return Ok(resolver),
            }
        }
    }

    /// # Safety
    /// Retain the poll's exact source until the next engine quiescence or destruction.
    unsafe fn arm(
        vacancy: sq::raw::Vacancy<'_, C::Token>,
        entry: &<Uring as Control>::Poll<'_, C::Token>,
    ) {
        // SAFETY: the caller retains the source through the native poll's lifetime.
        // These rings have no SQPOLL; the next owned poll submits this entry.
        unsafe { submit::raw::Vacancy::borrow(vacancy, entry.entry()) }.push();
    }

    fn try_retire<R>(resolver: &mut R) -> Result<(), <Uring as Source>::Poll>
    where
        C::Tuning: tuning::Progress<C::Token>,
        R: life::Resolver<Backend = Self>,
    {
        resolver.retire();
        resolver.backend().ring.submit()
    }

    pub(crate) fn drain<R>(resolver: R) -> Retirement<R>
    where
        C::Tuning: tuning::Progress<C::Token>,
        R: life::Reclaim<Backend = Self>,
    {
        Self::drain_with(resolver, |resolver| {
            match resolver.backend().ring.advance().map_err(Error::File)? {
                enter::Progress::Entered => Ok(ControlFlow::Continue(enter::Progress::Entered)),
                enter::Progress::Idle => Ok(if resolver.has_flights() {
                    ControlFlow::Continue(enter::Progress::Idle)
                } else {
                    ControlFlow::Break(())
                }),
            }
        })
        .map(|(resolver, ())| resolver)
    }

    fn drain_with<R, O>(
        mut resolver: R,
        mut advance: impl FnMut(
            &mut R,
        ) -> Result<
            ControlFlow<O, enter::Progress>,
            Error<<Uring as Source>::Poll, R::Fault>,
        >,
    ) -> Retirement<R, (R, O)>
    where
        C::Tuning: tuning::Progress<C::Token>,
        R: life::Reclaim<Backend = Self>,
    {
        loop {
            resolver.backend().ring.acquire();
            loop {
                resolver = match resolver.drain() {
                    Ok(resolver) => resolver,
                    Err(mut stopped) => {
                        stopped.state.backend().ring.publish();
                        return Err(stopped.convert());
                    }
                };
                if resolver.backend().ring.refresh().get() == 0 {
                    break;
                }
            }
            let advanced = advance(&mut resolver);
            let (current, progress) = Stopped::attach(resolver, advanced)?;
            resolver = current;
            match progress {
                ControlFlow::Continue(enter::Progress::Entered) => continue,
                ControlFlow::Continue(enter::Progress::Idle) => {}
                ControlFlow::Break(output) => return Ok((resolver, output)),
            }
            let result = resolver.backend().ring.try_wait(None).map_err(Error::File);
            (resolver, ()) = Stopped::attach(resolver, result)?;
        }
    }
}

type Retirement<R, O = R> =
    Result<O, Stopped<R, life::Error<<Uring as Source>::Poll, <R as life::Resolver>::Fault>>>;
