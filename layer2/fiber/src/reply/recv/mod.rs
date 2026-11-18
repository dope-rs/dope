//! Fiber adapters for reply attachment and extraction.

pub mod attach;
pub mod outcome;
mod terminal;

pub use terminal::{Collected, First};

use std::{convert, mem, pin, task};

use o3::{base::cell::region, depot};

use crate::{
    abi, context,
    reply::{
        self, Lane,
        arena::{retire::Retired, state},
        order,
        usage::Footprint,
    },
};

/// Declares how an extractor consumes data items from its reply slot.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Acceptance {
    /// Items can be extracted while the reply is still arriving.
    Payload,
    /// Data items are not consumed.
    Unit,
    /// The complete reply must fit without waiting for shared credits.
    Collected,
    /// Only the first data item is retained.
    First,
}

const _: () = {
    assert!(mem::size_of::<Acceptance>() == mem::size_of::<bool>());
    assert!(mem::size_of::<Option<Acceptance>>() == mem::size_of::<Acceptance>());
};

/// Selects when an attached response can observe progress.
pub trait Observation {
    const TERMINAL: bool;
}

impl Observation for () {
    const TERMINAL: bool = false;
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub(in crate::reply) enum Accepted {
    Payload,
    Unit,
    Collected,
    First,
    Complete,
    One,
}

impl Accepted {
    const fn new<M: Observation>(acceptance: Acceptance) -> Self {
        match acceptance {
            Acceptance::Payload => Self::Payload,
            Acceptance::Unit => Self::Unit,
            Acceptance::Collected if M::TERMINAL => Self::Complete,
            Acceptance::First if M::TERMINAL => Self::One,
            Acceptance::Collected => Self::Collected,
            Acceptance::First => Self::First,
        }
    }

    pub(in crate::reply) const fn acceptance(self) -> Acceptance {
        match self {
            Self::Payload => Acceptance::Payload,
            Self::Unit => Acceptance::Unit,
            Self::Collected | Self::Complete => Acceptance::Collected,
            Self::First | Self::One => Acceptance::First,
        }
    }

    pub(in crate::reply) const fn observes_data(self) -> bool {
        !matches!(self, Self::Complete | Self::One)
    }
}

const _: () = {
    assert!(mem::size_of::<Accepted>() == mem::size_of::<Acceptance>());
    assert!(mem::size_of::<Option<Accepted>>() == mem::size_of::<Option<Acceptance>>());
};

/// Selects reply admission before any data arrives.
pub trait Spec<'s, 'arena, 'credits, 'region, D: Footprint, E = D> {
    type Context: Unpin;
    type Mode: Observation;
    const ACCEPTANCE: Acceptance = Acceptance::Payload;
}

/// Extracts one logical value from a reply slot.
pub trait Extract<'s, 'arena, 'credits, 'region, D: Footprint, E = D>:
    Spec<'s, 'arena, 'credits, 'region, D, E, Mode = ()>
{
    type Output;

    fn extract(
        context: &Self::Context,
        slot: &mut reply::Slot<'_, 's, 'arena, 'credits, 'region, D, E>,
    ) -> Option<Self::Output>;
}

struct Registration<'s, 'arena, 'credits, 'region, D: Footprint, E, K> {
    lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
    key: state::EntryKey,
}

impl<'s, 'arena, 'credits, 'region, D: Footprint, E, K>
    Registration<'s, 'arena, 'credits, 'region, D, E, K>
{
    fn live(
        &self,
    ) -> state::Registered<'s, reply::Arena<'arena, 'credits, 'region, D, E, K>, &Self> {
        // SAFETY: this borrow precedes the owner's detach/drop retirement.
        unsafe { depot::raw::Proof::new((self.lane.arena, self.key)) }
    }

    fn retire(&self) {
        self.lane.defer_retire(Retired::new(self.key.index()));
    }
}

struct Handle<'s, 'arena, 'credits, 'region, D: Footprint, E, K> {
    reg: Option<Registration<'s, 'arena, 'credits, 'region, D, E, K>>,
}

impl<D: Footprint, E, K> Drop for Handle<'_, '_, '_, '_, D, E, K> {
    fn drop(&mut self) {
        self.retire();
    }
}

impl<'s, 'arena, 'credits, 'region, D: Footprint, E, K>
    Handle<'s, 'arena, 'credits, 'region, D, E, K>
{
    const fn new() -> Self {
        Self { reg: None }
    }

    fn register(
        &self,
        token: &mut region::Token<region::Identity<'region>>,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
        accept: Accepted,
        tag: K,
    ) -> Result<Registration<'s, 'arena, 'credits, 'region, D, E, K>, attach::Error> {
        self.check(lane)?;
        let key = lane.register(token, accept, tag)?;
        Ok(Registration { lane, key })
    }

    fn check(
        &self,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
    ) -> Result<(), attach::Error> {
        if let Some(current) = self.reg.as_ref() {
            return Err(attach::Error::Attached {
                current: current.lane.index,
                requested: lane.index,
            });
        }
        Ok(())
    }

    fn try_ordered(
        &mut self,
        token: &mut region::Token<region::Identity<'region>>,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
        accept: Accepted,
        tag: K,
    ) -> Result<order::Depth, attach::Error> {
        let reg = self.register(token, lane, accept, tag)?;
        let depth = reg.lane.depth(token);
        self.reg = Some(reg);
        Ok(depth)
    }

    fn try_attach(
        &mut self,
        token: &mut region::Token<region::Identity<'region>>,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
        accept: Accepted,
        tag: K,
    ) -> Result<(), attach::Error> {
        let reg = self.register(token, lane, accept, tag)?;
        self.reg = Some(reg);
        Ok(())
    }

    fn poll<R, O>(
        &mut self,
        mut context: pin::Pin<&mut context::Context<'_, 'region>>,
        extract: impl FnOnce(
            Option<&mut reply::Slot<'_, 's, 'arena, 'credits, 'region, D, E>>,
        ) -> task::Poll<R>,
        complete: impl FnOnce(&mut Self, R) -> O,
    ) -> task::Poll<O> {
        let output = {
            let wake = context.as_ref().waker();
            match self.slot(context.as_mut().region_token()) {
                Some((mut slot, signal)) => {
                    let output = extract(Some(&mut slot));
                    if output.is_pending() {
                        signal.register(wake.completion());
                    }
                    output
                }
                None => extract(None),
            }
        };
        output.map(|output| complete(self, output))
    }

    fn slot<'a>(
        &'a self,
        token: &'a mut region::Token<region::Identity<'region>>,
    ) -> Option<(
        reply::Slot<'a, 's, 'arena, 'credits, 'region, D, E>,
        &'a mut state::Signal<'region>,
    )>
    where
        's: 'a,
    {
        let reg = self.reg.as_ref()?;
        Some(reply::Arena::slot(token, reg.live()))
    }

    fn retire(&self) {
        if let Some(reg) = &self.reg {
            reg.retire();
        }
    }

    fn detach(&mut self) {
        self.retire();
        self.reg = None;
    }

    fn detach_order(&mut self, token: &region::Token<region::Identity<'region>>) -> order::State {
        let Some(reg) = self.reg.as_ref() else {
            return order::State::Detached;
        };
        let order = reply::Arena::registered_order(token, reg.live());
        self.detach();
        order
    }
}

pub struct Response<
    's,
    'arena,
    'credits,
    'region,
    D: Footprint,
    X: Spec<'s, 'arena, 'credits, 'region, D, E>,
    E = D,
    K = (),
> {
    handle: Handle<'s, 'arena, 'credits, 'region, D, E, K>,
    extract: X::Context,
}

impl<
    's,
    'arena,
    'credits,
    'region,
    D: Footprint,
    X: Spec<'s, 'arena, 'credits, 'region, D, E>,
    E,
    K,
> Response<'s, 'arena, 'credits, 'region, D, X, E, K>
{
    /// Constructs the response only after admission; failure returns the input unchanged.
    pub fn try_with<I, R>(
        token: &mut region::Token<region::Identity<'region>>,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
        tag: K,
        input: I,
        make: impl FnOnce(I) -> (X::Context, R),
    ) -> Result<(order::Depth, Self, R), (attach::Error, I)> {
        let key = match lane.register(token, Accepted::new::<X::Mode>(X::ACCEPTANCE), tag) {
            Ok(key) => key,
            Err(cause) => return Err((cause, input)),
        };
        let (extract, output) = make(input);
        let response = Self {
            handle: Handle {
                reg: Some(Registration { lane, key }),
            },
            extract,
        };
        Ok((lane.depth(token), response, output))
    }

    /// Consumes typed group input only after every response, slot, and credit is admitted.
    /// Any admission failure leaves every response and lane order unchanged.
    pub fn try_group_by<I, Y, V, F, R, const N: usize>(
        token: &mut region::Token<region::Identity<'region>>,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
        commands: &mut [Self; N],
        terminal: &mut Response<'s, 'arena, 'credits, 'region, D, Y, E, K>,
        input: I,
        make: impl FnOnce(I) -> ([V; N], F, K, R),
    ) -> Result<(order::Depth, R), (attach::Error, I)>
    where
        Y: Spec<'s, 'arena, 'credits, 'region, D, E>,
        F: FnMut(usize, V) -> K,
    {
        const { assert!(N > 0 && N < usize::MAX) };
        for command in commands.iter() {
            if let Err(cause) = command.handle.check(lane) {
                return Err((cause, input));
            }
        }
        if let Err(cause) = terminal.handle.check(lane) {
            return Err((cause, input));
        }
        let (key, output) = lane.group_with(
            token,
            (
                Accepted::new::<X::Mode>(X::ACCEPTANCE),
                Accepted::new::<Y::Mode>(Y::ACCEPTANCE),
            ),
            commands,
            input,
            make,
            |command, key| command.handle.reg = Some(Registration { lane, key }),
        )?;
        terminal.handle.reg = Some(Registration { lane, key });
        Ok((lane.depth(token), output))
    }

    pub const fn new() -> Self
    where
        X: Spec<'s, 'arena, 'credits, 'region, D, E, Context = ()>,
    {
        Self::with(())
    }

    pub const fn with(extract: X::Context) -> Self {
        Self {
            handle: Handle::new(),
            extract,
        }
    }

    /// Extracts an already available result once without installing a wake target.
    /// A completed output detaches the same response ownership as `poll_reply`.
    pub fn try_take(
        &mut self,
        token: &mut region::Token<region::Identity<'region>>,
    ) -> Option<X::Output>
    where
        X: Extract<'s, 'arena, 'credits, 'region, D, E>,
    {
        let output = {
            let (mut slot, _) = self.handle.slot(token)?;
            X::extract(&self.extract, &mut slot)
        };
        if output.is_some() {
            self.handle.detach();
        }
        output
    }

    /// Runs completion after the slot loan ends and the response detaches.
    /// Pending leaves completion uncalled and the response attached.
    pub fn poll_reply<O>(
        self: pin::Pin<&mut Self>,
        context: pin::Pin<&mut context::Context<'_, 'region>>,
        extract: impl FnOnce(
            &X::Context,
            &mut reply::Slot<'_, 's, 'arena, 'credits, 'region, D, E>,
        ) -> Option<X::Output>,
        complete: impl FnOnce(X::Output) -> O,
    ) -> task::Poll<O>
    where
        X: Extract<'s, 'arena, 'credits, 'region, D, E>,
    {
        let me = self.get_mut();
        me.handle.poll(
            context,
            |slot| {
                slot.and_then(|slot| extract(&me.extract, slot))
                    .map_or(task::Poll::Pending, task::Poll::Ready)
            },
            |handle, output| {
                handle.detach();
                complete(output)
            },
        )
    }

    pub fn poll_next(
        mut self: pin::Pin<&mut Self>,
        context: pin::Pin<&mut context::Context<'_, 'region>>,
    ) -> task::Poll<Option<X::Output>>
    where
        X: Extract<'s, 'arena, 'credits, 'region, D, E>,
    {
        let me = self.as_mut().get_mut();
        me.handle.poll(
            context,
            |slot| match slot {
                Some(slot) => match X::extract(&me.extract, slot) {
                    Some(output) => task::Poll::Ready(Some(output)),
                    None if slot.state.is_finished() => task::Poll::Ready(None),
                    None => task::Poll::Pending,
                },
                None => task::Poll::Ready(None),
            },
            |handle, output| {
                if output.is_none() {
                    handle.detach();
                }
                output
            },
        )
    }
}

impl<'s, 'arena, 'credits, 'region, D: Footprint, X, E, K> Default
    for Response<'s, 'arena, 'credits, 'region, D, X, E, K>
where
    X: Spec<'s, 'arena, 'credits, 'region, D, E, Context = ()>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<
    's,
    'arena,
    'credits,
    'region,
    D: Footprint,
    X: Spec<'s, 'arena, 'credits, 'region, D, E>,
    E,
    K,
> attach::Attach<'s, 'arena, 'credits, 'region, D, E, K>
    for Response<'s, 'arena, 'credits, 'region, D, X, E, K>
{
    fn try_ordered(
        &mut self,
        token: &mut region::Token<region::Identity<'region>>,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
        tag: K,
    ) -> Result<order::Depth, attach::Error> {
        self.handle
            .try_ordered(token, lane, Accepted::new::<X::Mode>(X::ACCEPTANCE), tag)
    }

    fn try_tagged(
        &mut self,
        token: &mut region::Token<region::Identity<'region>>,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
        tag: K,
    ) -> Result<(), attach::Error> {
        self.handle
            .try_attach(token, lane, Accepted::new::<X::Mode>(X::ACCEPTANCE), tag)
    }

    fn detach(&mut self) {
        self.handle.detach();
    }

    fn detach_order(&mut self, token: &region::Token<region::Identity<'region>>) -> order::State {
        self.handle.detach_order(token)
    }
}

impl<
    's,
    'arena,
    'credits,
    'region,
    D: Footprint,
    X: Extract<'s, 'arena, 'credits, 'region, D, E>,
    E,
    K,
> abi::Fiber<'region> for Response<'s, 'arena, 'credits, 'region, D, X, E, K>
{
    type Output = X::Output;

    fn poll(call: context::PollCall<'_, '_, 'region, Self>) -> task::Poll<X::Output> {
        let (this, context) = call.into_parts();
        this.poll_reply(context, X::extract, convert::identity)
    }
}

pub type Stream<'s, 'arena, 'credits, 'region, D, X, E = D, K = ()> =
    Response<'s, 'arena, 'credits, 'region, D, X, E, K>;
