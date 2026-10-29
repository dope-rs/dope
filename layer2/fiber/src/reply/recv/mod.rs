//! Fiber adapters for reply attachment and extraction.

pub mod attach;
pub mod outcome;

use std::{mem, pin, task};

use o3::base::cell::region;

use crate::{
    abi, context,
    reply::{
        self, Lane,
        arena::{retire::Retired, state},
        order,
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
}

const _: () = assert!(mem::size_of::<Acceptance>() == mem::size_of::<bool>());

/// Extracts one logical value from a reply slot.
pub trait Extract<'s, 'arena, 'credits, 'region, D, E = D> {
    type Context: Unpin;
    type Output;
    const ACCEPTANCE: Acceptance = Acceptance::Payload;

    fn extract(
        context: &Self::Context,
        slot: &mut reply::Slot<'_, 's, 'arena, 'credits, 'region, D, E>,
    ) -> Option<Self::Output>;
}

struct Registration<'s, 'arena, 'credits, 'region, D, E, K> {
    lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
    key: state::EntryKey,
}

impl<D, E, K> Drop for Registration<'_, '_, '_, '_, D, E, K> {
    fn drop(&mut self) {
        self.lane.defer_retire(Retired::new(self.key));
    }
}

struct Handle<'s, 'arena, 'credits, 'region, D, E, K> {
    reg: Option<Registration<'s, 'arena, 'credits, 'region, D, E, K>>,
}

impl<'s, 'arena, 'credits, 'region, D, E, K> Handle<'s, 'arena, 'credits, 'region, D, E, K> {
    const fn new() -> Self {
        Self { reg: None }
    }

    fn register(
        &self,
        token: &mut region::Token<region::Identity<'region>>,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
        accept: Acceptance,
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
        accept: Acceptance,
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
        accept: Acceptance,
        tag: K,
    ) -> Result<(), attach::Error> {
        let reg = self.register(token, lane, accept, tag)?;
        self.reg = Some(reg);
        Ok(())
    }

    fn poll<O>(
        &self,
        mut context: pin::Pin<&mut context::Context<'_, 'region>>,
        mut extract: impl FnMut(
            Option<&mut reply::Slot<'_, 's, 'arena, 'credits, 'region, D, E>>,
        ) -> task::Poll<O>,
    ) -> task::Poll<O> {
        let wake = context.as_ref().waker();
        let Some((mut slot, signal)) = self.slot(context.as_mut().region_token()) else {
            return extract(None);
        };
        loop {
            if let task::Poll::Ready(output) = extract(Some(&mut slot)) {
                return task::Poll::Ready(output);
            }
            if signal.poll(wake.completion()).is_pending() {
                return task::Poll::Pending;
            }
        }
    }

    fn slot<'a>(
        &self,
        token: &'a mut region::Token<region::Identity<'region>>,
    ) -> Option<(
        reply::Slot<'a, 's, 'arena, 'credits, 'region, D, E>,
        &'a mut state::Signal<'region>,
    )>
    where
        's: 'a,
    {
        let reg = self.reg.as_ref()?;
        reg.lane.arena.slot(token, reg.key)
    }

    fn detach(&mut self) {
        self.reg = None;
    }
}

pub struct Response<
    's,
    'arena,
    'credits,
    'region,
    D,
    X: Extract<'s, 'arena, 'credits, 'region, D, E>,
    E = D,
    K = (),
> {
    handle: Handle<'s, 'arena, 'credits, 'region, D, E, K>,
    extract: X::Context,
}

impl<'s, 'arena, 'credits, 'region, D, X: Extract<'s, 'arena, 'credits, 'region, D, E>, E, K>
    Response<'s, 'arena, 'credits, 'region, D, X, E, K>
{
    /// Consumes typed group input only after every response, slot, and credit is admitted.
    /// Any admission failure leaves every response and lane order unchanged.
    pub fn try_group_by<I, Y, R, const N: usize>(
        token: &mut region::Token<region::Identity<'region>>,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
        commands: &mut [Self; N],
        terminal: &mut Response<'s, 'arena, 'credits, 'region, D, Y, E, K>,
        input: I,
        make: impl FnOnce(I) -> ([K; N], K, R),
    ) -> Result<(order::Depth, R), (attach::Error, I)>
    where
        Y: Extract<'s, 'arena, 'credits, 'region, D, E>,
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
        let (keys, key, output) =
            lane.group_with(token, X::ACCEPTANCE, Y::ACCEPTANCE, input, make)?;
        for (command, key) in commands.iter_mut().zip(keys) {
            command.handle.reg = Some(Registration { lane, key });
        }
        terminal.handle.reg = Some(Registration { lane, key });
        Ok((lane.depth(token), output))
    }

    pub const fn new() -> Self
    where
        X: Extract<'s, 'arena, 'credits, 'region, D, E, Context = ()>,
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
    ) -> Option<X::Output> {
        let output = {
            let (mut slot, _) = self.handle.slot(token)?;
            X::extract(&self.extract, &mut slot)
        };
        if output.is_some() {
            self.handle.detach();
        }
        output
    }

    /// Polls the attached response within the caller's admitted operation.
    /// Extracting an output detaches the response before returning it; later
    /// polls remain pending until an explicit attachment starts another reply.
    pub fn poll_reply(
        self: pin::Pin<&mut Self>,
        context: pin::Pin<&mut context::Context<'_, 'region>>,
    ) -> task::Poll<X::Output> {
        let me = self.get_mut();
        let output = me.handle.poll(context, |slot| {
            slot.and_then(|slot| X::extract(&me.extract, slot))
                .map_or(task::Poll::Pending, task::Poll::Ready)
        });
        if output.is_ready() {
            me.handle.detach();
        }
        output
    }
}

impl<'s, 'arena, 'credits, 'region, D, X: Extract<'s, 'arena, 'credits, 'region, D, E>, E, K>
    Default for Response<'s, 'arena, 'credits, 'region, D, X, E, K>
where
    X: Extract<'s, 'arena, 'credits, 'region, D, E, Context = ()>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<'s, 'arena, 'credits, 'region, D, X: Extract<'s, 'arena, 'credits, 'region, D, E>, E, K>
    attach::Attach<'s, 'arena, 'credits, 'region, D, E, K>
    for Response<'s, 'arena, 'credits, 'region, D, X, E, K>
{
    fn try_ordered(
        &mut self,
        token: &mut region::Token<region::Identity<'region>>,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
        tag: K,
    ) -> Result<order::Depth, attach::Error> {
        self.handle.try_ordered(token, lane, X::ACCEPTANCE, tag)
    }

    fn try_tagged(
        &mut self,
        token: &mut region::Token<region::Identity<'region>>,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
        tag: K,
    ) -> Result<(), attach::Error> {
        self.handle.try_attach(token, lane, X::ACCEPTANCE, tag)
    }

    fn detach(&mut self) {
        self.handle.detach();
    }
}

impl<'s, 'arena, 'credits, 'region, D, X: Extract<'s, 'arena, 'credits, 'region, D, E>, E, K>
    abi::Fiber<'region> for Response<'s, 'arena, 'credits, 'region, D, X, E, K>
{
    type Output = X::Output;

    fn poll(call: context::PollCall<'_, '_, 'region, Self>) -> task::Poll<X::Output> {
        let (this, context) = call.into_parts();
        this.poll_reply(context)
    }
}

pub struct Stream<
    's,
    'arena,
    'credits,
    'region,
    D,
    X: Extract<'s, 'arena, 'credits, 'region, D, E>,
    E = D,
    K = (),
> {
    handle: Handle<'s, 'arena, 'credits, 'region, D, E, K>,
    extract: X::Context,
}

impl<'s, 'arena, 'credits, 'region, D, X: Extract<'s, 'arena, 'credits, 'region, D, E>, E, K>
    Stream<'s, 'arena, 'credits, 'region, D, X, E, K>
{
    pub const fn new() -> Self
    where
        X: Extract<'s, 'arena, 'credits, 'region, D, E, Context = ()>,
    {
        Self::with(())
    }

    pub const fn with(extract: X::Context) -> Self {
        Self {
            handle: Handle::new(),
            extract,
        }
    }

    pub fn poll_next(
        mut self: pin::Pin<&mut Self>,
        context: pin::Pin<&mut context::Context<'_, 'region>>,
    ) -> task::Poll<Option<X::Output>> {
        let me = self.as_mut().get_mut();
        let output = me.handle.poll(context, |slot| match slot {
            Some(slot) => match X::extract(&me.extract, slot) {
                Some(output) => task::Poll::Ready(Some(output)),
                None if slot.state.is_finished() => task::Poll::Ready(None),
                None => task::Poll::Pending,
            },
            None => task::Poll::Ready(None),
        });
        if matches!(output, task::Poll::Ready(None)) {
            me.handle.detach();
        }
        output
    }
}

impl<'s, 'arena, 'credits, 'region, D, X: Extract<'s, 'arena, 'credits, 'region, D, E>, E, K>
    Default for Stream<'s, 'arena, 'credits, 'region, D, X, E, K>
where
    X: Extract<'s, 'arena, 'credits, 'region, D, E, Context = ()>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<'s, 'arena, 'credits, 'region, D, X: Extract<'s, 'arena, 'credits, 'region, D, E>, E, K>
    attach::Attach<'s, 'arena, 'credits, 'region, D, E, K>
    for Stream<'s, 'arena, 'credits, 'region, D, X, E, K>
{
    fn try_ordered(
        &mut self,
        token: &mut region::Token<region::Identity<'region>>,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
        tag: K,
    ) -> Result<order::Depth, attach::Error> {
        self.handle.try_ordered(token, lane, X::ACCEPTANCE, tag)
    }

    fn try_tagged(
        &mut self,
        token: &mut region::Token<region::Identity<'region>>,
        lane: Lane<'s, 'arena, 'credits, 'region, D, E, K>,
        tag: K,
    ) -> Result<(), attach::Error> {
        self.handle.try_attach(token, lane, X::ACCEPTANCE, tag)
    }

    fn detach(&mut self) {
        self.handle.detach();
    }
}
