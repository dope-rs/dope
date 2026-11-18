use std::{pin, task};

use o3::base::cell::region;

use crate::{
    context,
    reply::{self, raw, retain, usage},
};

use super::{Acceptance, Handle, Response, Spec, outcome};

pub struct First;
pub struct Collected;

impl<'s, 'arena, 'credits, 'region, D: usage::Footprint, E>
    Spec<'s, 'arena, 'credits, 'region, D, E> for Collected
{
    type Context = ();
    type Mode = Self;
    const ACCEPTANCE: Acceptance = Acceptance::Collected;
}

impl<'s, 'arena, 'credits, 'region, D: usage::Footprint<Amount = usage::Bytes>, E>
    Spec<'s, 'arena, 'credits, 'region, D, E> for First
{
    type Context = ();
    type Mode = Self;
    const ACCEPTANCE: Acceptance = Acceptance::First;
}

impl<'s, 'arena, 'credits, 'region, D: usage::Footprint, E, K>
    Handle<'s, 'arena, 'credits, 'region, D, E, K>
{
    fn terminal<R>(
        &mut self,
        token: &mut region::Token<region::Identity<'region>>,
        pending: impl FnOnce(&mut reply::arena::state::Signal<'region>),
        complete: impl FnOnce(Result<raw::Terminal<'s, 'arena, 'credits, 'region, D>, E>) -> R,
    ) -> Option<R> {
        let output = {
            let reg = self.reg.as_ref()?;
            let retire = &reg.lane.arena.retire;
            let (mut slot, signal) = reply::Arena::slot(token, reg.live());
            match slot.terminal() {
                None => {
                    pending(signal);
                    return None;
                }
                Some(outcome::Terminal::Complete(done)) => {
                    let completed = raw::Complete::new(done);
                    // SAFETY: the completed slot and retirement source are from
                    // this registration. The callback cannot access this exclusively
                    // borrowed Handle; afterward only its registration is cleared.
                    let terminal = unsafe { completed.bind(retire) };
                    complete(Ok(terminal))
                }
                Some(outcome::Terminal::Failed(fault)) => {
                    let cause = fault.into_error();
                    reg.retire();
                    complete(Err(cause))
                }
            }
        };
        self.reg = None;
        Some(output)
    }

    fn poll_terminal<R>(
        &mut self,
        mut context: pin::Pin<&mut context::Context<'_, 'region>>,
        complete: impl FnOnce(Result<raw::Terminal<'s, 'arena, 'credits, 'region, D>, E>) -> R,
    ) -> task::Poll<R> {
        let wake = context.as_ref().waker();
        self.terminal(
            context.as_mut().region_token(),
            |signal| signal.register(wake.completion()),
            complete,
        )
        .map_or(task::Poll::Pending, task::Poll::Ready)
    }
}

impl<'s, 'arena, 'credits, 'region, D: usage::Footprint, E, K>
    Response<'s, 'arena, 'credits, 'region, D, Collected, E, K>
{
    pub fn try_all(
        &mut self,
        token: &mut region::Token<region::Identity<'region>>,
    ) -> Option<Result<reply::Batch<'s, 'arena, 'credits, 'region, D>, E>> {
        self.handle
            .terminal(token, |_| {}, |result| result.map(raw::Terminal::collect))
    }

    pub fn poll_all<O>(
        self: pin::Pin<&mut Self>,
        context: pin::Pin<&mut context::Context<'_, 'region>>,
        complete: impl FnOnce(Result<reply::Batch<'s, 'arena, 'credits, 'region, D>, E>) -> O,
    ) -> task::Poll<O> {
        self.get_mut()
            .handle
            .poll_terminal(context, |result| complete(result.map(raw::Terminal::collect)))
    }
}

impl<'s, 'arena, 'credits, 'region, D: usage::Footprint<Amount = usage::Bytes>, E, K>
    Response<'s, 'arena, 'credits, 'region, D, First, E, K>
{
    pub fn try_first(
        &mut self,
        token: &mut region::Token<region::Identity<'region>>,
    ) -> Option<Result<Option<retain::First<'s, 'arena, 'credits, 'region, D>>, E>> {
        self.handle
            .terminal(token, |_| {}, |result| result.map(raw::Terminal::first))
    }

    pub fn poll_first<O>(
        self: pin::Pin<&mut Self>,
        context: pin::Pin<&mut context::Context<'_, 'region>>,
        complete: impl FnOnce(Result<Option<retain::First<'s, 'arena, 'credits, 'region, D>>, E>) -> O,
    ) -> task::Poll<O> {
        self.get_mut()
            .handle
            .poll_terminal(context, |result| complete(result.map(raw::Terminal::first)))
    }
}
