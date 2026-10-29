use std::{ops, task};

use dope_sys::{
    Budget,
    engine::{self, ring},
    io::reactor::ready::delivery,
    ring::ready::Complete as _,
};
use o3::mem::quota::Admission;

use crate::driver::{Engine, Turn, tuning::Progress};

enum Completion {}

impl<'a, C, H, Q: delivery::Quota<H>> engine::raw::Dispatch<'a, C, H, Q> for Turn<'a, H, Q>
where
    C: ring::Context + delivery::Context<H, Backend = Engine<C>>,
    C::Tuning: Progress<C::Token>,
{
    unsafe fn new(host: &'a mut H, quota: Q) -> Self {
        Self { host, quota }
    }

    fn dispatch(
        self,
        mut emit: impl FnMut(C::Event, &mut H) -> ops::ControlFlow<C::Event>,
    ) -> C::Dispatch {
        let Self { host, quota } = self;
        quota.reclaim(host);
        C::with(host, |backend| backend.ring.acquire());
        let mut held = None;
        let mut fault = None;
        {
            let limit = quota.remaining();
            let mut budget = quota.budget::<Completion>(limit);
            loop {
                let item = match budget.admit(|| C::resolve(host)) {
                    Admission::Item(item) => item,
                    Admission::Empty | Admission::Exhausted => break,
                };
                let event = match item {
                    Ok(Some(event)) => event,
                    Ok(None) => continue,
                    Err(error) => {
                        fault = Some(error);
                        break;
                    }
                };
                match emit(event, host) {
                    ops::ControlFlow::Continue(()) => {}
                    ops::ControlFlow::Break(event) => {
                        held = Some(event);
                        break;
                    }
                }
            }
        }
        let pending = C::with(host, |backend| {
            let complete = backend.ring.refresh();
            backend.ring.flush();
            complete.get() != 0 || backend.tuning.has_pending()
        });
        if let Some(error) = fault {
            return (task::Poll::Pending, error).into();
        }
        let state = if pending || held.is_some() {
            task::Poll::Pending
        } else {
            task::Poll::Ready(())
        };
        (state, held).into()
    }
}
