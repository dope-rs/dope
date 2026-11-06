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
        C::backend(host).ring.acquire();
        let result = {
            let limit = quota.remaining();
            let mut budget = quota.budget::<Completion>(limit);
            loop {
                let item = match budget.admit(|| C::resolve(host)) {
                    Admission::Item(item) => item,
                    Admission::Empty | Admission::Exhausted => break Ok(None),
                };
                let event = match item {
                    Ok(Some(event)) => event,
                    Ok(None) => continue,
                    Err(error) => break Err(error),
                };
                if let ops::ControlFlow::Break(event) = emit(event, host) {
                    break Ok(Some(event));
                }
            }
        };
        let pending = {
            let backend = C::backend(host);
            let complete = backend.ring.refresh();
            backend.ring.flush();
            complete.get() != 0 || backend.tuning.has_pending()
        };
        let state = if pending {
            task::Poll::Pending
        } else {
            task::Poll::Ready(())
        };
        match result {
            Err(error) => (task::Poll::Pending, error).into(),
            Ok(Some(event)) => (task::Poll::Pending, Some(event)).into(),
            Ok(None) => (state, None::<C::Event>).into(),
        }
    }
}
