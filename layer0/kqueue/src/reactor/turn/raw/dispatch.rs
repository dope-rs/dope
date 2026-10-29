use dope_sys::{
    Budget as _,
    io::reactor::ready::{Backend as _, Source as _},
};

use crate::reactor::turn::{
    Admission, Completion, Dispatch, Engine, Kqueue, Resume, delivery, engine, ops, ready, task,
};

impl<'a, C, H, Q> engine::raw::Dispatch<'a, C, H, Q> for Dispatch<'a, H, Q>
where
    C: ready::Driver<Context: ready::raw::Decode + 'a> + delivery::Context<H, Backend = Engine<C>>,
    Q: delivery::Quota<H>,
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

        let cursor = C::with(host, |backend| backend.cursor());
        let limit = quota.share::<{ Kqueue::LANES.get() as usize }, 0>(cursor);
        {
            let mut budget = quota.budget::<Resume>(limit);
            C::with(host, |backend| unsafe {
                ready::raw::Poll::resume(backend, &mut budget)
            });
        }

        let mut held = None;
        let mut fault = None;
        {
            let limit = quota.remaining();
            let mut budget = quota.budget::<Completion>(limit);
            while let Admission::Item(item) = budget.admit(|| C::resolve(host)) {
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
        if fault.is_none() && quota.remaining() != 0 {
            let limit = quota.remaining();
            let mut budget = quota.budget::<Resume>(limit);
            C::with(host, |backend| unsafe {
                ready::raw::Poll::resume(backend, &mut budget)
            });
        }
        let more = C::with(host, |backend| {
            backend.has_pending() || backend.can_resume()
        });
        let state = if more || held.is_some() {
            task::Poll::Pending
        } else {
            task::Poll::Ready(())
        };
        match fault {
            Some(error) => (state, error).into(),
            None => (state, held).into(),
        }
    }
}
