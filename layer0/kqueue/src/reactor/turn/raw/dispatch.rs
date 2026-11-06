use dope_sys::{
    Budget as _,
    io::reactor::ready::{Backend as _, Source as _},
};

use crate::reactor::turn::{
    Admission, Completion, Dispatch, Engine, Kqueue, Resume, Work, delivery, engine, ops, ready,
    task, time,
};

impl<'a, C, H, Q> engine::raw::Dispatch<'a, C, H, Q> for Dispatch<'a, H, Q>
where
    C: ready::Driver<Context: ready::raw::Decode + 'a> + delivery::Context<H, Backend = Engine<C>>,
    C::Dispatch: From<<Engine<C> as ready::Backend<C>>::Failure>,
    Q: delivery::Quota<H>,
{
    unsafe fn new(host: &'a mut H, quota: Q) -> Self {
        Self { host, quota }
    }

    fn dispatch(
        self,
        mut emit: impl FnMut(C::Event, &mut H) -> ops::ControlFlow<C::Event>,
    ) -> C::Dispatch {
        let result = (|| -> Result<C::Dispatch, <Engine<C> as ready::Backend<C>>::Failure> {
            let Self { host, quota } = self;
            quota.reclaim(host);
            C::backend(host).check()?;
            let work = Work { quota };
            let quota = &work.quota;
            let cursor = if quota.remaining() == 0 {
                0
            } else {
                C::backend(host).cursor()
            };
            let mut resume = true;
            let mut kernel = true;
            let mut held = None;
            let mut fault = None;
            'turn: loop {
                let mut before = quota.remaining();
                if before == 0 {
                    break;
                }
                let mut step = 0;
                while step < Kqueue::LANES.get() as usize && quota.remaining() != 0 {
                    match (cursor + step) % Kqueue::LANES.get() as usize {
                        0 => {
                            if resume {
                                let mut budget = quota.budget::<Resume>(quota.remaining());
                                resume = unsafe {
                                    ready::raw::Poll::resume(C::backend(host), &mut budget)
                                }?
                                .is_continue();
                                if !resume {
                                    // The blocked item consumed one credit without progress.
                                    before -= 1;
                                }
                            }
                        }
                        1 => {
                            let remaining = quota.remaining();
                            let done = if step + 1 < Kqueue::LANES.get() as usize {
                                step += 1;
                                remaining
                            } else {
                                0
                            };
                            if kernel || C::backend(host).has_changes() {
                                kernel = unsafe {
                                    work.poll(
                                        C::backend(host),
                                        Some(time::Duration::ZERO),
                                        remaining,
                                        done,
                                    )
                                }?;
                            }
                        }
                        2 => {
                            if kernel {
                                kernel = unsafe {
                                    work.poll(
                                        C::backend(host),
                                        Some(time::Duration::ZERO),
                                        0,
                                        quota.remaining(),
                                    )
                                }?;
                            }
                        }
                        3 => {
                            let mut budget = quota.budget::<Completion>(quota.remaining());
                            while let Admission::Item(item) = budget.admit(|| C::resolve(host)) {
                                resume = true;
                                let event = match item {
                                    Ok(Some(event)) => event,
                                    Ok(None) => continue,
                                    Err(error) => {
                                        fault = Some(error);
                                        break 'turn;
                                    }
                                };
                                if let ops::ControlFlow::Break(event) = emit(event, host) {
                                    held = Some(event);
                                    break 'turn;
                                }
                            }
                        }
                        _ => unreachable!("lane"),
                    }
                    step += 1;
                }
                if quota.remaining() == before {
                    break;
                }
            }
            let more = {
                let backend = C::backend(host);
                backend.has_pending() || backend.can_resume() || backend.has_changes()
            };
            let state = if more || held.is_some() {
                task::Poll::Pending
            } else {
                task::Poll::Ready(())
            };
            Ok(match fault {
                Some(error) => (state, error).into(),
                None => (state, held).into(),
            })
        })();
        match result {
            Ok(dispatch) => dispatch,
            Err(error) => error.into(),
        }
    }
}
