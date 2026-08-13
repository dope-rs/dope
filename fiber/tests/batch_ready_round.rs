#![deny(unsafe_code)]

use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    task::Poll,
};

use dope::core::driver::schedule::ready::completion;
use dope_fiber::{
    abi::{
        Fiber,
        batch::{Batch, Domain},
    },
    context::PollCall,
    extensions::AppSessionExt,
};
use dope_test::{checks::TrackingAlloc, dispatch, scenario::rt::Runtime};

struct NoopHooks;

impl<'d> dispatch::Hooks<'d, ()> for NoopHooks {}

struct FanIn<'d, const N: usize> {
    index: usize,
    polls: Rc<[Cell<usize>; N]>,
    ready: Rc<Cell<bool>>,
    wakes: Rc<RefCell<Vec<completion::Waker<'d>>>>,
}

impl<'d, const N: usize> Fiber<'d> for FanIn<'d, N> {
    type Output = usize;

    fn poll(call: PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, context) = call.into_parts();
        let this = this.get_mut();
        this.polls[this.index].set(this.polls[this.index].get() + 1);
        if this.ready.get() {
            return Poll::Ready(this.index);
        }

        let all_bound = {
            let mut wakes = this.wakes.borrow_mut();
            wakes.push(context.as_ref().completion_waker());
            wakes.len() == N
        };
        if all_bound {
            this.ready.set(true);
            for wake in this.wakes.borrow_mut().drain(..) {
                wake.wake();
            }
        }
        Poll::Pending
    }
}

#[test]
fn preserves_a_large_simultaneous_ready_round_across_polls() {
    const WIDTH: usize = 512;

    Runtime::throughput().with_session(|mut session| {
        let polls: Rc<[Cell<usize>; WIDTH]> = Rc::new(core::array::from_fn(|_| Cell::new(0)));
        let ready = Rc::new(Cell::new(false));
        let wakes = Rc::new(RefCell::new(Vec::with_capacity(WIDTH)));
        let fibers: [FanIn<'_, WIDTH>; WIDTH] = core::array::from_fn(|index| FanIn {
            index,
            polls: Rc::clone(&polls),
            ready: Rc::clone(&ready),
            wakes: Rc::clone(&wakes),
        });
        let (outputs, (allocations, bytes)) = TrackingAlloc::<0>::measure(|| {
            session.with_app(dispatch::Builder::new(NoopHooks).probe::<0>(), |mut app| {
                app.block_on(dope_gen::fiber!('_, crate = ::dope_fiber => async move {
                    let mut domain = Domain::<WIDTH>::acquire()
                        .await
                        .expect("batch ready domain");
                    Batch::try_from_array(&mut domain, fibers)
                        .expect("batch queue allocation")
                        .await
                }))
            })
        });
        let outputs = outputs
            .expect("application teardown")
            .expect("large ready round completed")
            .collect::<Vec<_>>();

        assert_eq!((allocations, bytes), (0, 0));
        assert_eq!(outputs, (0..WIDTH).collect::<Vec<_>>());
        assert!(polls.iter().all(|polls| polls.get() == 2));
    });
}
