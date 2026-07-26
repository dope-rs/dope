use std::cell::Cell;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::Poll;
use std::time::{Duration, Instant};

use dope::driver::ready::ReadySlot;
use dope::runtime::dispatcher::Dispatcher;
use dope::runtime::executor::Session;
use dope::{Completion, Event};
use dope_fiber::abi::Fiber;
use dope_fiber::extensions::SessionExt;
use dope_fiber::raw::task::Context;
use o3::cell::BrandCell;

use crate::GUARD;
use crate::rt::{tok, with_session};

pub fn with_context<R>(run: impl for<'poll, 'd> FnOnce(Pin<&mut Context<'poll, 'd>>) -> R) -> R {
    with_session(|mut session| {
        let slot = session
            .driver()
            .make_ready_slot(tok(0))
            .expect("ready slot");
        let reference = session.driver();
        let access = session.driver_access();
        let mut context = pin!(Context::from_ready(reference, slot.key(), access));
        run(context.as_mut())
    })
}

pub fn poll_with_slot<'scope, 'd, S, F>(
    session: &mut Session<'scope, 'd, S>,
    slot: &ReadySlot<'d>,
    fiber: Pin<&mut F>,
) -> Poll<F::Output>
where
    F: Fiber<'d>,
{
    let reference = session.driver();
    let access = session.driver_access();
    let mut context = pin!(Context::from_ready(reference, slot.key(), access));
    Fiber::poll(fiber, context.as_mut())
}

pub fn poll_ready<'d, F: Fiber<'d> + ?Sized>(
    fiber: Pin<&mut F>,
    cx: Pin<&mut Context<'_, 'd>>,
) -> F::Output {
    match Fiber::poll(fiber, cx) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("expected Ready"),
    }
}

pub fn drive<'scope, 'd, S, D: Dispatcher<'d>, F: Fiber<'d>>(
    sess: &mut Session<'scope, 'd, S>,
    app: Pin<&BrandCell<'d, D>>,
    fiber: F,
) -> F::Output {
    sess.block_on(app, fiber).expect("runtime park")
}

pub struct Gate {
    hits: Cell<u32>,
}

impl Default for Gate {
    fn default() -> Self {
        Self { hits: Cell::new(0) }
    }
}

impl Gate {
    pub fn new() -> Rc<Self> {
        Rc::new(Self::default())
    }

    pub fn hit(&self) {
        self.hits.set(self.hits.get() + 1);
    }

    pub fn hits(&self) -> u32 {
        self.hits.get()
    }
}

struct Until {
    gate: Rc<Gate>,
    want: u32,
    start: Instant,
}

impl<'d> Fiber<'d> for Until {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<bool> {
        let this = self.get_mut();
        if this.gate.hits() >= this.want {
            return Poll::Ready(true);
        }
        if this.start.elapsed() >= GUARD {
            return Poll::Ready(false);
        }
        cx.wake();
        Poll::Pending
    }
}

pub fn run_until<'scope, 'd, D: Dispatcher<'d>>(
    sess: &mut Session<'scope, 'd>,
    app: Pin<&BrandCell<'d, D>>,
    gate: &Rc<Gate>,
    want: u32,
) {
    let until = Until {
        gate: gate.clone(),
        want,
        start: Instant::now(),
    };
    let reached = sess.block_on(app, until).expect("runtime park");
    assert!(
        reached,
        "timed out after {GUARD:?}: gate took {} of {want} hits",
        gate.hits()
    );
}

pub fn pump_events<'scope, 'd, S>(
    sess: &mut Session<'scope, 'd, S>,
    mut handle: impl FnMut(Event<'d>),
    mut done: impl FnMut() -> bool,
) -> bool {
    let mut buf = [const { None }; 32];
    for _ in 0..500 {
        if done() {
            return true;
        }
        let mut driver = sess.driver_access();
        let _ = driver.wait(Some(Duration::from_millis(5)));
        let n = driver.drain(&mut buf);
        for event in &mut buf[..n] {
            handle(event.take().expect("completion slot"));
        }
    }
    done()
}
