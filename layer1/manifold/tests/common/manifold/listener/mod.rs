use std::{cell::Cell, ops::ControlFlow, pin::Pin, rc::Rc};

use dope_core::{
    driver::{route::KeyTag, schedule},
    io,
    io::{
        event::{Kind, batch::Flow, net::accept},
        fd::handles::AcceptedSlot,
    },
};
use dope_manifold::{
    app::{dispatch, dispatch::event, env},
    network::listener::{self, handler},
};
use dope_runtime::executor;
use dope_test::harness::{
    observe::VecCellExt as _,
    signals::{Gate, Progress as _},
};
use o3::depot::capacity;

pub mod coupled;
pub mod defer;
pub mod egress;
pub mod pipeline;
pub mod probes;
pub mod terminal;
pub mod trace;

#[derive(Default)]
pub struct Trace {
    pub(crate) accepts: Cell<usize>,
    pub(crate) tunings: Cell<usize>,
    pub(crate) active_after_tuning: Cell<Option<capacity::Count>>,
    pub(crate) send_shutdowns: Cell<Vec<(capacity::Count, usize)>>,
}

impl Trace {
    pub fn accepts(&self) -> usize {
        self.accepts.get()
    }

    pub fn tunings(&self) -> usize {
        self.tunings.get()
    }

    pub fn tuned(&self) -> Option<capacity::Count> {
        self.active_after_tuning.get()
    }
}

pub enum CancelAt<'d> {
    Tuning,
    Send,
    Retain(Cell<Option<AcceptedSlot<'d>>>),
}

#[pin_project::pin_project]
pub struct Cancel<'n, 'p, 's, 'd, A, E>
where
    A: handler::Application<'d, KeyTag<0>>,
    E: env::Spec<Wire = A::Wire>,
{
    #[pin]
    pub(crate) listener: listener::Listener<'n, 'p, 's, 'd, 'd, KeyTag<0>, A, E>,
    pub(crate) at: CancelAt<'d>,
    pub(crate) trace: Rc<Trace>,
    pub(crate) gate: Gate,
}

impl<'n, 'p, 's, 'd, A, E> dispatch::raw::Projection<'d> for Cancel<'n, 'p, 's, 'd, A, E>
where
    A: handler::Application<'d, KeyTag<0>> + handler::Runtime<'d, 'd, KeyTag<0>>,
    E: env::Spec<Wire = A::Wire>,
{
    type Inner = listener::Listener<'n, 'p, 's, 'd, 'd, KeyTag<0>, A, E>;

    fn inner(self: Pin<&Self>) -> Pin<&Self::Inner> {
        self.project_ref().listener
    }

    fn inner_mut(self: Pin<&mut Self>) -> Pin<&mut Self::Inner> {
        self.project().listener
    }
}

// SAFETY: callbacks retain the installed pinned listener. Retain consumes only
// one nonterminal accepted prefix and owns its descriptor until release; every
// terminal completion is forwarded unchanged. Shutdown precedes its dispatch.
unsafe impl<'n, 'p, 's, 'd, A, E> dispatch::raw::Manifold<'d> for Cancel<'n, 'p, 's, 'd, A, E>
where
    A: handler::Application<'d, KeyTag<0>> + handler::Runtime<'d, 'd, KeyTag<0>>,
    E: env::Spec<Wire = A::Wire>,
{
    type Route = KeyTag<0>;
    type Dispatch = event::Timed<dispatch::raw::Retained>;
    type Install = dispatch::raw::Projected<Self>;
    type Finish = dispatch::raw::Projected<Self>;
    type Activate = dispatch::raw::Projected<Self>;
    type Probe = dispatch::raw::Projected<Self>;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = dispatch::raw::Retained;

    const COORDS: usize =
        <<Self as dispatch::raw::Projection<'d>>::Inner as dispatch::raw::Manifold<'d>>::COORDS;

    unsafe fn pre_park<'a, 'turn, 'root>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> Pin<&'a mut Self>
    where
        Self: 'root,
    {
        unsafe {
            dispatch::raw::Manifold::pre_park(self.as_mut().project().listener, turn, driver);
        }
        self
    }

    fn shutdown<'a, 'turn, 'root>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::Shutdown>,
    ) -> Pin<&'a mut Self>
    where
        Self: 'root,
    {
        dispatch::raw::Manifold::shutdown(self.as_mut().project().listener, turn, driver);
        self
    }
}

impl<'n, 'p, 's, 'd, A, E> event::Turn<'d> for Cancel<'n, 'p, 's, 'd, A, E>
where
    A: handler::Application<'d, KeyTag<0>> + handler::Runtime<'d, 'd, KeyTag<0>>,
    E: env::Spec<Wire = A::Wire>,
{
    type Access = dispatch::raw::Retained;

    fn dispatch<'app, 'driver, 'turn, 'root>(
        call: executor::raw::Call<
            'app,
            Self,
            (),
            (
                io::Event<'d>,
                schedule::Turn<'turn, 'd>,
                dispatch::raw::Context<'driver, 'root, 'd, Self::Access>,
            ),
        >,
    ) -> ControlFlow<io::Event<'d>>
    where
        'd: 'root,
    {
        let (owner, (), (done, turn, mut driver)) = call.into_parts();
        let mut this = owner.project();
        let kind = match (done.into_kind(), &mut *this.at) {
            (Kind::Accept(done), CancelAt::Retain(slot)) if this.gate.hits() == 0 => {
                let (_, flow, outcome) = done.into_parts();
                assert_eq!(flow, Flow::More, "retain only a multishot prefix");
                let accepted = match outcome {
                    accept::Outcome::Accepted(accepted) => Ok(accepted),
                    accept::Outcome::Failed(cause) => Err(cause),
                }
                .expect("first native accept");
                slot.set(Some(accepted));
                this.gate.hit();
                return ControlFlow::Continue(());
            }
            (kind, _) => kind,
        };
        let accept = matches!(&kind, Kind::Accept(_));
        let tuning = matches!(&kind, Kind::Tuning(_));
        let cancel = match this.at {
            CancelAt::Tuning => tuning,
            CancelAt::Send => matches!(&kind, Kind::Send(_)),
            CancelAt::Retain(_) => false,
        };
        let done = kind.into();
        let before = this.listener.as_ref().get_ref().len();
        if cancel {
            if matches!(this.at, CancelAt::Send) {
                this.trace.send_shutdowns.push((
                    this.listener.as_ref().get_ref().capacity(),
                    turn.reborrow().maint().remaining(),
                ));
            }
            dispatch::raw::Manifold::shutdown(this.listener.as_mut(), turn.reborrow(), &mut driver);
        }
        let result = event::Turn::dispatch(executor::raw::Call::new(
            this.listener.as_mut(),
            (),
            (done, turn.reborrow(), driver.narrow()),
        ));
        if accept
            && matches!(result, ControlFlow::Continue(()))
            && this.listener.as_ref().get_ref().len() > before
        {
            this.trace.accepts.set(this.trace.accepts.get() + 1);
        }
        if tuning {
            this.trace.tunings.set(this.trace.tunings.get() + 1);
            this.trace
                .active_after_tuning
                .set(Some(this.listener.as_ref().get_ref().len()));
        }
        if cancel {
            this.gate.hit();
        }
        result
    }
}

pub mod finish;
