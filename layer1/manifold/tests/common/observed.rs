use std::{cell::Cell, convert::Infallible, ops::ControlFlow, pin::Pin, rc::Rc};

use dope_core::{
    driver::{retained, route::KeyTag, schedule, schedule::clock},
    io,
    random::Seed,
};
use dope_manifold::{
    app::{
        dispatch::{self, event},
        receive,
    },
    network::listener::{
        conn,
        handler::{self, failures, retire, send},
    },
};
use dope_runtime::executor;
use dope_test::harness::signals::Gate;
use o3::buffer::{bytes::TryRetain, resident};

use crate::common::manifold::listener::finish::close::control::Control;

#[derive(Default)]
pub struct State {
    pub(crate) hold: Cell<bool>,
    pub(crate) intercepted: Gate,
    pub(crate) parked: Cell<bool>,
    pub(crate) exhausted: Cell<bool>,
    pub(crate) early: Cell<usize>,
    pub(crate) received: Gate,
    pub(crate) bytes: Cell<usize>,
}

pub struct App(pub(crate) Rc<State>);

impl<'d> handler::Application<'d, KeyTag<0>> for App {
    type Build = Infallible;
    type Conn = bool;
    type Wire = Control;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = receive::Gated;
    type Send = send::Silent;

    fn build_failed(self: Pin<&mut Self>, cause: Infallible) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<bool, Infallible> {
        Ok(true)
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn accept(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, KeyTag<0>, Control, bool>,
    ) -> receive::Outcome {
        conn.wake_target().wake();
        receive::Outcome::Continue
    }

    fn is_blocked(
        self: Pin<&Self>,
        conn: conn::Ref<'_, '_, '_, '_, 'd, KeyTag<0>, Control, bool>,
    ) -> bool {
        *conn.state()
    }

    fn activate<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, KeyTag<0>, Control, bool>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, KeyTag<0>, Control, bool>
    where
        'd: 'w,
    {
        if !self.0.parked.get() {
            conn.wake_target().wake();
        } else if *conn.state() {
            *conn.state_mut() = false;
            while let Some(permit) = conn.work().permit() {
                permit.run(|| ());
            }
            self.0.exhausted.set(true);
        }
        conn
    }
}

impl<'d> handler::Borrowed<'d, KeyTag<0>> for App {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, KeyTag<0>, Control, bool>,
        chunk: R,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> receive::Outcome {
        self.0
            .early
            .set(self.0.early.get() + usize::from(!self.0.exhausted.get()));
        self.0.bytes.set(self.0.bytes.get() + chunk.as_ref().len());
        self.0.received.hit();
        receive::Outcome::Continue
    }
}

impl<'d> handler::Gated<'d, KeyTag<0>> for App {
    fn send_drained(
        self: Pin<&mut Self>,
        _: conn::Drained<'_, '_, '_, '_, '_, 'd, KeyTag<0>, Control, bool>,
    ) -> handler::PostDrain {
        handler::PostDrain::Idle
    }
}

#[pin_project::pin_project]
pub struct Observed<'d, M> {
    #[pin]
    pub(crate) listener: M,
    pub(crate) state: Rc<State>,
    pub(crate) pending: Option<io::Event<'d>>,
}

impl<'d, M> dispatch::raw::Projection<'d> for Observed<'d, M> {
    type Inner = M;
    fn inner(self: Pin<&Self>) -> Pin<&Self::Inner> {
        self.project_ref().listener
    }
    fn inner_mut(self: Pin<&mut Self>) -> Pin<&mut Self::Inner> {
        self.project().listener
    }
}

// SAFETY: every lifecycle and dispatch operation retains the installed listener;
// one owned native completion is replayed unchanged before listener maintenance.
unsafe impl<'d, M> dispatch::raw::Manifold<'d> for Observed<'d, M>
where
    M: dispatch::raw::Manifold<
            'd,
            Route = KeyTag<0>,
            PrePark = dispatch::raw::Retained,
            Shutdown = dispatch::raw::Retained,
        > + event::Turn<'d, Access = dispatch::raw::Retained>,
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
        let mut this = self.as_mut().project();
        if !this.state.hold.get()
            && let Some(done) = this.pending.take()
            && let ControlFlow::Break(done) = M::dispatch(executor::raw::Call::new(
                this.listener.as_mut(),
                (),
                (done, turn.reborrow(), driver.reborrow()),
            ))
        {
            *this.pending = Some(done);
        }
        // SAFETY: this is the unchanged pinned listener of the installed wrapper.
        unsafe {
            dispatch::raw::Manifold::pre_park(this.listener, turn, driver);
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
        self.as_ref().project_ref().state.hold.set(false);
        dispatch::raw::Manifold::shutdown(self.as_mut().project().listener, turn, driver);
        self
    }
}

impl<'d, M: event::Turn<'d, Access = dispatch::raw::Retained>> event::Turn<'d> for Observed<'d, M> {
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
        let (owner, (), (done, turn, driver)) = call.into_parts();
        let this = owner.project();
        let kind = match done.into_kind() {
            io::event::Kind::Recv(done) if this.state.hold.get() => match done.classify() {
                io::event::net::recv::Kind::Data(data) => {
                    assert!(this.pending.is_none(), "one held native completion");
                    *this.pending = Some(data.rejoin().into());
                    this.state.intercepted.hit();
                    return ControlFlow::Continue(());
                }
                io::event::net::recv::Kind::Control(done) => io::event::Kind::Recv(done.rejoin()),
            },
            kind => kind,
        };
        let received = matches!(&kind, io::event::Kind::Recv(_));
        let result = event::Turn::dispatch(executor::raw::Call::new(
            this.listener,
            (),
            (kind.into(), turn, driver),
        ));
        if received && matches!(result, ControlFlow::Continue(())) {
            this.state.parked.set(true);
        }
        result
    }
}
