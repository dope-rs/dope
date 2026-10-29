use std::{cell::Cell, convert::Infallible, ops::ControlFlow, pin::Pin, rc::Rc};

use dope_core::{
    driver::{retained, route::KeyTag, schedule, schedule::clock},
    io,
    random::Seed,
};
use dope_manifold::{
    app::{
        dispatch::{self, event},
        env::Bundle,
        receive,
    },
    network::listener::{
        self, conn,
        handler::{self, failures, retire, send},
    },
    timing::Throughput,
};
use dope_net::tcp::Tcp;
use dope_runtime::executor;
use dope_test::harness::signals::Gate;
use o3::buffer::{bytes::TryRetain, resident};

use crate::common::manifold::listener::finish::close::control::Control;

#[derive(Default)]
pub struct State {
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

type Listener<'n, 'p, 's, 'd> =
    listener::Listener<'n, 'p, 's, 'd, 'd, KeyTag<0>, App, Bundle<Tcp, Control, Throughput>>;

#[pin_project::pin_project]
pub struct Observed<'n, 'p, 's, 'd: 's> {
    #[pin]
    pub(crate) listener: Listener<'n, 'p, 's, 'd>,
    pub(crate) state: Rc<State>,
}

impl<'n, 'p, 's, 'd> dispatch::raw::Projection<'d> for Observed<'n, 'p, 's, 'd> {
    type Inner = Listener<'n, 'p, 's, 'd>;
    fn inner(self: Pin<&Self>) -> Pin<&Self::Inner> {
        self.project_ref().listener
    }
    fn inner_mut(self: Pin<&mut Self>) -> Pin<&mut Self::Inner> {
        self.project().listener
    }
}

// SAFETY: every lifecycle and dispatch operation retains the installed listener;
// observation only releases the test gate after its receive has been parked.
unsafe impl<'n, 'p, 's, 'd> dispatch::raw::Manifold<'d> for Observed<'n, 'p, 's, 'd> {
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
        // SAFETY: this is the unchanged pinned listener of the installed wrapper.
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

impl<'n, 'p, 's, 'd> event::Turn<'d> for Observed<'n, 'p, 's, 'd> {
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
        let kind = done.into_kind();
        let received = matches!(&kind, io::event::Kind::Recv(_));
        let this = owner.project();
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
