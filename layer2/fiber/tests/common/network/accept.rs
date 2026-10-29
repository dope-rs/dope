use std::{
    cell::Cell,
    marker::PhantomData,
    net::{SocketAddr, TcpListener},
    ops::ControlFlow,
    pin::Pin,
};

use dope::{
    core::{
        driver::{self, schedule},
        io::{
            self,
            event::Kind,
            fd::handles,
            socket::{self, install::Install, option::Set},
        },
    },
    manifold::app::dispatch::{self, event},
    net::{self, tcp::Tcp},
    runtime::executor,
};

pub struct Native;

impl net::Transport for Native {
    type Addr = SocketAddr;
    type Prepared = socket::Inet;
    type StreamConfig = <Tcp as net::Transport>::StreamConfig;

    fn options(config: Self::StreamConfig) -> Set {
        <Tcp as net::Transport>::options(config)
    }
}

impl net::Listen for Native {
    type ListenerConfig = <Tcp as net::Listen>::ListenerConfig;
    type BoundListener = TcpListener;

    fn bind<'d>(
        driver: &mut driver::Context<'_, 'd>,
        addr: &SocketAddr,
        cap: socket::Backlog,
        config: &Self::ListenerConfig,
    ) -> Result<(handles::Descriptor<'d>, SocketAddr), net::BindingError> {
        <Tcp as net::Listen>::bind(driver, addr, cap, config)
    }

    fn adopt<'d>(
        driver: &mut driver::Context<'_, 'd>,
        bound: TcpListener,
    ) -> Result<(handles::Descriptor<'d>, SocketAddr), net::BindingError> {
        Install::adopt_stream(driver.reborrow(), bound.into()).map_err(net::BindingError::Bootstrap)
    }
}

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
pub struct App<'a, 'd, L>
where
    L: dispatch::raw::Manifold<'d> + event::Turn<'d, Access = dispatch::raw::Retained>,
{
    #[pin]
    #[manifold]
    listener: Exhaust<'a, L>,
    #[dispatcher(state)]
    domain: PhantomData<&'d ()>,
}

impl<'a, 'd, L> App<'a, 'd, L>
where
    L: dispatch::raw::Manifold<'d> + event::Turn<'d, Access = dispatch::raw::Retained>,
{
    pub fn new(inner: L, seen: &'a Cell<u8>, failure: &'a Cell<Option<Error>>) -> Self {
        Self {
            listener: Exhaust {
                inner,
                seen,
                failure,
            },
            domain: PhantomData,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Accept,
    Tuning,
}

#[pin_project::pin_project]
pub struct Exhaust<'a, L> {
    #[pin]
    inner: L,
    seen: &'a Cell<u8>,
    failure: &'a Cell<Option<Error>>,
}

impl<'d, L: dispatch::raw::Manifold<'d>> dispatch::raw::Projection<'d> for Exhaust<'_, L> {
    type Inner = L;

    fn inner(self: Pin<&Self>) -> Pin<&L> {
        self.project_ref().inner
    }
    fn inner_mut(self: Pin<&mut Self>) -> Pin<&mut L> {
        self.project().inner
    }
}

// SAFETY: the fixture retains and forwards the same pinned installed listener.
// Dispatch consumes scheduler credits once before forwarding each completion kind.
unsafe impl<'d, L> dispatch::raw::Manifold<'d> for Exhaust<'_, L>
where
    L: dispatch::raw::Manifold<'d> + event::Turn<'d, Access = dispatch::raw::Retained>,
{
    type Route = L::Route;
    type Dispatch = event::Timed<dispatch::raw::Retained>;
    type Install = dispatch::raw::Projected<Self>;
    type Finish = dispatch::raw::Projected<Self>;
    type Activate = dispatch::raw::Projected<Self>;
    type Probe = dispatch::raw::Projected<Self>;
    type PrePark = L::PrePark;
    type Shutdown = L::Shutdown;

    const COORDS: usize = L::COORDS;

    unsafe fn pre_park<'a, 'turn, 'root>(
        mut self: Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> Pin<&'a mut Self>
    where
        Self: 'root,
    {
        unsafe { L::pre_park(self.as_mut().project().inner, turn, driver) };
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
        L::shutdown(self.as_mut().project().inner, turn, driver);
        self
    }
}

impl<'d, L> event::Turn<'d> for Exhaust<'_, L>
where
    L: dispatch::raw::Manifold<'d> + event::Turn<'d, Access = dispatch::raw::Retained>,
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
        let (owner, (), (done, turn, driver)) = call.into_parts();
        let kind = done.into_kind();
        let bit = match &kind {
            Kind::Accept(_) => 1,
            Kind::Tuning(_) => 2,
            _ => 0,
        };
        let this = owner.project();
        let blocked = this.seen.get() & bit == 0 && bit != 0;
        if blocked {
            this.seen.set(this.seen.get() | bit);
            while let Some(permit) = turn.app().permit() {
                permit.run(|| ());
            }
        }
        let result = L::dispatch(executor::raw::Call::new(
            this.inner,
            (),
            (kind.into(), turn, driver),
        ));
        if blocked && matches!(&result, ControlFlow::Continue(())) && this.failure.get().is_none() {
            this.failure.set(Some(if bit == 1 {
                Error::Accept
            } else {
                Error::Tuning
            }));
        }
        result
    }
}
