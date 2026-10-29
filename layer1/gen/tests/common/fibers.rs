use std::{marker::PhantomData, pin::Pin, rc::Rc};

use dope_test::fibers::Yield;
use engine::{
    core::driver::{route, schedule},
    manifold::app::dispatch,
};
use fibers::abi::{self, Ready};

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
pub struct App {
    #[pin]
    #[manifold]
    pub dummy: Dummy,
}

pub struct Dummy;

pub struct Ranking;

#[fibers::fiber_fn('d, crate = ::fibers)]
pub(crate) async fn borrowed<'a, 'd>(value: &'a Rc<()>) -> &'a Rc<()> {
    Yield::new().await;
    value
}

pub struct Scoped<'app, 'd: 'app> {
    pub value: &'app str,
    pub driver: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'app, 'd: 'app> Scoped<'app, 'd> {
    #[deny(unused_variables)]
    #[fibers::fiber_fn('d, crate = ::fibers)]
    pub async fn ready<'a>(&'a self) -> &'a str {
        #![allow(unused_variables)]
        let unused = ();
        Ready::new(self.value).await
    }
}

impl Ranking {
    pub fn rank<'d>(
        &self,
        user: Vec<u8>,
    ) -> impl abi::Fiber<'d, Output = Result<Option<usize>, ()>> + use<'d> {
        Ready::new(Ok(Some(user.len())))
    }
}

// SAFETY: Dummy retains no driver-visible owner and participates in the full
// generated shutdown sequence exercised by this test crate.
unsafe impl<'d> dispatch::raw::Manifold<'d> for Dummy {
    const COORDS: usize = 0;

    type Route = route::KeyTag<0>;

    type Dispatch = dispatch::raw::Omit;
    type Install = dispatch::raw::Omit;
    type Finish = dispatch::raw::Omit;
    type Activate = dispatch::raw::Omit;
    type Probe = dispatch::raw::Omit;
    type PrePark = dispatch::raw::Plain;
    type Shutdown = dispatch::raw::Plain;

    unsafe fn pre_park<'a, 'turn, 'root>(
        self: Pin<&'a mut Self>,
        _: schedule::Turn<'turn, 'd>,
        _: &mut dispatch::raw::Context<'_, 'root, 'd, Self::PrePark>,
    ) -> Pin<&'a mut Self>
    where
        Self: 'root,
    {
        self
    }

    fn shutdown<'a, 'turn, 'root>(
        self: Pin<&'a mut Self>,
        _: schedule::Turn<'turn, 'd>,
        _: &mut dispatch::raw::Context<'_, 'root, 'd, Self::Shutdown>,
    ) -> Pin<&'a mut Self>
    where
        Self: 'root,
    {
        self
    }
}
