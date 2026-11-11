use crate::client::{self, auth, protocol};
use crate::runtime::{brand, endpoint::owner, routing};
use dope::core::driver::{route, schedule};
use dope::manifold::app::dispatch;
use dope::manifold::app::dispatch::event;
use dope::runtime::executor;
use std::pin;

// SAFETY: all retained addresses belong to the structurally pinned endpoint.
unsafe impl<'pool, 'd, const ID: u8, P, B, A, C, R> dispatch::raw::Manifold<'d>
    for client::Dialer<'pool, 'd, ID, P, B, A, C, R>
where
    P: protocol::Protocol<'d>,
    B: client::Backoff,
    A: auth::Mode,
    A::Binding: owner::Binding<'d, ID>,
    C: client::ConfigSource,
    R: routing::PeerRoute,
    Self: dispatch::raw::Projection<'d>,
    <Self as dispatch::raw::Projection<'d>>::Inner: dispatch::raw::Manifold<
            'd,
            Route = route::KeyTag<ID>,
            PrePark = dispatch::raw::Retained,
            Shutdown = dispatch::raw::Plain,
        > + event::Drive<'d, Access = dispatch::raw::Plain>,
{
    type Route = route::KeyTag<ID>;
    type Dispatch = event::Driven<dispatch::raw::Plain>;
    type Install = dispatch::raw::Projected<Self>;
    type Finish = Self;
    type Activate = dispatch::raw::Projected<Self>;
    type Probe = Self;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = dispatch::raw::Plain;

    const COORDS: usize =
        <<Self as dispatch::raw::Projection<'d>>::Inner as dispatch::raw::Manifold<'d>>::COORDS;

    unsafe fn pre_park<'a, 'turn>(
        mut self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self> {
        // SAFETY: inherited from this forwarding method's installed-root contract.
        unsafe {
            dispatch::raw::Manifold::pre_park(
                self.as_mut().project().endpoint,
                turn.reborrow(),
                driver,
            )
        };
        let budget = self.as_ref().get_ref().limits.events.get();
        let now = driver.driver_ref().clock().now();
        self.as_mut().drain_ready(now, budget);
        let mut changed = false;
        let mut left = self.as_ref().get_ref().limits.retries.get();
        while left != 0 {
            let Some(did) = brand::Token::with(|scope| self.as_mut().try_due(scope, now)) else {
                break;
            };
            left -= 1;
            changed |= did;
        }
        if changed {
            // SAFETY: the same installed endpoint remains structurally pinned.
            unsafe {
                dispatch::raw::Manifold::pre_park(self.as_mut().project().endpoint, turn, driver)
            };
        }
        self
    }

    fn shutdown<'a, 'turn>(
        mut self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'd, Self::Shutdown>,
    ) -> pin::Pin<&'a mut Self> {
        self.as_mut().seal();
        {
            let this = self.as_mut().project();
            dispatch::raw::Manifold::shutdown(this.endpoint, turn, driver);
        }
        self.as_mut().drain_events(usize::MAX);
        self
    }
}

impl<'pool, 'd, const ID: u8, P, B, A, C, R> executor::raw::Finishes<'d, Self>
    for client::Dialer<'pool, 'd, ID, P, B, A, C, R>
where
    P: protocol::Protocol<'d>,
    B: client::Backoff,
    A: auth::Mode,
    A::Binding: owner::Binding<'d, ID>,
    C: client::ConfigSource,
    R: routing::PeerRoute,
    Self: dispatch::raw::Projection<'d>,
    <Self as dispatch::raw::Projection<'d>>::Inner: dispatch::raw::Manifold<'d>,
{
    fn call<'app, 'finish, 'owner, 'epoch>(
        finish: executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self>,
    ) -> executor::raw::Finish<'app, 'finish, 'owner, 'epoch, 'd, Self> {
        let finish =
            <dispatch::raw::Projected<Self> as executor::raw::Finishes<'d, Self>>::call(finish);
        let (mut owner, install, proof) = finish.into_parts();
        owner.as_mut().drain_events(usize::MAX);
        executor::raw::Finish::new(owner, install, proof)
    }
}
