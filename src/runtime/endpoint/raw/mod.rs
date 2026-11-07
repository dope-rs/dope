mod drive;

use crate::{runtime::Transit, wire::packet::long::retry};
use o3::buffer;
use o3::buffer::Residency;
use reactor::buffers::Entry;
use std::pin;

use o3::buffer::resident;

use core::driver;
use dope::core;
use dope::manifold::app::dispatch;
use dope::manifold::app::dispatch::event;
use dope::manifold::network::datagram;
use driver::route;
use driver::schedule;

use crate::conn::id;
use crate::runtime::endpoint;
use crate::runtime::endpoint::control;
use crate::runtime::endpoint::owner;
use crate::runtime::endpoint::reactor;
use crate::runtime::mux::{handler, protocol};
use crate::runtime::receive::Storage;

/// # Safety
/// `View` cannot move or release handler storage, nor extend `Loan` to `'d`.
pub unsafe trait Control<'d, const ID: u8, B: crate::stream::Recv = Vec<u8>>:
    handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>
{
    type View<'step>
    where
        Self: 'step,
        'd: 'step;

    fn view<'step>(loan: dispatch::raw::Loan<'step, 'd, Self>) -> Self::View<'step>
    where
        'd: 'step;
}

unsafe impl<'d, const ID: u8, H, B, A, M, Q: Residency + buffer::raw::Source>
    dispatch::raw::Manifold<'d> for endpoint::Endpoint<'d, ID, H, B, A, M, Q>
where
    H: handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, resident::Shared<'d>>,
    M: owner::Binding<'d, ID>,
    datagram::Endpoint<
        'd,
        route::KeyTag<ID>,
        reactor::Engine<'d, H, ID, B, A, M::Mode, Q>,
        endpoint::Payload<Q>,
    >: dispatch::raw::Manifold<
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
    type Probe = dispatch::raw::Projected<Self>;
    type PrePark = dispatch::raw::Retained;
    type Shutdown = dispatch::raw::Plain;

    const COORDS: usize =
        <<Self as dispatch::raw::Projection<'d>>::Inner as dispatch::raw::Manifold<'d>>::COORDS;

    unsafe fn pre_park<'a, 'turn>(
        mut self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'd, Self::PrePark>,
    ) -> pin::Pin<&'a mut Self> {
        unsafe {
            dispatch::raw::Manifold::pre_park(self.as_mut().project().udp, turn.reborrow(), driver)
        };
        let time = driver.driver_ref().clock().now();
        let count = time.at(self.as_mut()).apply(|state, now| {
            let this = state.project();
            let host = this.udp.handler_mut().project();
            host.rotation
                .maintain_at(now, &mut protocol::Io::new(host.mux))
                .started()
        });
        if count != 0 {
            unsafe { dispatch::raw::Manifold::pre_park(self.as_mut().project().udp, turn, driver) };
        }
        self
    }

    fn shutdown<'a, 'turn>(
        mut self: pin::Pin<&'a mut Self>,
        turn: schedule::Turn<'turn, 'd>,
        driver: &mut dispatch::raw::Context<'_, 'd, Self::Shutdown>,
    ) -> pin::Pin<&'a mut Self> {
        dispatch::raw::Manifold::shutdown(self.as_mut().project().udp, turn, driver);
        self
    }
}

unsafe impl<'d, const ID: u8, H, B, A, M, Q: Residency + buffer::raw::Source>
    dispatch::raw::Control<'d> for endpoint::Endpoint<'d, ID, H, B, A, M, Q>
where
    H: handler::Handler<ID, B, resident::Shared<'d>, id::Driver<'d>>,
    B: Storage<'d, resident::Shared<'d>>,
    M: owner::Binding<'d, ID>,
    endpoint::Endpoint<'d, ID, H, B, A, M, Q>: dispatch::raw::Manifold<'d>,
{
    type View<'step>
        = control::Control<'step, 'd, ID, H, B, A, M, Q>
    where
        Self: 'step,
        'd: 'step;

    fn view<'step>(owner: dispatch::raw::Installed<'step, 'd, Self>) -> Self::View<'step>
    where
        'd: 'step,
    {
        control::Control::new(owner)
    }
}

// SAFETY: both variants retain their exact source extent and resident owner.
unsafe impl<Q: buffer::raw::Source> buffer::raw::Source for endpoint::Payload<Q> {}

// SAFETY: admission consumes the original Retry suffix and allocation together,
// after every possible rejection of the generative operation owner.
unsafe impl<Q> buffer::raw::Convert<endpoint::Payload<Q>> for Transit<'_, retry::Suffix> {
    fn into(self) -> endpoint::Payload<Q> {
        endpoint::Payload::Retry(self.value)
    }
}

// SAFETY: the pending entry retains the exact stable source through shared access.
unsafe impl<Q: buffer::raw::Source> buffer::raw::Source for Entry<Q> {}

// SAFETY: the transparent entry's original packet, visible range, residency and
// lifetime move unchanged after admission succeeds.
unsafe impl<Q> buffer::raw::Convert<endpoint::Payload<Q>> for Entry<Q> {
    fn into(self) -> endpoint::Payload<Q> {
        endpoint::Payload::Pooled(self.packet)
    }
}
