pub mod capacity;
mod packet;
mod receive;
pub mod shutdown;

use dope_core::driver::route::KeyTag;
use dope_manifold::network::datagram::Endpoint;
pub use packet::Packet;
pub use receive::Receive;

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime)]
pub struct App<'d, H> {
    #[pin]
    #[manifold]
    pub(crate) endpoint: Endpoint<'d, KeyTag<0>, H>,
}
