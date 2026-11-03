use dope_core::dope_sys::io::socket::route::selector::{Frame, PrefixError, PrefixPlan};
use dope_manifold::network::datagram::packet::Packet;
use o3::buffer::bytes::scope;

fn substitute<'first, 'second, 'turn, 'd>(
    first: scope::Owner<'first, Packet<'turn, 'd>>,
    second: scope::Owner<'second, Packet<'turn, 'd>>,
) -> Result<Packet<'turn, 'd>, PrefixError> {
    let plan = PrefixPlan::fixed::<31, 0xc0, 0x14, 0x16>();
    match plan.try_parse(first.view())? {
        Frame::Direct => Ok(second.into_inner()),
        Frame::Prefixed { remaining, .. } => Ok(second.project(remaining)),
    }
}

fn main() {}
