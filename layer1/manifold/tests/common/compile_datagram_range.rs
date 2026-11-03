use dope_core::dope_sys::io::socket::route::selector::{Frame, PrefixError, PrefixPlan};
use dope_manifold::network::datagram::packet::Packet;
use o3::buffer::{bytes::scope, raw::Store, storage::provided};

fn strip<'turn, 'd, T: provided::Mutable<'d> + Store>(
    packet: Packet<'turn, 'd, T>,
) -> Result<Packet<'turn, 'd, T>, PrefixError> {
    scope::Owner::with(packet, |owner| {
        let plan = PrefixPlan::fixed::<31, 0xc0, 0x14, 0x16>();
        match plan.try_parse(owner.view())? {
            Frame::Direct => Ok(owner.into_inner()),
            Frame::Prefixed { remaining, .. } => Ok(owner.project(remaining)),
        }
    })
}

fn native<'turn, 'd>(packet: Packet<'turn, 'd>) -> Result<Packet<'turn, 'd>, PrefixError> {
    strip(packet)
}

fn embedded<'a, 'd>(
    prefix: provided::Prefix<'a, 'd>,
) -> Result<Packet<'a, 'd, provided::Prefix<'a, 'd>>, PrefixError> {
    strip(Packet::from_prefix(prefix))
}

fn main() {}
