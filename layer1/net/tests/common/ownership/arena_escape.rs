use dope_net::link::egress::{
    Config,
    metadata::{LaneCount, arena},
};
use o3::{alloc, base::cell::region};

fn escape<'p, 'd>(
    token: &region::Token<region::Identity<'d>>,
) -> Result<arena::Arena<'p, 'd, &'d [u8]>, alloc::Error> {
    let mut store = arena::Store::try_lanes(Config::DEFAULT, LaneCount::fixed::<1>())?;
    arena::Arena::try_new(token, &mut store)
}

fn main() {}
