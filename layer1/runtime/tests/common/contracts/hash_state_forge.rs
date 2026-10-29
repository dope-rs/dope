use dope_core::random::Seed;
use dope_runtime::random::{Domain, HashState};

fn forge<'d>() {
    let _ = |seed: &Seed, domain: Domain| -> HashState<'d> { HashState::for_seed(seed, domain) };
}

fn main() {}
