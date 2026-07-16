extern crate dope;

use dope::manifold::file::Files;
use dope_fiber::file::{Fixed, Source, Stat};

fn stat_fixed<'d>(files: &Files<'d, 1, 1>, source: &Source<'d, Fixed>) {
    let _ = Stat::source(files, source);
}

fn main() {}
