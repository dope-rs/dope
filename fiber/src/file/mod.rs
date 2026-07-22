mod open;
mod read;
mod stat;

use std::io;
use std::io::Error;

pub use dope::manifold::file::{Metadata, Source};
pub use open::Open;
pub use read::Read;
pub use stat::Stat;

fn already_done() -> io::Error {
    Error::other("dope::file: fiber polled after completion")
}
