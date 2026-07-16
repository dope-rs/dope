mod open;
mod read;
mod splice;
mod stat;

use std::io;
use std::io::Error;

use dope::manifold::file::SourceRef;
pub use dope::manifold::file::{Direct, Fixed, Metadata, Source};
pub use open::{Open, OpenKind};
pub use read::{BlockRead, Read};
pub use splice::SpliceToPipe;
pub use stat::Stat;

use dope::driver::token::Token;
enum Stage {
    Init,
    Pending(Token),
    Done,
}

impl Stage {
    fn already_done() -> io::Error {
        Error::other("dope::file: fiber polled after completion")
    }
}
