pub mod open;
pub mod read;
pub mod stat;

use std::io::Error;

use dope::manifold::file::metadata::Metadata;
use dope::manifold::file::source::Source;
fn already_done() -> Error {
    Error::other("dope::file: fiber polled after completion")
}
