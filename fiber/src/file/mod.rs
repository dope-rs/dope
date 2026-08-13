mod open;
mod readall;

use std::io;

pub use open::regular::OpenRegular;
pub use readall::ReadAll;
fn already_done() -> io::Error {
    use std::io::Error;
    Error::other("dope::file: fiber polled after completion")
}
