use dope::net::link::{
    egress::{self, load, reserve::exact},
    pool::send,
};
use o3::buffer::resident;

#[must_use]
pub enum Write<'d> {
    PortShuttingDown {
        payload: resident::Shared<'d>,
    },
    ConnectionUnavailable {
        payload: Option<resident::Shared<'d>>,
    },
    ConnectionClosed {
        payload: Option<resident::Shared<'d>>,
    },
    /// The connection failed after the write entered its send lifecycle.
    /// Exact wire causes are emitted once through `read`; this coarse result
    /// deliberately does not duplicate or borrow that cause.
    ConnectionFailed {
        payload: Option<resident::Shared<'d>>,
    },
    Rejected(send::Rejected<resident::Shared<'d>>),
    Invalid(egress::Rejected<egress::Load<resident::Shared<'d>>, load::Error>),
    Admission(exact::Unavailable<resident::Shared<'d>>),
}
