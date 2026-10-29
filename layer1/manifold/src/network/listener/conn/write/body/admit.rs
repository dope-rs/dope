use dope_net::link::egress;
use egress::reserve::exact;

use crate::network::listener::conn::write;

/// Owner-preserving result of one fallible body admission transaction.
pub enum Admit<B> {
    Committed,
    Deferred {
        cause: write::Deferred,
        load: egress::NonEmpty<B>,
    },
    Closed(egress::NonEmpty<B>),
    Unavailable(exact::Unavailable<B>),
    Rejected {
        cause: write::Reject,
        load: egress::NonEmpty<B>,
    },
}
