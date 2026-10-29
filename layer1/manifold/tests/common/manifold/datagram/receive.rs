use dope_core::{
    driver::route::{Identity, Materialize, kind, table},
    io,
};
use dope_test::reclaim::Matches;

pub struct Receive<ID: Identity>(pub(crate) table::Parts<<ID as Materialize<kind::Recv>>::With>);

impl<ID: Identity> Matches<io::Reclaimed> for Receive<ID> {
    fn matches(&self, event: &io::Reclaimed) -> bool {
        matches!(event, io::Reclaimed::RecvRoute { target, result, flow }
            if target.route() == ID::ID.into()
            && target.kind() == Ok(kind::Kind::Recv)
            && target.slot() == self.0.slot()
            && target.epoch() == self.0.epoch()
            && *result == Err(dope_core::Errno::CANCELED)
            && matches!(flow, io::event::batch::Flow::Last))
    }
}
