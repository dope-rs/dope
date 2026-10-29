use dope_core::driver::route::Identity;

use crate::{
    client::attempt,
    service::{
        self, discover,
        pool::{dials, members},
        sync,
    },
};

impl<'q, 'h, 'd, T, R, I, const N: usize, F, ID: Identity> attempt::Peer<'d, T, ID>
    for service::Pool<'q, 'h, 'd, ID, T, R, I, N, F>
where
    T: dope_net::Transport,
    R: sync::Policy,
    I: Eq,
    F: discover::Access,
{
    type Target = I;

    fn attach<O>(
        &mut self,
        key: attempt::Id<'d, ID>,
        map: impl FnOnce(&Self::Target) -> O,
    ) -> attempt::Connection<O> {
        if !self.feedback.is_enabled() {
            return attempt::Connection::Disabled;
        }
        let dials::Connection::Pending(connect) = self.dials.connection(key) else {
            return attempt::Connection::Stale;
        };
        let output = match self.membership.recover(connect.target()) {
            members::Peer::Ready { target, output } => {
                let output = map(output);
                connect.commit(target);
                output
            }
            members::Peer::Missing => {
                return attempt::Connection::Retired;
            }
            members::Peer::Exhausted => return attempt::Connection::Exhausted,
        };
        self.refresh();
        attempt::Connection::Applied(output)
    }
}
