use std::convert;

use dope_core::driver::{result, route::Identity};
use dope_net::link::pool::change::opening;
use o3::depot::capacity;

use crate::{
    client::attempt::{self, mode},
    service::{self, discover, sync},
};

impl<'q, 'h, 'd, T, R, I, const N: usize, F, ID: Identity> attempt::Control<'d, T, ID>
    for service::Pool<'q, 'h, 'd, ID, T, R, I, N, F>
where
    T: dope_net::Transport,
    R: sync::Policy,
    I: Eq,
    F: discover::Access,
{
    type Mode = mode::Service;
    type Error = convert::Infallible;

    fn resize(&mut self, cap: capacity::Count) -> Result<(), attempt::ResizeError> {
        let avail = self.dials.capacity();
        if avail < cap {
            return Err(attempt::ResizeError::new(cap, avail));
        }
        Ok(())
    }

    fn is_pending(&self) -> bool {
        self.dials.needs_poll
    }

    fn connected(&mut self, key: attempt::Id<'d, ID>) -> Result<attempt::Connection, Self::Error> {
        Ok(attempt::Peer::attach(self, key, |_| ()))
    }

    fn rejected(
        &mut self,
        key: attempt::Id<'d, ID>,
        error: result::SubmitError,
    ) -> result::SubmitError {
        if error.cause() == result::SubmitCause::IdentityExhausted
            && self.dials.active(key).is_some()
        {
            self.reject(key, opening::Exhaustion::Submission(error));
        }
        error
    }

    fn kill(
        &mut self,
        key: attempt::Id<'d, ID>,
        cause: Option<opening::Exhaustion>,
    ) -> Result<(), Self::Error> {
        if let Some(cause) = cause {
            let _ = self.exhaust(key, cause);
        } else {
            if let Some(target) = self.dials.close(key) {
                self.membership.cancel(target);
            }
            self.refresh();
        }
        Ok(())
    }
}
