use std::{cell::Cell, net::SocketAddr, task::Poll};

use dope::runtime::executor::root;

use crate::common::refresh::server::{Failure, Server};

pub(crate) struct Until<'a> {
    changes: &'a Cell<u32>,
    before: u32,
    server: &'a mut Server,
    lost: u8,
}

impl<'a> Until<'a> {
    pub(crate) fn new(changes: &'a Cell<u32>, server: &'a mut Server) -> Self {
        Self {
            changes,
            before: changes.get(),
            server,
            lost: 0,
        }
    }
}

impl<'d> root::Root<'d> for Until<'_> {
    type Output = Result<Option<(Vec<u8>, SocketAddr)>, Failure>;

    fn poll(context: root::Context<'_, 'd, Self>) -> Poll<Self::Output> {
        let (this, wake, _, _) = context.into_parts();
        let this = this.get_mut();
        match this.server.poll(&mut this.lost) {
            Ok(Some(reply)) => return Poll::Ready(Ok(Some(reply))),
            Ok(None) => {}
            Err(error) => return Poll::Ready(Err(error)),
        }
        if this.changes.get() != this.before || this.lost == 3 {
            Poll::Ready(Ok(None))
        } else {
            wake.wake();
            Poll::Pending
        }
    }
}
