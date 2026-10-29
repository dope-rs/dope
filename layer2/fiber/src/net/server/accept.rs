use std::task;

use dope::{
    core::driver::route::{Identity, KeyTag},
    net::wire,
};

use crate::{
    abi, context,
    manifold::network::listener::{self, conn},
    net::{self, server},
    wait,
};

/// Exact waiter admission or terminal listener admission failure.
pub enum Error<'d> {
    Wait(wait::Error<'d>),
    Listener(listener::AcceptError),
}

#[pin_project::pin_project]
#[must_use = "a fiber does nothing unless it is driven"]
pub struct Accept<'scope, 'w, 'd: 'w, W: wire::Spec + 'w, ID: Identity = KeyTag<0>> {
    host: server::Handle<'scope, 'w, 'd, W, ID>,
    #[pin]
    waiter: wait::Waiter<'scope, 'd>,
}

impl<'scope, 'w, 'd: 'w, W: wire::Spec + 'w, ID: Identity> Accept<'scope, 'w, 'd, W, ID> {
    pub(super) fn new(host: server::Handle<'scope, 'w, 'd, W, ID>) -> Self {
        Self {
            host,
            waiter: wait::Waiter::new(),
        }
    }
}

impl<'scope, 'w, 'd: 'w, W: wire::Spec + 'w, ID: Identity> abi::Fiber<'d>
    for Accept<'scope, 'w, 'd, W, ID>
{
    type Output = Result<net::Io<'scope, 'w, 'd, W, conn::Id<'d, ID>>, Error<'d>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        use task::Poll;

        let (this, context) = call.into_parts();
        let this = this.project();
        let waiter = this.waiter;
        let Some((_, accepted)) = this.host.port.accepts.next::<0>() else {
            if let Some(error) = this.host.port.accept_error.get() {
                waiter.as_ref().unregister();
                return Poll::Ready(Err(Error::Listener(error)));
            }
            return match this
                .host
                .port
                .waiters
                .try_register(waiter.as_ref(), context.as_ref())
            {
                Ok(()) => Poll::Pending,
                Err(error) => Poll::Ready(Err(Error::Wait(error))),
            };
        };
        waiter.as_ref().unregister();
        Poll::Ready(Ok(net::Io::new(&this.host.port.connections, accepted)))
    }
}
