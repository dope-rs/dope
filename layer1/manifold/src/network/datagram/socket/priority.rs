use std::pin;

use dope_core::driver::{result, retained, route::Identity, schedule::app};
use o3::buffer;

use crate::network::datagram::{self, send};

#[derive(Clone, Copy)]
pub(in crate::network::datagram) enum Priority {
    Application,
    Receive,
    Send,
}

impl Priority {
    fn next(self) -> Self {
        match self {
            Self::Application => Self::Receive,
            Self::Receive => Self::Send,
            Self::Send => Self::Application,
        }
    }
}

/// Each participant leads within three passes with nonzero budget and uses
/// the remaining ledger, so idle participants never reserve unused credits.
pub(in crate::network::datagram) fn run<
    'a,
    'c,
    'owner,
    'd: 'owner,
    ID: Identity,
    B: buffer::Residency + buffer::raw::Source,
    M: send::Mode,
    H,
>(
    mut socket: pin::Pin<&'a mut datagram::Socket<'d, ID, B, M>>,
    mut handler: pin::Pin<&mut H>,
    work: app::Work<'a, 'd>,
    driver: &'a mut retained::Context<'c, 'owner, 'd>,
    mut pre_park: impl FnMut(
        pin::Pin<&mut H>,
        pin::Pin<&mut datagram::Socket<'d, ID, B, M>>,
        app::Work<'_, 'd>,
        &mut retained::Context<'c, 'owner, 'd>,
    ),
    mut transfer: impl FnMut(pin::Pin<&mut H>, send::Transfer<'d, ID>),
) -> Result<(), result::SubmitError> {
    if !socket.sender.is_accepting() {
        pre_park(handler, socket.as_mut(), work, driver);
        socket.project().receive.retry_stop(driver);
        return Ok(());
    }
    let first = socket.priority;
    if work.remaining() != 0 {
        *socket.as_mut().project().priority = first.next();
    }
    let mut step = |priority| match priority {
        Priority::Application => {
            pre_park(handler.as_mut(), socket.as_mut(), work, driver);
            Ok(())
        }
        Priority::Receive => {
            let this = socket.as_mut().project();
            this.receive
                .arm(this.binding.descriptor(), this.recv_flights, work, driver)
        }
        Priority::Send => {
            let this = socket.as_mut().project();
            let fd = this.binding.descriptor();
            this.sender.flush_with(
                |_| fd,
                work,
                driver,
                Some(|event| transfer(handler.as_mut(), event)),
            )
        }
    };
    let result = step(first)
        .and_then(|()| step(first.next()))
        .and_then(|()| step(first.next().next()));
    if let Err(error) = result {
        let this = socket.project();
        this.sender.fail(error);
        this.receive.stop(driver);
    }
    result
}
