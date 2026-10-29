use std::{cell::Cell, convert::Infallible, num::NonZero, rc::Rc};

use dope_core::{
    driver,
    driver::{route::KeyTag, schedule::ready},
    io::socket,
};
use dope_manifold::{
    app::Park,
    client::{
        app::{self, admit::Automatic, connect::Full, failure::Observed, request},
        attempt,
        close::{Directive, LocalOrigin, Policy, Reason},
        conn, deadline,
        session::{
            self, Output, Parser, Plain, Retirement, Scheduling, Session, Target,
            app::drain::Ready,
            codec::{Input, mode},
            policy::{self, output},
            response, turns,
        },
    },
};
use dope_net::{
    link::{egress, event, pool::change::opening},
    wire::{self, failure},
};
use dope_test::harness::signals::{Gate, Progress as _};
use o3::{
    base::{Call, cell::region},
    buffer,
    buffer::storage::Shared,
};

use crate::common::{
    contract::{Report, codec, codec::errors},
    manifold::client::{
        error::{self, Error::Finish},
        finish::Error,
        flush,
    },
};

pub const RESPONSE: &[u8] = b"REMOTE-BEFORE-EOF";
pub const AFTER_EOF: &[u8] = b"WRITE-AFTER-REMOTE-EOF";

#[derive(Clone, Copy, Debug)]
pub enum Action {
    Send,
    Close,
    Timeout,
}

pub struct Half {
    pub(crate) action: Action,
    pub(crate) progress: Progress,
    pub(crate) response: Option<Shared>,
    pub(crate) audit: Rc<Report<error::Error, 16>>,
    pub(crate) conn: Gate,
    pub(crate) closed: Gate,
    pub(crate) input: Rc<Cell<Vec<u8>>>,
}

pub struct Progress {
    pub(crate) eof: bool,
    pub(crate) issued: bool,
    pub(crate) sent: bool,
    pub(crate) parked: Gate,
    pub(crate) release: Gate,
}

impl<'d> Session<'d> for Half {
    type Codec = codec::Pending;
    type Connect = Full;
    type ConnState = Option<ready::Target<'d>>;
    type Failure = Observed;
    type Requests = request::Disabled;
    type Send = Shared;
    fn codec(&self) -> &Self::Codec {
        &codec::Pending
    }

    fn mode(&self, _: conn::Id<'d, KeyTag<0>>, _: &Self::ConnState) -> session::ReceiveMode {
        if matches!(self.action, Action::Timeout) && self.progress.parked.hits() != 0 {
            session::ReceiveMode::Paused
        } else {
            session::ReceiveMode::Duplex
        }
    }

    fn duplex<'id, 'input, R: buffer::Cursor>(
        &mut self,
        input: Input<'id, 'input, 'd, R>,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> session::Duplex<'id>
    where
        'd: 'input,
    {
        let Some(count) = NonZero::new(input.len()) else {
            return session::Duplex::Blocked;
        };
        let consumed = match input.try_consumed(count) {
            Ok(consumed) => consumed,
            Err(cause) => return session::Duplex::Failed(cause),
        };
        let bytes = input.as_slice();
        let mut received = self.input.take();
        received.extend_from_slice(bytes);
        self.input.set(received);
        if matches!(self.action, Action::Timeout) {
            self.progress.parked.hit();
            return session::Duplex::Blocked;
        }
        session::Duplex::Consumed(consumed)
    }

    fn eof(
        &mut self,
        context: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> app::Eof {
        let target = context.wake_target();
        *context.state = Some(target);
        self.progress.eof = true;
        target.wake();
        app::Eof::Retain
    }
}

impl<'d, W> policy::Compatible<'d, 'd, W, KeyTag<0>> for Half
where
    W: wire::Spec<OpenFailure = failure::Never, TerminalFailure = failure::Never> + 'd,
{
    type Open = Self;
    type Terminal = Self;

    fn open(&self) -> &Self::Open {
        self
    }

    fn terminal(&self) -> &Self::Terminal {
        self
    }
}

impl<'d> Parser<'d> for Half {
    type Mode = mode::Plain;
    type Response = response::Immediate;
}

impl<'d> Plain<'d> for Half {
    type Delivery = response::Direct;

    fn reject(
        &mut self,
        _: errors::Truncated,
        ctx: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> response::Direct {
        use dope_manifold::client::close;

        ctx.request(close::Directive::reconnect(close::Reason::Protocol));
        response::Direct::Committed
    }

    fn deliver<'input, R: buffer::Cursor>(
        &mut self,
        head: Infallible,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) -> response::Direct
    where
        'd: 'input,
    {
        match head {}
    }
}

impl<'d> Output<'d> for Half {
    fn flush() -> impl for<'a, 'c, 't, 'e> Call<(
        &'a mut Self,
        &'c mut turns::Context<'t, 'e, 'd, Self::ConnState, Self::Send>,
    )> {
        flush(|owner: &mut Self, context| {
            let conn = owner.conn.hits();
            let closed = owner.closed.hits();
            if conn != 1 || closed != 0 {
                owner.audit.faults.record(Finish(Error::Flush {
                    connected: conn,
                    closed,
                    empty: context.sink.is_empty(),
                }));
            }
            if !owner.progress.eof || owner.progress.issued || owner.progress.release.hits() == 0 {
                return;
            }
            owner.progress.issued = true;
            match owner.response.take() {
                Some(response) => {
                    let mut parts = context.parts();
                    match parts
                        .sink
                        .try_enqueue(parts.driver.region_token(), response)
                    {
                        egress::Enqueue::Committed => {}
                        egress::Enqueue::Rejected(error) => {
                            owner.audit.faults.record(error::Error::Enqueue(error));
                        }
                    }
                }
                None => {
                    context.request(Directive::reconnect(Reason::Local(LocalOrigin::ApiRequest)))
                }
            }
        })
    }

    fn sent() -> impl for<'a, 't> Call<(
        &'a mut Self,
        conn::Id<'d, KeyTag<0>>,
        egress::Pending,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Sent::<Infallible, Infallible, Infallible, _>::Queue(
            |(owner, _, pending): (&mut Self, conn::Id<'d, KeyTag<0>>, egress::Pending)| {
                let conn = owner.conn.hits();
                if !matches!(pending, egress::Pending::Absent) || conn != 1 {
                    owner.audit.faults.record(Finish(Error::HalfSent {
                        connected: conn,
                        pending: matches!(pending, egress::Pending::Present),
                    }));
                }
                owner.audit.record_sent();
                owner.progress.sent = true;
            },
        )
    }

    fn cancel() -> impl for<'a, 't> Call<(
        &'a mut Self,
        Self::Send,
        &'t mut region::Token<region::Identity<'d, 1>>,
    )> {
        use Infallible;

        output::Cancel::<Infallible, Infallible, Infallible, _, _>::Ready(
            drop::<Self::Send>,
            |owner: &mut Self| {
                owner.audit.cancelled();
            },
        )
    }
}

impl<'d> session::Connected<'d> for Half {
    type Activation = Automatic;

    fn connect(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        _: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        self.conn.hit();
    }
}

impl<'d> session::Failed<'d> for Half {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Connect(error));
    }
}

impl<'d> session::Exhausted<'d> for Half {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        error: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.audit.faults.record(error::Error::Exhaust(error));
    }
}

impl<'d> Retirement<'d> for Half {
    type Drain = Ready;

    fn io_fail(
        &mut self,
        error: dope_core::Errno,
        _: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send>,
    ) {
        self.audit.faults.record(error::Error::Receive(error));
    }

    fn quarantine(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut Self::ConnState,
        reason: Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let conn = self.conn.hits();
        let closed = self.closed.hits();
        let issued = self.progress.issued;
        let sent = self.progress.sent;
        if conn != 1
            || closed != 0
            || match self.action {
                Action::Send => !issued || !sent || reason != Reason::Remote,
                Action::Close => !issued || reason != Reason::Local(LocalOrigin::ApiRequest),
                Action::Timeout => {
                    issued
                        || sent
                        || self.progress.eof
                        || self.progress.parked.hits() != 1
                        || reason != Reason::Timeout(deadline::Kind::Lifetime)
                }
            }
        {
            self.audit.faults.record(Finish(Error::HalfRetired {
                reason,
                connected: conn,
                closed,
                issued,
                sent,
                action: self.action,
            }));
        }
    }

    fn fence(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        state: &Self::ConnState,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> app::Fence {
        if let (Some(target), false) = (state, self.progress.sent) {
            target.wake();
            app::Fence::Pending
        } else {
            app::Fence::Ready
        }
    }

    fn clear_send<'turn>(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        egress::Cleared::Done
    }

    fn policy(
        &self,
        _: conn::Id<'d, KeyTag<0>>,
        _: &Self::ConnState,
        _: Reason,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> Policy {
        match self.action {
            Action::Timeout => Policy::Permanent,
            Action::Send | Action::Close => Policy::Reconnect,
        }
    }

    fn disconnect(&mut self, _: turns::Retired<'_, 'd, Self::ConnState, KeyTag<0>, Self>) {
        self.closed.hit();
        let conn = self.conn.hits();
        let closed = self.closed.hits();
        if closed > conn {
            self.audit.faults.record(Finish(Error::Epoch {
                connected: conn,
                closed,
            }));
        }
    }
}

impl<'d> Scheduling<'d> for Half {
    fn park() -> Option<impl for<'v, 'w, 'x, 'y> Call<(&'v mut Self, &'w mut Park<'x, 'y, 'd>)>> {
        Some(|(session, _): (&mut Self, &mut Park<'_, '_, 'd>)| {
            if matches!(session.action, Action::Send | Action::Close)
                && session.progress.eof
                && !session.progress.issued
            {
                session.progress.parked.hit();
            }
        })
    }
}
impl<'d> Target<'d, KeyTag<0>, 1> for Half {}
