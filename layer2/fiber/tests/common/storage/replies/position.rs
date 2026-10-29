use std::cell::Cell;

use dope::{
    core::{driver, driver::route::KeyTag, io::socket},
    manifold::{
        app::receive,
        client::{self, app, attempt, close, conn},
    },
    net::{
        link::{egress, event, pool::change::opening},
        wire::{Identity, open, terminal},
    },
};
use dope_test::harness::{observe::Trace, signals::Gate};
use o3::{base::cell::region, buffer, buffer::storage::Shared, mem::rc};

#[derive(Debug)]
pub enum Error {
    Connect(event::ConnectFault),
    Exhausted(opening::Exhaustion),
    Receive(dope::core::Errno),
    Input,
    Send,
}

pub struct Session<'a> {
    pub check: for<'d> fn(conn::Id<'d, KeyTag<0>>, &mut region::Token<region::Identity<'d>>),
    pub connected: &'a Gate,
    pub checked: &'a Cell<bool>,
    pub errors: &'a Trace<Error, 4>,
}

impl<'d> app::Application<'d> for Session<'_> {
    type Conn = ();
    type Connect = app::connect::Full;
    type Deferred = app::defer::Controlled;
    type Failure = app::failure::Observed;
    type Wire = Identity;
    type Send = Shared;
    type Input = receive::Borrowed;
    type Requests = app::request::Disabled;

    fn try_conn(&self) -> Result<(), rc::Error> {
        Ok(())
    }
}

impl<'d> app::Receive<'d> for Session<'_> {
    type Continuation = app::resume::Complete;

    fn quiesce<'a>(
        &self,
        barrier: app::Barrier<'a, 'd, KeyTag<0>>,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::Drain<'a, 'd, KeyTag<0>> {
        barrier.ready()
    }
}

impl<'d> app::Borrowed<'d> for Session<'_> {
    fn chunk<'e, O, R: buffer::bytes::TryRetain>(
        &mut self,
        _: conn::Ctx<'_, 'd, KeyTag<0>, Identity, (), O>,
        _: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Shared>,
        _: R,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::ChunkOutcome {
        self.errors.record(Error::Input);
        app::ChunkOutcome::failed(receive::Failure::Contract)
    }
}

impl<'w, 'd: 'w> app::Opened<'w, 'd> for Session<'_> {
    fn open(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        cause: open::Error<'w, Identity>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        cause.eliminate()
    }
}

impl<'d> app::Connected<'d> for Session<'_> {
    type Activation = app::admit::Automatic;

    fn connected<'e, O>(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        _: socket::Addr,
        conn: conn::Established<'_, 'd, KeyTag<0>, Identity, (), O>,
        _: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Shared>,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        if conn.id().index().into_usize() == 1 {
            (self.check)(conn.id(), driver.region_token());
            self.checked.set(true);
        }
        self.connected.hit();
    }
}

impl<'d> app::Exhausted<'d> for Session<'_> {
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.errors.record(Error::Exhausted(cause));
    }
}

impl<'d> app::Failed<'d> for Session<'_> {
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, KeyTag<0>>,
        cause: event::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.errors.record(Error::Connect(cause));
    }
}

impl<'w, 'd: 'w> app::Lifecycle<'w, 'd> for Session<'_> {
    fn prepare<'e, O>(
        &mut self,
        _: conn::Ctx<'_, 'd, KeyTag<0>, Identity, (), O>,
        _: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Shared>,
        _: &mut driver::Context<'_, 'd>,
    ) {
    }

    fn sent(
        &mut self,
        _: conn::Id<'d, KeyTag<0>>,
        _: egress::Pending,
        _: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        self.errors.record(Error::Send);
    }

    fn cancel(&mut self, _: Shared, _: &mut region::Token<region::Identity<'d, 1>>) {
        self.errors.record(Error::Send);
    }

    fn wire_fail<'e, O>(
        &mut self,
        _: conn::Io<'_, 'd, KeyTag<0>, Identity, (), O>,
        _: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Shared>,
        cause: terminal::Error<'w, Identity>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        cause.eliminate()
    }

    fn io_fail<'e, O>(
        &mut self,
        _: conn::Io<'_, 'd, KeyTag<0>, Identity, (), O>,
        _: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Shared>,
        error: dope::core::Errno,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.errors.record(Error::Receive(error));
    }

    fn close<'e, O>(
        &mut self,
        _: conn::Ctx<'_, 'd, KeyTag<0>, Identity, (), O>,
        _: egress::Queue<'_, 'e, 'd, { client::IOV_CAP }, Shared>,
        reason: close::Reason,
        _: &mut driver::Context<'_, 'd>,
    ) -> app::CloseOutcome {
        app::CloseOutcome::Complete(close::Directive::permanent(reason))
    }

    fn drained(
        &mut self,
        _: conn::Retired<'d, KeyTag<0>, Self>,
        _: &mut (),
        _: &mut driver::Context<'_, 'd>,
    ) {
    }
}

impl<'d> app::Scheduling<'d> for Session<'_> {
    fn progress(
        &self,
        _: &region::Token<region::Identity<'d>>,
        _: driver::schedule::clock::Clock<'_, 'd>,
    ) -> driver::schedule::Progress<'d> {
        driver::schedule::Progress::Quiescent
    }
}
