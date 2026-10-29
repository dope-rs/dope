use std::{cell::Cell, convert, pin::Pin, rc::Rc};

use dope_core::{
    driver::{retained::Context, route, schedule::clock},
    io::socket,
    random::Seed,
};
use dope_manifold::{
    app::receive::{Borrowed, Outcome},
    network::listener::{
        conn,
        handler::{self, Application, failures, retire, send::Silent},
    },
};
use dope_net::wire::{
    self, Identity, domain, domain::Limits, failure, open, receive, receive::mode, send, terminal,
};
use dope_test::harness::{observe::Trace, signals::Gate};
use o3::{
    alloc,
    base::Call,
    buffer::{bytes::TryRetain, resident},
};

pub struct Open;

pub struct Wire;

pub struct Bootstrap;

impl failure::Errors<failure::Open> for Wire {
    type Error<'d>
        = Open
    where
        Self: 'd;
}

pub struct Runtime(pub(crate) Cell<bool>);

impl<'w, 'd: 'w, ID: route::Identity + 'w> open::Runtime<'w, 'd, Wire, ID> for Runtime {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Identity, ()) -> open::Disposition<T, Identity, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Wire>> {
        if self.0.replace(false) {
            return open::Outcome::Failed {
                context,
                cause: open::Error::from_cause(Open),
            };
        }
        let output = match consume(context, Identity, ()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        open::Outcome::Opened(output)
    }
}

impl domain::Init<Wire> for Bootstrap {
    fn runtime<'w, 'd, ID: route::Identity + 'w>(
        _: Limits,
        _: <Wire as wire::Spec>::Binding<'w, ID>,
    ) -> <Wire as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Wire: 'w,
        'd: 'w,
    {
        Runtime(Cell::new(true))
    }
}

impl wire::Spec for Wire {
    type Connection<'d, ID: route::Identity + 'd> = Identity;
    type Storage<ID: route::Identity> = ();
    type Binding<'d, ID: route::Identity + 'd> = ();
    type Runtime<'w, 'd, ID: route::Identity + 'w>
        = Runtime
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d> = ();
    type OpenFailure = failure::Possible;
    type TerminalFailure = failure::Never;

    type Bootstrap = Bootstrap;
    type Receive = receive::Transparent<mode::Disabled>;
    type Send = send::Direct;
}

pub struct Opening {
    pub(crate) accepted: Rc<Cell<u32>>,
    pub(crate) gate: Gate,
    pub(crate) failed: Gate,
}

impl<'w, 'd: 'w, ID: route::Identity> failures::Policy<'w, 'd, Opening, ID> for Opening {
    fn open_failed(app: Pin<&mut Self>, cause: open::Error<'w, Wire>) {
        let Open = cause.into_cause();
        app.get_mut().failed.hit();
    }

    fn peer() -> Option<impl for<'a> Call<(Pin<&'a mut Self>, socket::Error)>> {
        None::<convert::Infallible>
    }

    fn wire_failed<'e>(
        _: Pin<&mut Self>,
        _: conn::Ctx<'e, '_, '_, '_, 'w, 'd, ID, Wire, ()>,
        cause: terminal::Error<'w, Wire>,
    ) {
        cause.eliminate()
    }
}

impl<'d, ID: route::Identity> Application<'d, ID> for Opening {
    type Conn = ();
    type Build = convert::Infallible;
    type Wire = Wire;
    type WireFailure = Self;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = Borrowed;
    type Send = Silent;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(())
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn accept(self: Pin<&mut Self>, _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, ()>) -> Outcome {
        let this = self.get_mut();
        this.accepted.set(this.accepted.get() + 1);
        this.gate.hit();
        Outcome::CloseAfter
    }
}

impl<'d, ID: route::Identity> handler::Borrowed<'d, ID> for Opening {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Wire, ()>,
        _: R,
        _: &mut Context<'_, '_, 'd>,
    ) -> Outcome {
        Outcome::Continue
    }
}

pub struct Allocation {
    pub(crate) reject: Cell<bool>,
    pub(crate) accepted: Rc<Cell<u32>>,
    pub(crate) gate: Gate,
    pub(crate) failed: Gate,
    pub(crate) closed: Gate,
    pub(crate) causes: Rc<Trace<alloc::Error, 1>>,
}

impl<'d, ID: route::Identity> Application<'d, ID> for Allocation {
    type Conn = ();
    type Build = alloc::Error;
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = Borrowed;
    type Send = Silent;

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        if self.reject.replace(false) {
            return Err(alloc::Error::Overflow);
        }
        Ok(())
    }

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        self.causes.record(cause);
        self.failed.hit();
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, ()>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, ()>
    where
        'd: 'w,
    {
        self.closed.hit();
        conn
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn accept(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, ()>,
    ) -> Outcome {
        let this = self.get_mut();
        this.accepted.set(this.accepted.get() + 1);
        this.gate.hit();
        Outcome::CloseAfter
    }
}

impl<'d, ID: route::Identity> handler::Borrowed<'d, ID> for Allocation {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, ()>,
        _: R,
        _: &mut Context<'_, '_, 'd>,
    ) -> Outcome {
        Outcome::Continue
    }
}
