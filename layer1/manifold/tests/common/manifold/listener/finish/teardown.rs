//! Listener teardown and retained-credit integration coverage.

use std::{convert, fs, io, path::PathBuf, pin::Pin};

use dope_core::{
    driver::{
        retained, route,
        route::table::conn::Capacity,
        schedule::clock,
        settings,
        settings::{
            claim::{Claim, Needs},
            demands::{Accept, Files},
            errors,
        },
    },
    io::recv,
    random::Seed,
};
use dope_manifold::{
    app::receive::{Outcome, Owned, limits},
    network::listener::{
        conn,
        handler::{self, Application, failures, retire, send},
    },
    timing::Throughput,
};
use dope_net::wire::{
    self, Identity, domain, failure, open, receive,
    receive::{Chunk, batch, eof, mode, reserve},
    send::Direct,
    terminal,
};
use o3::buffer::resident;
use wire::receive::{mode::access, reserve::Completion as _};

pub struct Teardown;

pub struct Recv;

impl receive::Policy<Teardown> for Recv {
    type Batch<'a>
        = batch::One<&'a [u8]>
    where
        Teardown: 'a;
    type Cursor<'w, 'd>
        = recv::Shared<'d>
    where
        Teardown: 'w,
        'd: 'w;
    type Access = access::Direct;
    type Eof = eof::Clean;
    type Reserve = reserve::Direct;
    type Credit = mode::Enabled;
    type Discard = mode::Enabled;

    fn process<'id, 'a, 'input, 'w, 'd, ID: route::Identity + 'w>(
        txn: reserve::Guard<ID, Teardown>,
        bytes: &'input mut [u8],
        capacity: batch::Grant<'id, 'd, Teardown>,
        end: terminal::Both<'_, terminal::Error<'w, Teardown>>,
    ) -> terminal::Processed<batch::Produced<'id, Self::Batch<'input>>>
    where
        Teardown: 'w,
        'd: 'w,
        'w: 'a + 'input,
    {
        let output = capacity.one(&*bytes);
        txn.complete();
        end.output(output)
    }

    fn read_lease<'a, 'w, 'd, ID: route::Identity + 'w>(
        txn: reserve::Guard<ID, Teardown>,
        bytes: recv::Lease<'d>,
        end: terminal::Both<'_, terminal::Error<'w, Teardown>>,
    ) -> terminal::Processed<Option<Self::Cursor<'w, 'd>>>
    where
        Teardown: 'w,
        'd: 'w,
        'w: 'a,
    {
        let output = Some(bytes.into_shared());
        txn.complete();
        end.output(output)
    }
}

impl<'w, 'd: 'w, ID: route::Identity + 'w> open::Runtime<'w, 'd, Teardown, ID> for Teardown {
    fn open<C, T>(
        &mut self,
        context: C,
        consume: impl FnOnce(C, Identity, ()) -> open::Disposition<T, Identity, ()>,
    ) -> open::Outcome<C, T, open::Error<'w, Teardown>> {
        let output = match consume(context, Identity, ()) {
            open::Disposition::Commit(output) | open::Disposition::Retry { output, .. } => output,
        };
        open::Outcome::Opened(output)
    }
}

impl wire::Spec for Teardown {
    type Connection<'d, ID: route::Identity + 'd>
        = Identity
    where
        Self: 'd;
    type Storage<ID: route::Identity> = ();
    type Binding<'d, ID: route::Identity + 'd>
        = ()
    where
        Self: 'd;
    type Runtime<'w, 'd, ID: route::Identity + 'w>
        = Self
    where
        Self: 'w,
        'd: 'w;
    type SendStorage<'d>
        = ()
    where
        Self: 'd;
    type OpenFailure = failure::Never;
    type TerminalFailure = failure::Never;

    type Bootstrap = Teardown;
    type Receive = Recv;
    type Send = Direct;
}

impl domain::Init<Teardown> for Teardown {
    fn runtime<'w, 'd, ID: route::Identity + 'w>(
        _: domain::Limits,
        _: <Teardown as wire::Spec>::Binding<'w, ID>,
    ) -> <Teardown as wire::Spec>::Runtime<'w, 'd, ID>
    where
        Teardown: 'w,
        'd: 'w,
    {
        Teardown
    }
}

pub struct App<'w, 'd: 'w, F> {
    pub(crate) held: Option<Chunk<'w, 'd, Teardown>>,
    pub(crate) marker: PathBuf,
    pub(crate) observed: F,
}

impl<'w, 'd: 'w, ID: route::Identity, F: Fn(io::Result<()>) + Unpin> Application<'d, ID>
    for App<'w, 'd, F>
{
    type Conn = ();
    type Build = convert::Infallible;
    type Wire = Teardown;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = Owned;
    type Send = send::Silent;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(())
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }
}

impl<'w, 'd: 'w, ID: route::Identity, F: Fn(io::Result<()>) + Unpin> handler::Owned<'w, 'd, ID>
    for App<'w, 'd, F>
{
    const RETENTION: limits::Retention = limits::Retention::new(1, 0);

    fn chunk(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, 'w, 'd, ID, Teardown, ()>,
        chunk: Chunk<'w, 'd, Teardown>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let this = self.get_mut();
        this.held = Some(chunk);
        (this.observed)(fs::write(&this.marker, b"held"));
        Outcome::Continue
    }
}

pub fn try_config(capacity: Capacity) -> Result<settings::Config, errors::Claim> {
    let accept = Accept::from(capacity);
    let claim = Claim::new(settings::Routes::EMPTY).files(Files::accept(accept));
    Needs::new().include(claim)?.resolve::<Throughput>()
}
