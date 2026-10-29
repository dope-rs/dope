use std::{cell::Cell, convert::Infallible, pin::Pin, rc::Rc};

use dope_core::{
    driver::{retained, route::KeyTag, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::{env::Bundle, receive},
    network::listener::{
        self, conn,
        handler::{self, failures, retire, send},
    },
    timing::Throughput,
};
use dope_net::{tcp::Tcp, wire::Identity};
use dope_runtime::control;
use dope_test::harness::signals::Gate;
use o3::buffer::{bytes::TryRetain, resident};

pub struct App<'d> {
    pub(crate) id: Rc<Cell<Option<conn::Id<'d, KeyTag<0>>>>>,
    pub(crate) closed: Gate,
}

impl<'d> handler::Application<'d, KeyTag<0>> for App<'d> {
    type Conn = ();
    type Build = Infallible;
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = receive::Borrowed;
    type Send = send::Silent;

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<(), Infallible> {
        Ok(())
    }

    fn build_failed(self: Pin<&mut Self>, cause: Infallible) {
        match cause {}
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn accept(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, KeyTag<0>, Identity, ()>,
    ) -> receive::Outcome {
        self.id.set(Some(conn.id()));
        receive::Outcome::Continue
    }

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, KeyTag<0>, Identity, ()>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, KeyTag<0>, Identity, ()>
    where
        'd: 'w,
    {
        self.closed.hit();
        conn
    }
}

impl<'d> handler::Borrowed<'d, KeyTag<0>> for App<'d> {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'_, '_, '_, '_, '_, 'd, KeyTag<0>, Identity, ()>,
        _: R,
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> receive::Outcome {
        receive::Outcome::Continue
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime, driver = 'd)]
#[coordinate]
pub struct Host<'n, 'p, 's, 'd: 's> {
    #[pin]
    #[manifold(control)]
    pub(crate) listener: listener::Listener<
        'n,
        'p,
        's,
        'd,
        'd,
        KeyTag<0>,
        App<'d>,
        Bundle<Tcp, Identity, Throughput>,
    >,
    #[dispatcher(state)]
    pub(crate) id: Rc<Cell<Option<conn::Id<'d, KeyTag<0>>>>>,
}

impl<'n, 'p, 's, 'd> Host<'n, 'p, 's, 'd> {
    fn coordinate(mut this: host_coord::Fields<'_, 'n, 'p, 's, 'd>) -> control::Flow {
        if let Some(id) = this.id.take() {
            this.listener.close_after(id);
        }
        control::Flow::Idle
    }
}
