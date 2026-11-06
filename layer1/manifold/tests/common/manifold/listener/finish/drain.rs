use std::{convert, pin::Pin};

use dope_core::{
    driver::{self, route, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::{receive, receive::Outcome},
    network::listener::{
        conn::{self, write},
        handler::{self, Application, failures, retire, send},
    },
};
use dope_net::wire::Identity;
use dope_test::harness::signals::Gate;
use o3::buffer::{bytes::TryRetain, resident};

pub struct Drain {
    pub(crate) payload: Vec<u8>,
    pub(crate) gate: Gate,
}

impl<'d, ID: route::Identity> Application<'d, ID> for Drain {
    type Build = convert::Infallible;
    type Conn = ();
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = receive::Borrowed;
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

    fn close<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, ()>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, ID, Identity, ()>
    where
        'd: 'w,
    {
        self.as_ref().get_ref().gate.hit();
        conn
    }
}

impl<'d, ID: route::Identity> handler::Borrowed<'d, ID> for Drain {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, ()>,
        chunk: R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Outcome {
        assert!(!chunk.as_ref().is_empty());
        let payload = &self.as_ref().get_ref().payload;
        let n = payload.len();
        let outcome = conn.write().try_bounded(driver, |&mut (), prefix| {
            prefix[..n].copy_from_slice(payload);
            write::Draft::new(n, None, ()).close_after()
        });
        assert!(matches!(outcome, Ok(write::Outcome::Committed(()))));
        Outcome::Continue
    }
}
