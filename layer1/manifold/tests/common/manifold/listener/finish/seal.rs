use std::{convert::Infallible, pin::Pin};

use dope_core::{
    driver::{retained, route::KeyTag, schedule, schedule::ready},
    random::Seed,
};
use dope_manifold::{
    app::{env::Bundle, receive},
    network::listener::{
        self, conn,
        conn::write,
        handler::{self, failures, retire, send},
    },
    timing::Throughput,
};
use dope_net::{link::slot::types::CloseGate, tcp::Tcp, wire::Identity};
use dope_runtime::executor;
use o3::{
    base::cell::region,
    buffer::{bytes::TryRetain, resident},
    depot::capacity,
};

enum Phase {
    Open,
    Sealing,
    Sealed,
}

pub struct App<'d> {
    phase: Phase,
    wake: Option<ready::Target<'d>>,
}

impl App<'_> {
    pub fn new() -> Self {
        Self {
            phase: Phase::Open,
            wake: None,
        }
    }
}

impl<'d> handler::Application<'d, KeyTag<0>> for App<'d> {
    type Conn = bool;
    type Build = Infallible;
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = resident::Shared<'d>;
    type Input = receive::Borrowed;
    type Send = send::Silent;

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<bool, Infallible> {
        Ok(false)
    }

    fn build_failed(self: Pin<&mut Self>, cause: Infallible) {
        match cause {}
    }

    fn deadline(
        self: Pin<&Self>,
        _: schedule::clock::Clock<'_, 'd>,
    ) -> Option<schedule::clock::Deadline<'d>> {
        None
    }

    fn accept(
        self: Pin<&mut Self>,
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, KeyTag<0>, Identity, bool>,
    ) -> receive::Outcome {
        let this = self.get_mut();
        assert!(this.wake.replace(conn.wake_target()).is_none());
        receive::Outcome::Continue
    }

    fn seal(self: Pin<&mut Self>) -> Pin<&mut Self> {
        let this = self.get_mut();
        this.phase = Phase::Sealing;
        this.wake.expect("accepted connection").wake();
        Pin::new(this)
    }

    fn activate<'e, 'a, 'p, 's, 'w>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, KeyTag<0>, Identity, bool>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> conn::Ctx<'e, 'a, 'p, 's, 'w, 'd, KeyTag<0>, Identity, bool>
    where
        'd: 'w,
    {
        let this = self.get_mut();
        if matches!(this.phase, Phase::Sealing) {
            this.phase = Phase::Sealed;
            let sent = conn.write().try_bounded(driver, |_, prefix| {
                prefix[0] = b's';
                write::Draft::new(1, None, ())
            });
            assert!(matches!(sent, Ok(write::Outcome::Committed(()))));
        }
        conn
    }

    fn close_gate(
        self: Pin<&Self>,
        conn: conn::Ref<'_, '_, '_, '_, 'd, KeyTag<0>, Identity, bool>,
    ) -> CloseGate {
        if *conn.state() {
            CloseGate::Immediate
        } else {
            CloseGate::Deferred
        }
    }
}

impl<'d> handler::Borrowed<'d, KeyTag<0>> for App<'d> {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, KeyTag<0>, Identity, bool>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> receive::Outcome {
        let byte = match self.phase {
            Phase::Open => b'a',
            Phase::Sealed => b'b',
            Phase::Sealing => panic!("input before seal notice"),
        };
        assert_eq!(chunk.as_ref(), [byte]);
        *conn.state_mut() = byte == b'b';
        let sent = conn.write().try_bounded(driver, |_, prefix| {
            prefix[0] = byte;
            write::Draft::new(1, None, ())
        });
        assert!(matches!(sent, Ok(write::Outcome::Committed(()))));
        receive::Outcome::Continue
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(core = ::dope_core, manifold = ::dope_manifold, runtime = ::dope_runtime, driver = 'd)]
pub struct Host<'n, 'p, 's, 'd: 's> {
    #[pin]
    #[manifold]
    pub listener: listener::Listener<
        'n,
        'p,
        's,
        'd,
        'd,
        KeyTag<0>,
        App<'d>,
        Bundle<Tcp, Identity, Throughput>,
    >,
}

// SAFETY: sealing preserves the installed listener and every admitted connection
// through the generated native shutdown and finish traversals.
unsafe impl<'n, 'p, 's, 'd> executor::raw::Drain<'d> for Host<'n, 'p, 's, 'd> {
    fn begin(
        self: Pin<&mut Self>,
        _: schedule::Turn<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        self.project().listener.seal(driver.driver());
    }

    fn progress(
        self: Pin<&Self>,
        region: &region::Token<region::Identity<'d>>,
    ) -> schedule::Progress<'d> {
        if self.listener.len() == capacity::Count::EMPTY {
            schedule::Progress::Quiescent
        } else {
            schedule::Progress::waiting(region)
        }
    }

    fn force(
        self: Pin<&mut Self>,
        _: schedule::Turn<'_, 'd>,
        _: &mut retained::Context<'_, '_, 'd>,
    ) {
    }
}
