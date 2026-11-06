use std::{cell::Cell, net::SocketAddr, pin::Pin};

use dope::runtime::shutdown;
use dope_quic::{
    client::{self, protocol},
    conn::{
        id,
        stream::{Event, SendOutcome},
    },
    errors,
    runtime::{
        endpoint::Endpoint,
        mux::{
            app,
            handler::{
                self,
                hook::{
                    data::release,
                    life::{close, closing, create, ready},
                    set,
                    wire::{cert, endpoint, ingress, packet},
                    writable,
                },
            },
        },
    },
    stream::{self, send},
    wire::stream::Id,
};
use o3::buffer::{resident, storage};

use crate::common::retry::traces::Immediate;

pub const PAYLOAD: &[u8] = b"retained until forced finalization";

#[derive(Default)]
pub struct Observed {
    pub ready: Cell<u8>,
    pub stop: Cell<Option<shutdown::Trigger>>,
    pub released: Cell<Option<stream::release::Event>>,
    pub server_closed: Cell<bool>,
    pub client_closed: Cell<bool>,
}

impl Observed {
    fn ready(&self) {
        let ready = self.ready.get() + 1;
        self.ready.set(ready);
        if ready == 2 {
            self.stop
                .take()
                .expect("shutdown trigger")
                .fire()
                .expect("shutdown signal");
        }
    }
}

pub struct Server<'a>(pub &'a Observed);

impl<'d> handler::Handler<0, Vec<u8>, resident::Shared<'d>, id::Driver<'d>> for Server<'_> {
    type Connection = ();
    type Create = create::Unit;
    type Events = set::Set<
        set::On,
        set::Off,
        set::Off,
        set::Off,
        set::On,
        set::Off,
        ingress::Off,
        packet::On,
        endpoint::Off,
        closing::Off,
        close::On,
    >;
}

impl<'d> ready::Sink<0, Vec<u8>, resident::Shared<'d>, id::Driver<'d>> for Server<'_> {
    fn established(
        event: ready::Event<'_, Self, 0, Vec<u8>, resident::Shared<'d>, id::Driver<'d>>,
    ) {
        let (this, mut slot, _, _) = event.into();
        let mut streams = slot.transport().streams();
        let mut sender = streams.send();
        let stream = sender.open_bidi().expect("one peer stream");
        let payload = storage::Shared::try_copy(PAYLOAD).expect("retained payload");
        assert!(matches!(
            sender.try_buffer(stream.send(), send::Buffer::Retained(payload.into())),
            SendOutcome::Accepted
        ));
        this.0.ready();
    }
}

impl<'d> release::Sink<0, Vec<u8>, resident::Shared<'d>, id::Driver<'d>> for Server<'_> {
    fn release(event: release::Event<'_, Self, 0, id::Driver<'d>>) -> app::Readiness {
        let (this, _, release) = event.into_parts();
        assert!(!this.0.server_closed.get());
        assert!(this.0.released.replace(Some(release)).is_none());
        app::Readiness::Ready
    }
}

impl<'d> close::Sink<0, Vec<u8>, resident::Shared<'d>, id::Driver<'d>> for Server<'_> {
    fn close(event: close::Event<'_, Self, 0, Vec<u8>, resident::Shared<'d>, id::Driver<'d>>) {
        let (this, _, (), _) = event.into_parts();
        assert!(this.0.released.get().is_some());
        assert!(!this.0.server_closed.replace(true));
    }
}

impl<'d> cert::Sink<0, id::Driver<'d>> for Server<'_> {
    fn certificate(event: cert::Event<'_, '_, Self, 0, id::Driver<'d>>) {
        panic!("server certificate: {:?}", event.into_parts().3);
    }
}

impl<'d> packet::Sink<0, Vec<u8>, resident::Shared<'d>, id::Driver<'d>> for Server<'_> {
    fn packet(mut event: packet::Event<'_, Self>) {
        panic!("server packet: {:?}", event.as_parts().2);
    }
}

pub struct Client<'a>(pub &'a Observed);

impl<'d> protocol::Protocol<'d> for Client<'_> {
    type Receive = Vec<u8>;
    type Writable = writable::Off;

    fn connect(self: Pin<&mut Self>, _: client::SlotScope<'_>) {
        self.0.ready();
    }

    fn close(self: Pin<&mut Self>, _: client::SlotScope<'_>) {
        assert!(!self.0.client_closed.replace(true));
    }

    fn failed(self: Pin<&mut Self>, _: client::AttemptScope<'_, 'd>, error: &errors::Connect) {
        panic!("connect: {error:?}");
    }

    fn datagram(self: Pin<&mut Self>, _: client::SlotScope<'_>, _: Vec<u8>) {
        panic!("unexpected datagram");
    }

    fn stream(self: Pin<&mut Self>, _: client::SlotScope<'_>, _: Id, _: Option<Vec<u8>>, _: bool) {
        panic!("peer stream credit is zero");
    }

    fn stream_event(self: Pin<&mut Self>, _: client::SlotScope<'_>, _: Event) {
        panic!("unexpected stream event");
    }

    fn packet(self: Pin<&mut Self>, _: SocketAddr, error: packet::Failure<'_>, _: usize) {
        panic!("packet: {error:?}");
    }

    fn certificate<const DOMAIN: u8>(
        self: Pin<&mut Self>,
        _: id::Handle<id::Driver<'d>, DOMAIN>,
        _: SocketAddr,
        error: &rustls::Error,
    ) {
        panic!("client certificate: {error:?}");
    }
}

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
pub struct App<'d, 'a> {
    #[pin]
    #[manifold]
    pub server: Endpoint<'d, 0, Server<'a>>,
    #[pin]
    #[manifold]
    pub client: client::Dialer<'d, 'd, 1, Client<'a>, Immediate>,
}
