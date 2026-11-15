use std::{net::SocketAddr, ops::ControlFlow, pin::Pin};

use dope::{core::io::datagram::ReceiveError, manifold::network::datagram::RejectCause};
use dope_quic::{
    conn::{id, output::transmit},
    runtime::mux::handler::{
        self,
        hook::{
            life::{close, closing, create},
            set,
            wire::{cert, endpoint, ingress, packet, socket, udp},
            writable,
        },
        net,
    },
    stream::RecvBuffer,
};
use o3::buffer::resident::Shared;

pub const REQUESTS: [&[u8]; 4] = [&[b'A'; 74], b"M", &[b'B'; 74], &[0]];

#[derive(Debug)]
pub enum Failure {
    Receive(ReceiveError),
    Transmit(transmit::Error),
    Certificate(rustls::Error),
}

pub struct Capture<'d> {
    pub peer: SocketAddr,
    pub size: usize,
    pub classified: usize,
    pub settled: usize,
    pub ingress: usize,
    pub rejected: bool,
    pub pending: Option<net::Owner<id::Driver<'d>>>,
    pub pointer: *const u8,
    pub packet_errors: usize,
    pub failure: Option<Failure>,
}

impl<'d> handler::Handler<0, RecvBuffer<'d, false>, Shared<'d>, id::Driver<'d>> for Capture<'d> {
    type Connection = ();
    type Create = create::Unit;
    type Events = set::Set<
        set::Off,
        set::Off,
        set::Off,
        set::Off,
        set::Off,
        set::On,
        ingress::On,
        packet::On,
        endpoint::Off,
        closing::Off,
        close::Off,
        writable::Off,
        udp::On<4>,
    >;
}

impl<'d> udp::Sink<0, RecvBuffer<'d, false>, Shared<'d>, id::Driver<'d>> for Capture<'d> {
    fn udp(
        mut self: Pin<&mut Self>,
        source: SocketAddr,
        bytes: &[u8],
    ) -> ControlFlow<Option<Vec<u8>>> {
        assert_eq!(source, self.peer);
        assert!(self.pending.is_none(), "classification precedes settlement");
        assert_eq!(self.settled, self.classified.div_ceil(2));
        assert_eq!(bytes, REQUESTS[self.classified]);
        self.classified += 1;
        match bytes {
            b"M" => ControlFlow::Break(None),
            [0] => ControlFlow::Continue(()),
            _ => {
                let reply = vec![bytes[0]; self.size];
                self.pointer = reply.as_ptr();
                ControlFlow::Break(Some(reply))
            }
        }
    }
}

impl<'d> socket::Sink<0, RecvBuffer<'d, false>, Shared<'d>, id::Driver<'d>> for Capture<'d> {
    fn socket<'scope>(event: socket::Event<'scope, Self, 0, id::Driver<'d>>) {
        let (mut this, event) = event.into_parts();
        match event {
            net::Event::Accepted(accepted) => {
                assert!(this.pending.replace(accepted.owner()).is_none());
                assert_eq!(accepted.destination(), this.peer);
                assert_eq!(accepted.bytes(), this.size);
                assert!(!accepted.shape().is_segmented());
            }
            net::Event::Transferred(transfer) => {
                assert!(this.pending == Some(transfer.from()));
                this.pending = Some(transfer.to());
            }
            net::Event::Settled(done) => {
                assert!(this.pending.take() == Some(done.owner()));
                assert_eq!(done.destination(), this.peer);
                assert_eq!(
                    done.payload().as_ptr(),
                    this.pointer,
                    "original reply allocation"
                );
                assert_eq!(done.payload(), vec![b'A' + this.settled as u8; this.size]);
                assert!(matches!(done.outcome(), net::Outcome::Sent(_)));
                this.settled += 1;
            }
            net::Event::Rejected { error, .. } => {
                assert!(
                    matches!(error, RejectCause::Resident { required, available }
                    if required.get() == this.size && available == 2400)
                );
                assert!(!this.rejected);
                this.rejected = true;
            }
            net::Event::Receive { error, .. } => {
                this.failure.get_or_insert(Failure::Receive(error));
            }
            net::Event::Transmit(error) => {
                this.failure
                    .get_or_insert(Failure::Transmit(error.into_parts().1));
            }
        }
    }
}

impl<'d> ingress::Sink<0, RecvBuffer<'d, false>, Shared<'d>, id::Driver<'d>> for Capture<'d> {
    fn ingress<'scope, 'driver>(event: ingress::Event<'scope, 'driver, Self>) {
        let (mut this, packet) = event.into_parts();
        assert_eq!(packet.source(), this.peer);
        assert_eq!(packet.bytes(), 1);
        assert_eq!(this.classified, 4);
        this.ingress += 1;
    }
}

impl<'d> packet::Sink<0, RecvBuffer<'d, false>, Shared<'d>, id::Driver<'d>> for Capture<'d> {
    fn packet(event: packet::Event<'_, Self>) {
        let (mut this, source, _, len) = event.into_parts();
        assert_eq!(source, this.peer);
        assert_eq!(len, 1);
        this.packet_errors += 1;
    }
}

impl<'d> cert::Sink<0, id::Driver<'d>> for Capture<'d> {
    fn certificate(event: cert::Event<'_, '_, Self, 0, id::Driver<'d>>) {
        let (mut this, _, _, error) = event.into_parts();
        this.failure
            .get_or_insert(Failure::Certificate(error.clone()));
    }
}
