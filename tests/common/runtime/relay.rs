use dope::core::driver::schedule::clock;
use dope::runtime::control;
use dope_quic::{
    conn::id,
    errors,
    proto::datagram::Sealed,
    runtime::{
        endpoint::{self, owner},
        mux::handler::{
            self,
            hook::{
                life::{create, ready},
                set,
                wire::{cert, ingress, packet, socket},
            },
            net,
        },
        routing,
    },
    stream,
};
use dope_test::harness::peer::{self as native, udp::Socket};
use o3::buffer::resident;
use o3::buffer::write::Sink as _;
use std::net::SocketAddr;

#[derive(Debug)]
pub enum ReadError<E> {
    Socket(native::Error),
    Empty,
    Prefix {
        actual: usize,
        required: std::num::NonZero<usize>,
    },
    Ticket,
    Packet(E),
}

#[derive(Debug)]
pub enum SendError {
    Socket(native::Error),
    Allocation(o3::alloc::Error),
    Partial { actual: usize, expected: usize },
}

pub fn receive<E>(
    socket: &Socket<'_>,
    ticket: Option<dope_quic::security::relay::Ticket>,
    mut apply: impl FnMut(&mut [u8]) -> Result<(), E>,
) -> Result<usize, ReadError<E>> {
    let mut buffer = [0; 2048];
    let mut count = 0;
    loop {
        let len = match socket.recv(&mut buffer) {
            Ok(len) => len,
            Err(native::Error::Syscall(error)) if error.is_block() => return Ok(count),
            Err(error) => return Err(ReadError::Socket(error)),
        };
        let mut packet = &mut buffer[..len];
        if let Some(ticket) = ticket {
            let Some((prefix, inner)) = packet.split_at_mut_checked(ticket.as_bytes().len()) else {
                return Err(ReadError::Prefix {
                    actual: len,
                    required: std::num::NonZero::<usize>::MIN
                        .saturating_add(ticket.as_bytes().len() - 1),
                });
            };
            if prefix != ticket.as_bytes() {
                return Err(ReadError::Ticket);
            }
            packet = inner;
        }
        if packet.is_empty() {
            return Err(ReadError::Empty);
        }
        apply(packet).map_err(ReadError::Packet)?;
        count += 1;
    }
}

pub fn send(
    socket: &Socket<'_>,
    remote: SocketAddr,
    ticket: Option<dope_quic::security::relay::Ticket>,
    packet: &[u8],
) -> Result<(), SendError> {
    let wire = frame(ticket, packet).map_err(SendError::Allocation)?;
    let actual = socket.send_to(&wire, remote).map_err(SendError::Socket)?;
    if actual != wire.len() {
        return Err(SendError::Partial {
            actual,
            expected: wire.len(),
        });
    }
    Ok(())
}

pub fn frame(
    ticket: Option<dope_quic::security::relay::Ticket>,
    packet: &[u8],
) -> Result<Vec<u8>, o3::alloc::Error> {
    let mut wire = Vec::new();
    wire.write_slices([
        ticket.as_ref().map_or(&[][..], |ticket| ticket.as_bytes()),
        packet,
    ])?;
    Ok(wire)
}

pub fn negotiation() -> [u8; 32] {
    let mut packet = [0; 32];
    packet[0] = 0x80;
    packet[1..5].copy_from_slice(&0xfafa_fafa_u32.to_be_bytes());
    packet[5] = 8;
    packet[14] = 8;
    packet
}

#[derive(Default)]
pub struct Events<'d> {
    pub created: Vec<id::Handle<id::Driver<'d>, 0>>,
    pub established: Vec<id::Handle<id::Driver<'d>, 0>>,
    pub received: Vec<errors::Receive>,
    pub diagnostics: usize,
    pub certificates: usize,
    pub accepted: Vec<(usize, net::Shape)>,
    pub settled: Vec<(usize, usize)>,
    pub socket_failures: usize,
}

impl<'d> handler::Handler<0, stream::RecvBuffer<'d>, resident::Shared<'d>, id::Driver<'d>>
    for Events<'d>
{
    type Connection = ();
    type Create = create::Call;
    type Events = set::Set<
        set::On,
        set::Off,
        set::Off,
        set::Off,
        set::Off,
        set::On,
        ingress::Off,
        packet::On,
    >;
}

impl<'d> create::Sink<0, stream::RecvBuffer<'d>, resident::Shared<'d>, id::Driver<'d>>
    for Events<'d>
{
    fn create(
        event: create::Event<
            '_,
            Self,
            0,
            stream::RecvBuffer<'d>,
            resident::Shared<'d>,
            id::Driver<'d>,
        >,
    ) {
        let (this, _, handle) = event.into_parts();
        this.get_mut().created.push(handle);
    }
}

impl<'d> ready::Sink<0, stream::RecvBuffer<'d>, resident::Shared<'d>, id::Driver<'d>>
    for Events<'d>
{
    fn established(
        event: ready::Event<
            '_,
            Self,
            0,
            stream::RecvBuffer<'d>,
            resident::Shared<'d>,
            id::Driver<'d>,
        >,
    ) {
        let (this, _, handle, _) = event.into();
        this.get_mut().established.push(handle);
    }
}

impl<'d> packet::Sink<0, stream::RecvBuffer<'d>, resident::Shared<'d>, id::Driver<'d>>
    for Events<'d>
{
    fn packet(event: packet::Event<'_, Self>) {
        let (this, _, failure, _) = event.into_parts();
        let this = this.get_mut();
        match failure {
            packet::Failure::Receive(error) => this.received.push(error),
            packet::Failure::Diagnostic(_) => this.diagnostics += 1,
        }
    }
}

impl<'d> cert::Sink<0, id::Driver<'d>> for Events<'d> {
    fn certificate(event: cert::Event<'_, '_, Self, 0, id::Driver<'d>>) {
        let (this, _, _, _) = event.into_parts();
        this.get_mut().certificates += 1;
    }
}

impl<'d> socket::Sink<0, stream::RecvBuffer<'d>, resident::Shared<'d>, id::Driver<'d>>
    for Events<'d>
{
    fn socket<'scope>(event: socket::Event<'scope, Self, 0, id::Driver<'d>>) {
        let (this, event) = event.into_parts();
        let this = this.get_mut();
        match event {
            net::Event::Accepted(accepted) => {
                this.accepted.push((accepted.bytes(), accepted.shape()))
            }
            net::Event::Settled(settled) if matches!(settled.outcome(), net::Outcome::Sent(_)) => {
                this.settled
                    .push((settled.payload().as_ptr() as usize, settled.payload().len()));
            }
            net::Event::Transferred(_) => {}
            _ => this.socket_failures += 1,
        }
    }
}

pub type Server<'d, 'tls, 'pool> = endpoint::Endpoint<
    'd,
    0,
    Events<'d>,
    stream::RecvBuffer<'d>,
    endpoint::Idle,
    owner::Pooled<'tls, routing::SingleRoute>,
    Sealed<'pool>,
>;

pub type Control<'step, 'd, 'tls, 'pool> = endpoint::control::Control<
    'step,
    'd,
    0,
    Events<'d>,
    stream::RecvBuffer<'d>,
    endpoint::Idle,
    owner::Pooled<'tls, routing::SingleRoute>,
    Sealed<'pool>,
>;

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
#[coordinate]
pub struct App<'d, 'tls, 'pool, F>
where
    F: for<'step, 'clock> FnMut(
        &mut Control<'step, 'd, 'tls, 'pool>,
        clock::Clock<'clock, 'd>,
    ) -> control::Flow,
{
    #[pin]
    #[manifold(control)]
    pub server: Server<'d, 'tls, 'pool>,
    #[dispatcher(state)]
    pub poll: F,
    #[dispatcher(schedule)]
    pub timeout: clock::Deadline<'d>,
}

impl<'d, 'tls, 'pool, F> App<'d, 'tls, 'pool, F>
where
    F: for<'step, 'clock> FnMut(
        &mut Control<'step, 'd, 'tls, 'pool>,
        clock::Clock<'clock, 'd>,
    ) -> control::Flow,
{
    fn coordinate(mut this: app_coord::Fields<'_, 'd, 'tls, 'pool, F>) -> control::Flow {
        (this.poll)(&mut this.server, this.step.clock())
    }
}
