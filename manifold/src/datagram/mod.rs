use std::{io, net, num, ops, pin, time};

use dope_core::{
    driver::{
        self, lifecycle,
        ops::Buffers as _,
        route::{self, table},
        schedule,
    },
    io::datagram,
};
use o3::{buffer::pool, cell::region};

mod binding;
mod drive;
pub mod packet;
mod receive;
mod sealed;
mod send;

pub(in crate::datagram) use sealed::Submission;

pub const GSO_LIMITS: Option<datagram::GsoLimits> = datagram::GSO_LIMITS;

/// An owned datagram view over a suffix of its allocation.
/// Socket submission reads `storage[start..]` while reclamation retains the
/// allocation, preserving headroom without compaction.
#[derive(Debug)]
pub struct OwnedSuffix {
    pub(in crate::datagram) storage: Vec<u8>,
    start: usize,
}

impl OwnedSuffix {
    pub fn new(storage: Vec<u8>, start: usize) -> Result<Self, Vec<u8>> {
        if start > storage.len() {
            return Err(storage);
        }
        Ok(Self { storage, start })
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.storage[self.start..]
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.storage[self.start..]
    }

    pub fn len(&self) -> usize {
        self.storage.len() - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.start == self.storage.len()
    }

    pub fn into_storage(self) -> Vec<u8> {
        self.storage
    }
}

impl AsRef<[u8]> for OwnedSuffix {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsMut<[u8]> for OwnedSuffix {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl ops::Deref for OwnedSuffix {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl ops::DerefMut for OwnedSuffix {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

/// Validated bounds for a sending datagram endpoint.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    pending_sends: table::Capacity,
    retained_send_bytes: send::ResidentBytes,
    in_flight_sends: table::Capacity,
    retained_receive_bytes: usize,
}

impl Config {
    /// Validates progress-capable pending and in-flight bounds.
    pub fn new(
        pending_sends: usize,
        retained_send_bytes: usize,
        in_flight_sends: usize,
    ) -> io::Result<Self> {
        let pending_sends = Self::capacity(
            pending_sends,
            "dope: pending datagram capacity must be nonzero and fit token slots",
        )?;
        let in_flight_sends = Self::capacity(
            in_flight_sends,
            "dope: in-flight datagram capacity must be nonzero and fit token slots",
        )?;
        Ok(Self {
            pending_sends,
            retained_send_bytes: send::ResidentBytes::new(retained_send_bytes),
            in_flight_sends,
            retained_receive_bytes: 0,
        })
    }

    /// Bounds driver receive storage retained beyond its dispatch turn.
    pub fn with_retained_receive_bytes(mut self, retained_receive_bytes: usize) -> Self {
        self.retained_receive_bytes = retained_receive_bytes;
        self
    }

    fn capacity(value: usize, message: &'static str) -> io::Result<table::Capacity> {
        table::Capacity::new(value)
            .filter(|capacity| capacity.get() != 0)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, message))
    }

    fn into_parts(self) -> (usize, send::ResidentBytes, table::Capacity, usize) {
        (
            self.pending_sends.get(),
            self.retained_send_bytes,
            self.in_flight_sends,
            self.retained_receive_bytes,
        )
    }
}

pub trait Handler<'d, const ID: u8> {
    fn packet<'turn>(
        &mut self,
        addr: net::SocketAddr,
        packet: packet::Packet<'turn, 'd>,
        sock: pin::Pin<&'turn mut Socket<'d, ID>>,
        now: time::Instant,
    );

    /// Reclaims an owned payload after the kernel has released its pointer.
    /// This runs for every terminal send result, including during shutdown.
    fn recycle(&mut self, payload: Vec<u8>) {
        drop(payload);
    }

    /// Queues application work immediately before the socket is driven.
    fn pre_park<'turn>(
        &mut self,
        sock: pin::Pin<&mut Socket<'d, ID>>,
        now: time::Instant,
        work: schedule::Application<'turn, 'd>,
    ) {
        let _ = (sock, now, work);
    }

    /// Contributes application work or a deadline in addition to socket I/O.
    fn progress(&self, region: &region::Token<'d>) -> schedule::Progress<'d> {
        let _ = region;
        schedule::Progress::Quiescent
    }

    /// Stops handler-owned application state after the socket has stopped
    /// accepting new work.
    fn shutdown(&mut self) {}
}

/// Safe lifecycle owner for one datagram socket and handler.
#[pin_project::pin_project]
pub struct Endpoint<'d, const ID: u8, H> {
    #[pin]
    socket: Socket<'d, ID>,
    handler: H,
}

impl<'d, const ID: u8, H> Endpoint<'d, ID, H> {
    pub fn bind(
        addr: net::SocketAddr,
        handler: H,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Self> {
        Ok(Self {
            socket: Socket::bind(addr, driver)?,
            handler,
        })
    }

    pub fn bind_with_config(
        addr: net::SocketAddr,
        handler: H,
        config: Config,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Self> {
        Ok(Self {
            socket: Socket::bind_with_config(addr, config, driver)?,
            handler,
        })
    }

    pub fn handler(&self) -> &H {
        &self.handler
    }

    pub fn handler_mut(self: pin::Pin<&mut Self>) -> &mut H {
        self.project().handler
    }

    pub fn local_addr(&self) -> net::SocketAddr {
        self.socket.binding.local_addr()
    }
}

const RECV_ARM_TAG: route::SlotIndex = route::SlotIndex::ZERO;

type RecvTag<const ID: u8> = route::KeyTag<ID, { route::RECV }>;
type SendTag<const ID: u8> = route::KeyTag<ID, { route::SEND }>;

/// A dispatcher-owned datagram socket whose address remains stable while I/O
/// is retained by the kernel.
///
/// ```compile_fail,E0277
/// fn require_unpin<T: Unpin>() {}
/// require_unpin::<dope_manifold::datagram::Socket<'static, 0>>();
/// ```
#[pin_project::pin_project(PinnedDrop, !Unpin)]
pub struct Socket<'d, const ID: u8> {
    receive: receive::Receive<'d, ID>,
    sender: send::Sender<'d, ID>,
    binding: binding::Binding<'d, ID>,
}

#[pin_project::pinned_drop]
impl<const ID: u8> PinnedDrop for Socket<'_, ID> {
    fn drop(self: pin::Pin<&mut Self>) {
        self.project().binding.assert_droppable();
    }
}

impl<'d, const ID: u8> Socket<'d, ID> {
    fn install(self: pin::Pin<&mut Self>, install: &mut lifecycle::Install<'_, 'd>) {
        self.project().binding.install(install);
    }

    fn dispatch<H: Handler<'d, ID>>(
        mut self: pin::Pin<&mut Self>,
        event: crate::DriverEvent<'d>,
        handler: &mut H,
        now: time::Instant,
    ) {
        use dope_core::io::event::Kind;
        match event.into_kind() {
            Kind::Recv(completion) => {
                let (target, more, event) = completion.into_parts();
                let packet = self
                    .as_mut()
                    .project()
                    .receive
                    .complete(target, more, event);
                if let Some((source, payload)) = packet {
                    handler.packet(source, packet::Packet::new(payload), self, now);
                }
            }
            Kind::Send(completion) => {
                let (target, _) = completion.into_parts();
                if let Some(payload) = self.as_mut().project().sender.complete(target) {
                    handler.recycle(payload);
                }
            }
            _ => {}
        }
    }

    fn bind(addr: net::SocketAddr, driver: &mut driver::Context<'_, 'd>) -> io::Result<Self> {
        let config = Config::new(4096, 16 << 20, 4096)?;
        Self::bind_with_config(addr, config, driver)
    }

    fn bind_with_config(
        addr: net::SocketAddr,
        config: Config,
        driver: &mut driver::Context<'_, 'd>,
    ) -> io::Result<Self> {
        let (pending, retained, in_flight, retained_receive_bytes) = config.into_parts();
        let retention = packet::Retention::new(retained_receive_bytes, driver.region_token_ref());
        let recv_flights = driver.flight_slots::<RecvTag<ID>>(1)?;
        let send_flights = driver.flight_slots::<SendTag<ID>>(in_flight.get())?;
        let receive_slot_bytes = send::ResidentBytes::new(driver.buffer_len());
        let sender = send::Sender::try_new(
            pending,
            retained,
            in_flight,
            receive_slot_bytes,
            send_flights,
        )?;
        let (binding, receive_target) = binding::Binding::bind(addr, driver)?;
        let receive = receive::Receive::new(
            receive_target,
            binding.descriptor(),
            recv_flights,
            retention,
        );
        Ok(Self {
            receive,
            sender,
            binding,
        })
    }

    pub fn queue_to(
        self: pin::Pin<&mut Self>,
        payload: Vec<u8>,
        addr: net::SocketAddr,
    ) -> Result<(), Vec<u8>> {
        self.try_enqueue(payload, addr, None, send::Payload::Owned)
    }

    /// Queues a suffix without moving it to the front of its allocation.
    pub fn queue_suffix(
        self: pin::Pin<&mut Self>,
        payload: OwnedSuffix,
        addr: net::SocketAddr,
    ) -> Result<(), OwnedSuffix> {
        self.try_enqueue(payload, addr, None, send::Payload::OwnedSuffix)
    }

    pub fn queue_buffer(
        self: pin::Pin<&mut Self>,
        payload: pool::Cursor,
        addr: net::SocketAddr,
    ) -> Result<(), pool::Cursor> {
        self.try_enqueue(payload, addr, None, send::Payload::Buffer)
    }

    pub fn queue_packet<'turn>(
        self: pin::Pin<&mut Self>,
        packet: packet::Packet<'turn, 'd>,
        addr: net::SocketAddr,
    ) -> Result<(), packet::Packet<'turn, 'd>> {
        let view = packet.into_view();
        self.try_enqueue(view, addr, None, send::Payload::Packet)
            .map_err(packet::Packet::new)
    }

    pub fn retain_packet<'turn>(
        self: pin::Pin<&Self>,
        packet: packet::Packet<'turn, 'd>,
    ) -> Result<packet::Retained<'d>, packet::Packet<'turn, 'd>> {
        self.get_ref().receive.retain(packet)
    }

    pub fn packet_retainer<'turn>(self: pin::Pin<&'turn Self>) -> packet::Retainer<'turn, 'd> {
        self.get_ref().receive.retainer()
    }

    pub fn queue_retained_packet(
        self: pin::Pin<&mut Self>,
        packet: packet::Retained<'d>,
        addr: net::SocketAddr,
    ) -> Result<(), packet::Retained<'d>> {
        let packet = packet.into_inner();
        self.try_enqueue(packet, addr, None, send::Payload::RetainedPacket)
            .map_err(packet::Retained::from_inner)
    }

    pub fn queue_gso(
        self: pin::Pin<&mut Self>,
        payload: Vec<u8>,
        segment_size: num::NonZeroU16,
        addr: net::SocketAddr,
    ) -> Result<(), Vec<u8>> {
        let Some(capability) = datagram::GsoCapability::acquire() else {
            return Err(payload);
        };
        let limits = capability.limits();
        let bytes = payload.len();
        let segment_bytes = usize::from(segment_size.get());
        let segments = bytes.div_ceil(segment_bytes);
        if segments < 2 || segments > limits.max_segments || bytes > limits.max_bytes {
            return Err(payload);
        }
        self.try_enqueue(
            payload,
            addr,
            Some(capability.segment(segment_size)),
            send::Payload::Owned,
        )
    }

    fn try_enqueue<P, F>(
        self: pin::Pin<&mut Self>,
        payload: P,
        addr: net::SocketAddr,
        segment: Option<datagram::GsoSegment>,
        into_payload: F,
    ) -> Result<(), P>
    where
        P: send::ResidentPayload,
        F: FnOnce(send::Bounded<P>) -> send::Payload<'d>,
    {
        self.project()
            .sender
            .try_enqueue(payload, addr, segment, into_payload)
    }

    fn progress(&self, region: &region::Token<'d>) -> schedule::Progress<'d> {
        self.sender
            .progress(region)
            .reduce(self.receive.progress(region))
    }

    /// Stops accepting new work. Queued sends are retired incrementally under
    /// the shared maintenance budget; submitted sends and the receive arm remain
    /// owned until terminal completion.
    fn shutdown<H: Handler<'d, ID>>(
        mut self: pin::Pin<&mut Self>,
        work: schedule::Maintenance<'_, 'd>,
        handler: &mut H,
        driver: &mut driver::Context<'_, 'd>,
    ) {
        let this = self.as_mut().project();
        this.sender.stop();
        this.receive.stop(driver);
        handler.shutdown();
        this.sender.drain(work, |payload| handler.recycle(payload));
    }

    fn finish(self: pin::Pin<&mut Self>, context: &mut lifecycle::Finalize<'_, 'd>) {
        let this = self.project();
        assert!(
            this.sender.is_empty(),
            "datagram owner reached finish before sends quiesced"
        );
        this.receive.finish(context);
        this.binding.finish(context);
    }
}
