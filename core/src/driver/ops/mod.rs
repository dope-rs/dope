pub mod access;
pub mod poll;
pub(crate) mod reactors;
pub(in crate::driver) mod retirements;

use std::{io, marker, mem, net};

use o3::permit;

use crate::{
    backend::{self, bound, fixed},
    driver::{
        self, flight,
        route::{self, kind},
        settings,
    },
    io::{
        fd::handles,
        socket::{self, establishment, option},
    },
    platform::reactor,
};

mod supported;
#[doc(hidden)]
pub use supported::Supported;

pub struct OutboundReservation<'d, const ID: u8> {
    lease: permit::Lease<OutboundReturn<'d>>,
    _brand: marker::PhantomData<fn(&'d ()) -> &'d ()>,
    _route: marker::PhantomData<route::KeyTag<ID>>,
    _thread: o3::ThreadBound,
}

struct OutboundReturn<'d> {
    driver: driver::Reference<'d>,
}

impl<'d> OutboundReturn<'d> {
    fn release(&self, key: driver::OutboundKey) -> Option<driver::RetiredSlots<'d>> {
        self.driver.outbound().release_outbound_owner(key)
    }
}

impl permit::Return for OutboundReturn<'_> {
    type Item = driver::OutboundKey;

    fn return_item(&self, key: Self::Item) {
        if let Some(slots) = self.release(key) {
            self.driver.maintenance().defer_outbound_slots(slots);
        }
    }
}

const _: () = {
    assert!(
        mem::size_of::<OutboundReservation<'static, 0>>()
            == mem::size_of::<(driver::Reference<'static>, driver::OutboundKey)>()
    );
    assert!(
        mem::align_of::<OutboundReservation<'static, 0>>()
            == mem::align_of::<(driver::Reference<'static>, driver::OutboundKey)>()
    );
};

impl<'d, const ID: u8> OutboundReservation<'d, ID> {
    pub(crate) fn new(driver: driver::Reference<'d>, key: driver::OutboundKey) -> Self {
        use o3::ThreadBound;
        Self {
            lease: permit::Lease::new(OutboundReturn { driver }, key),
            _brand: marker::PhantomData,
            _route: marker::PhantomData,
            _thread: ThreadBound::NEW,
        }
    }

    /// Binds one local pool target to its physical descriptor slot.
    ///
    /// A target from another route cannot acquire this reservation:
    ///
    /// ```compile_fail
    /// use dope_core::driver::{ops::OutboundReservation, route::{KeyTag, Target}};
    ///
    /// fn cross_route<'d>(
    ///     reservation: &OutboundReservation<'d, 1>,
    ///     target: Target<'d, KeyTag<2>>,
    /// ) {
    ///     let _ = reservation.bind(target);
    /// }
    /// ```
    pub fn bind(
        &self,
        target: route::Target<'d, route::KeyTag<ID>>,
    ) -> Option<route::Bound<'d, route::KeyTag<ID>, handles::SocketSlot<'d>>> {
        use handles::SocketSlot;
        let return_outbound = self.lease.sink();
        let key = *self.lease.item();
        let slot = return_outbound
            .driver
            .files()
            .acquire_outbound_descriptor(key, target.slot())?;
        let slot = SocketSlot::from_outbound_slot(slot, return_outbound.driver)?;
        Some(target.bind(slot))
    }

    #[doc(hidden)]
    pub fn physical_index(&self, local: route::SlotIndex) -> Option<u32> {
        self.lease
            .sink()
            .driver
            .files()
            .outbound_physical_index(*self.lease.item(), local)
    }

    pub(crate) fn retire(self) -> Option<driver::RetiredSlots<'d>> {
        let (return_outbound, key) = self.lease.into_parts();
        return_outbound.release(key)
    }
}

pub trait Bootstrap<'d>: Buffers + Supported {
    fn bind_listener_slot(
        &mut self,
        addr: net::SocketAddr,
        backlog: i32,
        config: &socket::ListenerConfig,
    ) -> io::Result<(handles::Descriptor<'d>, net::SocketAddr)>;
    fn bind_datagram_slot(
        &mut self,
        addr: net::SocketAddr,
    ) -> io::Result<(handles::DatagramDescriptor<'d>, net::SocketAddr)> {
        if self.buffer_len() < settings::Receive::MIN_DATAGRAM_BUFFER_LEN as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope: receive buffer slot cannot hold a datagram envelope",
            ));
        }
        self.bind_datagram_slot_raw(addr)
            .map(|(descriptor, addr)| (handles::DatagramDescriptor::validated(descriptor), addr))
    }

    #[doc(hidden)]
    fn bind_datagram_slot_raw(
        &mut self,
        addr: net::SocketAddr,
    ) -> io::Result<(handles::Descriptor<'d>, net::SocketAddr)>;
}

pub trait Buffers {
    fn buffer_len(&self) -> usize;
    fn buffer_count(&self) -> usize;
    fn accept_capacity(&self) -> usize;
    fn outbound_capacity(&self) -> usize;
}

/// Driver control operations whose routed resources retain their typed owner.
///
/// Raw tokens are not accepted by the safe submission boundary:
///
/// ```compile_fail
/// use dope_core::{
///     driver::{Context, flight, ops::Control, route::{KeyTag, Token}},
///     io::{fd::handles::SocketSlot, socket::StreamSpec},
/// };
///
/// fn submit<'d>(
///     driver: &mut Context<'_, 'd>,
///     flights: &flight::Slots<'d, KeyTag<1>>,
///     slot: SocketSlot<'d>,
///     socket: StreamSpec,
///     token: Token,
/// ) {
///     let _ = Control::submit_socket(driver, flights, slot, socket, token);
/// }
/// ```
pub trait Control<'d> {
    fn reserve_route(&mut self, id: u8) -> bool;
    fn release_route(&mut self, id: u8);
    fn submit_socket<Tag: route::Tag>(
        &mut self,
        flights: &flight::Slots<'d, Tag>,
        request: route::Bound<'d, Tag, handles::SocketSlot<'d>>,
        socket: socket::StreamSpec,
    ) -> Result<handles::CreatingSocket<'d>, driver::SubmitError>;
    fn submit_tuning<Tag: route::Tag>(
        &mut self,
        request: route::Bound<'d, Tag, handles::Descriptor<'d>>,
        options: option::StreamOptions,
    ) -> Result<option::Tuning<'d>, handles::Descriptor<'d>>;
    fn cancel_tuning(
        &mut self,
        pending: establishment::TuningPending<'d>,
    ) -> Result<
        establishment::TuningPending<'d, establishment::Cancelled>,
        (establishment::TuningPending<'d>, driver::SubmitError),
    >;
    fn cancel_connection(
        &mut self,
        pending: establishment::ConnectionPending<'d>,
    ) -> Result<
        establishment::ConnectionPending<'d, establishment::Cancelled>,
        (establishment::ConnectionPending<'d>, driver::SubmitError),
    >;
}

pub trait Files<'d> {
    fn close(&mut self, fd: impl Into<handles::Descriptor<'d>>);
    fn reserve_outbound<const ID: u8>(
        &mut self,
        count: u32,
    ) -> io::Result<OutboundReservation<'d, ID>>;

    #[doc(hidden)]
    fn retire_outbound<const ID: u8>(&mut self, reservation: OutboundReservation<'d, ID>);
}

/// Submits driver-branded operations.
///
/// Cancellation cannot be redirected with an unbranded raw token:
///
/// ```compile_fail
/// use dope_core::driver::{Context, ops::Submit, route::Token};
///
/// fn cancel_raw<'d>(driver: &mut Context<'_, 'd>, target: Token) {
///     let _ = Submit::cancel(driver, target);
/// }
/// ```
pub trait Submit<'d> {
    /// Submits a provided-buffer stream receive. The backend copies the fixed
    /// descriptor slot and retains no borrow from this call.
    #[doc(hidden)]
    fn submit_recv<Tag: route::Tag>(
        &mut self,
        slots: &flight::Slots<'d, Tag>,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError>;

    /// Submits a provided-buffer datagram receive. The backend copies the fixed
    /// descriptor slot and retains no borrow from this call.
    #[doc(hidden)]
    fn submit_recv_datagram<Tag: route::Tag>(
        &mut self,
        slots: &flight::Slots<'d, Tag>,
        fd: &handles::DatagramDescriptor<'d>,
        identity: route::Operation<'d, Tag>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError>;

    /// Submits a multishot accept. The backend copies the fixed listener slot
    /// and retains no borrow from this call.
    #[doc(hidden)]
    fn submit_accept_multishot<Tag: route::Tag>(
        &mut self,
        slots: &flight::Slots<'d, Tag>,
        listener: &handles::Descriptor<'d>,
        identity: route::Operation<'d, Tag>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError>;

    fn cancel<Tag: route::Tag>(
        &mut self,
        flight: &mut flight::Flight<'d>,
        target: route::Operation<'d, Tag>,
    ) -> Result<(), driver::SubmitError>;
}

impl Buffers for driver::Context<'_, '_> {
    fn buffer_len(&self) -> usize {
        self.driver_ref().receive().layout().buffer_len()
    }

    fn buffer_count(&self) -> usize {
        self.driver_ref().receive().layout().entries() as usize
    }

    fn accept_capacity(&self) -> usize {
        self.accept_slot_capacity()
    }

    fn outbound_capacity(&self) -> usize {
        self.outbound_slot_capacity()
    }
}

impl<'d> Control<'d> for driver::Context<'_, 'd>
where
    backend::Backend: backend::Socket,
{
    fn reserve_route(&mut self, id: u8) -> bool {
        self.backend().routes.reserve(id)
    }

    fn release_route(&mut self, id: u8) {
        self.backend().routes.release(id);
    }

    fn submit_socket<Tag: route::Tag>(
        &mut self,
        flights: &flight::Slots<'d, Tag>,
        request: route::Bound<'d, Tag, handles::SocketSlot<'d>>,
        socket: socket::StreamSpec,
    ) -> Result<handles::CreatingSocket<'d>, driver::SubmitError> {
        let (target, slot) = request.into_parts();
        backend::Socket::submit_socket(self.backend(), flights, target, slot, socket)
    }

    fn submit_tuning<Tag: route::Tag>(
        &mut self,
        request: route::Bound<'d, Tag, handles::Descriptor<'d>>,
        options: option::StreamOptions,
    ) -> Result<option::Tuning<'d>, handles::Descriptor<'d>> {
        let (target, fd) = request.into_parts();
        backend::Socket::submit_tuning(self.backend(), target, fd, options)
    }

    fn cancel_tuning(
        &mut self,
        mut pending: establishment::TuningPending<'d>,
    ) -> Result<
        establishment::TuningPending<'d, establishment::Cancelled>,
        (establishment::TuningPending<'d>, driver::SubmitError),
    > {
        let target = establishment::CancelTarget::Tuning(&mut pending.fd);
        match backend::Socket::cancel_establishment(self.backend(), target) {
            Ok(()) => Ok(establishment::TuningPending {
                fd: pending.fd,
                target: pending.target,
                state: marker::PhantomData,
            }),
            Err(error) => Err((pending, error)),
        }
    }

    fn cancel_connection(
        &mut self,
        mut pending: establishment::ConnectionPending<'d>,
    ) -> Result<
        establishment::ConnectionPending<'d, establishment::Cancelled>,
        (establishment::ConnectionPending<'d>, driver::SubmitError),
    > {
        let target = match pending.flight.as_mut() {
            Some(flight) => establishment::CancelTarget::Connect(flight),
            None => establishment::CancelTarget::Tuning(&mut pending.fd),
        };
        match backend::Socket::cancel_establishment(self.backend(), target) {
            Ok(()) => Ok(establishment::ConnectionPending {
                fd: pending.fd,
                target: pending.target,
                flight: pending.flight,
                state: marker::PhantomData,
            }),
            Err(error) => Err((pending, error)),
        }
    }
}

impl<'d> Files<'d> for driver::Context<'_, 'd> {
    fn close(&mut self, fd: impl Into<handles::Descriptor<'d>>) {
        fd.into().close(self.backend());
    }

    fn reserve_outbound<const ID: u8>(
        &mut self,
        count: u32,
    ) -> io::Result<OutboundReservation<'d, ID>> {
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope: outbound reservation must be non-empty",
            ));
        }
        let driver = self.driver_ref();
        let files = driver.files();
        if ID == route::FRAMEWORK {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope: framework route cannot own outbound slots",
            ));
        }
        if files.has_outbound_route::<ID>() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "dope: route already owns outbound slots",
            ));
        }
        let slots = fixed::Lifecycle::alloc_slots(self.backend(), count, driver)
            .map_err(|error| files.map_fixed_allocation_error(error))?;
        let key = match files.track_outbound_slots::<ID>(slots) {
            Ok(key) => key,
            Err(slots) => {
                fixed::Lifecycle::release_slots(self.backend(), slots);
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "dope: outbound slots could not be tracked",
                ));
            }
        };
        Ok(OutboundReservation::new(driver, key))
    }

    fn retire_outbound<const ID: u8>(&mut self, reservation: OutboundReservation<'d, ID>) {
        if let Some(retired) = reservation.retire() {
            let slots = self.driver_ref().outbound().take_retired_slots(retired);
            fixed::Lifecycle::release_slots(self.backend(), slots);
        }
    }
}

impl<'d> Submit<'d> for driver::Context<'_, 'd>
where
    backend::Backend: reactor::Source,
{
    fn submit_recv<Tag: route::Tag>(
        &mut self,
        slots: &flight::Slots<'d, Tag>,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError> {
        let raw = backend::Copied::recv(fd);
        submit_nonretaining(self, slots, raw, target.operation(kind::RECV))
    }

    fn submit_recv_datagram<Tag: route::Tag>(
        &mut self,
        slots: &flight::Slots<'d, Tag>,
        fd: &handles::DatagramDescriptor<'d>,
        identity: route::Operation<'d, Tag>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError> {
        let raw = backend::Copied::recv_datagram(fd);
        submit_nonretaining(self, slots, raw, identity.with_kind(kind::RECV))
    }

    fn submit_accept_multishot<Tag: route::Tag>(
        &mut self,
        slots: &flight::Slots<'d, Tag>,
        listener: &handles::Descriptor<'d>,
        identity: route::Operation<'d, Tag>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError> {
        let raw = backend::Copied::accept_multishot(listener);
        submit_nonretaining(self, slots, raw, identity.with_kind(kind::ACCEPT))
    }

    fn cancel<Tag: route::Tag>(
        &mut self,
        flight: &mut flight::Flight<'d>,
        target: route::Operation<'d, Tag>,
    ) -> Result<(), driver::SubmitError> {
        if !flight.matches(target.into_token()) {
            return Err(driver::SubmitError);
        }
        let mut queue = reactor::Source::queue(self.backend());
        reactor::Queue::cancel(&mut queue, flight)
    }
}

fn submit_nonretaining<'d, Tag: route::Tag>(
    context: &mut driver::Context<'_, 'd>,
    slots: &flight::Slots<'d, Tag>,
    copied: backend::Copied,
    target: route::Operation<'d, Tag>,
) -> Result<flight::Flight<'d>, driver::SubmitError>
where
    backend::Backend: reactor::Source,
{
    let submission =
        bound::Bound::reserve(copied.into_raw(), target, slots).ok_or(driver::SubmitError)?;
    let mut queue = reactor::Source::queue(context.backend());
    reactor::Queue::submit(&mut queue, submission)
}
