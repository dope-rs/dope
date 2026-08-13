pub(crate) mod bound;
mod captured;
pub(crate) mod fixed;
pub(crate) mod operations;
pub(crate) mod raw;

use std::{io, mem, net, os::fd};

pub(crate) use captured::Captured;

use crate::{
    driver::{
        self, flight,
        route::{self, kind},
    },
    io::{
        fd::handles,
        socket::{self, establishment, option},
    },
    platform::reactor,
};

/// A native request whose borrowed inputs were copied while lowering.
#[repr(transparent)]
pub(crate) struct Copied(RawSubmission);

const _: () = {
    assert!(mem::size_of::<Copied>() == mem::size_of::<RawSubmission>());
    assert!(mem::align_of::<Copied>() == mem::align_of::<RawSubmission>());
};

impl Copied {
    pub(crate) fn recv(fd: &handles::Descriptor<'_>) -> Self {
        let operation = operations::Recv {
            slot: fd.slot_ref(),
        };
        Self(<RawSubmission as raw::Lower>::recv(raw::Prepared::new(
            operation,
        )))
    }

    pub(crate) fn recv_datagram(fd: &handles::DatagramDescriptor<'_>) -> Self {
        let operation = operations::RecvMsgMulti {
            slot: fd.slot_ref(),
        };
        Self(<RawSubmission as raw::Lower>::recv_message(
            raw::Prepared::new(operation),
        ))
    }

    pub(crate) fn accept_multishot(listener: &handles::Descriptor<'_>) -> Self {
        let operation = operations::AcceptMultishot {
            listener: listener.slot_ref(),
        };
        Self(<RawSubmission as raw::Lower>::accept_multishot(
            raw::Prepared::new(operation),
        ))
    }

    pub(crate) fn into_raw(self) -> RawSubmission {
        self.0
    }
}

pub(crate) trait WakeFactory {
    fn open_blocking_wake_ends() -> io::Result<(fd::OwnedFd, fd::OwnedFd)>;
    fn open_nonblocking_wake_ends() -> io::Result<(fd::OwnedFd, fd::OwnedFd)>;
}

pub(crate) trait Socket: reactor::Source {
    const MAX_IOVECS: usize;
    const KEEP_ALIVE_IDLE: libc::c_int;
    const KEEP_ALIVE_INTERVAL: libc::c_int;
    const KEEP_ALIVE_RETRIES: libc::c_int;

    fn encode_v4(addr: net::SocketAddrV4) -> libc::sockaddr_in;
    fn encode_v6(addr: net::SocketAddrV6) -> libc::sockaddr_in6;
    fn encode_unix(bytes: &[u8]) -> io::Result<(libc::sockaddr_un, libc::socklen_t)>;
    fn submit_socket<'d, Tag: route::Tag>(
        &mut self,
        flights: &flight::Slots<'d, Tag>,
        target: route::Target<'d, Tag>,
        slot: handles::SocketSlot<'d>,
        socket: socket::StreamSpec,
    ) -> Result<handles::CreatingSocket<'d>, driver::SubmitError> {
        let raw = raw::Prepared::lower(slot.slot_ref(), socket);
        let Some(submission) = bound::Bound::reserve(raw, target.operation(kind::SOCKET), flights)
        else {
            return Err(driver::SubmitError);
        };
        let flight = reactor::Queue::submit(&mut reactor::Source::queue(self), submission)?;
        Ok(slot.into_creating(flight))
    }
    fn submit_tuning<'d, Tag: route::Tag>(
        &mut self,
        target: route::Target<'d, Tag>,
        fd: handles::Descriptor<'d>,
        options: option::StreamOptions,
    ) -> Result<option::Tuning<'d>, handles::Descriptor<'d>>;
    fn submit_tuned_connect<'owner, 'd: 'owner, Tag: route::Tag>(
        &mut self,
        flights: &flight::Slots<'d, Tag>,
        options: option::StreamOptions,
        connect: raw::RetainedConnect<'owner, 'd, Tag>,
    ) -> Result<establishment::ConnectionPending<'d>, handles::Descriptor<'d>>;
    fn cancel_establishment(
        &mut self,
        target: establishment::CancelTarget<'_, '_>,
    ) -> Result<(), driver::SubmitError>;
}

cfg_select! {
    target_os = "linux" => {
        mod uring;
        pub(crate) use uring::Uring;
        pub(crate) type Backend = Uring;
        pub(crate) type RawSubmission = uring::submission::RawSubmission;
    }
    target_os = "macos" => {
        mod kqueue;
        pub(crate) use kqueue::Kqueue;
        pub(crate) type Backend = Kqueue;
        pub(crate) type RawSubmission = kqueue::submission::RawSubmission;
    }
    _ => {
        compile_error!("dope-core supports only Linux (io_uring) and macOS (kqueue)");
    }
}
