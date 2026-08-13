use std::{io, os::fd, pin, ptr};

use io_uring::{cqueue, opcode, squeue, types};

use crate::{
    backend::uring::{self, opcodes, ring},
    driver::settings,
    io::socket,
};

const FIXED_SLOT: u32 = 0;
const DIRECT_SOCKET: u64 = 1;
const SET_SOCKOPT: u64 = 2;
const CLOSE_DIRECT_SOCKET: u64 = 3;
const RECV_MULTI: u64 = 4;
const RECVMSG_MULTI: u64 = 5;
const DIRECT_ACCEPT: u64 = 6;
const CLOSE_DIRECT_ACCEPT: u64 = 7;
const DIRECT_ACCEPT_MULTI: u64 = 8;
const CLOSE_DIRECT_ACCEPT_MULTI: u64 = 9;
const RECEIVE_PAYLOADS: [&[u8]; 3] = [b"a", b"b", b"c"];

pub(in crate::backend::uring) struct Admissions {
    candidate: ring::Candidate,
    file_slots: settings::FileSlots,
}

impl Admissions {
    pub(in crate::backend::uring) const fn new(
        candidate: ring::Candidate,
        file_slots: settings::FileSlots,
    ) -> Self {
        Self {
            candidate,
            file_slots,
        }
    }

    pub(in crate::backend::uring) fn admit(mut self) -> io::Result<ring::Ready> {
        probe_opcodes(&self.candidate.0.io)?;
        probe_direct_socket_and_set_sockopt(&mut self.candidate.0)?;
        probe_multishot_receive(&mut self.candidate.0)?;
        if self.file_slots.accept() != 0 {
            probe_direct_accept(&mut self.candidate.0, self.file_slots)?;
            probe_direct_accept_multi(&mut self.candidate.0, self.file_slots)?;
        }
        require_empty(&mut self.candidate.0.io)?;
        ring::RegisteredRaw::new(self.candidate.0).map(ring::Ready)
    }
}

fn probe_opcodes(ring: &io_uring::IoUring) -> io::Result<()> {
    let mut probe = io_uring::Probe::new();
    ring.submitter()
        .register_probe(&mut probe)
        .map_err(|error| context("opcode probe", error))?;
    for required in opcodes::OPCODES {
        if !probe.is_supported(required.code) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("dope: io_uring opcode {} is unavailable", required.name),
            ));
        }
    }
    Ok(())
}

fn probe_direct_socket_and_set_sockopt(raw: &mut ring::Raw) -> io::Result<()> {
    let destination = types::DestinationSlot::try_from_slot_target(FIXED_SLOT)
        .map_err(|_| io::Error::other("dope: invalid canary fixed-file slot"))?;
    let socket = opcode::Socket::new(libc::AF_INET, libc::SOCK_STREAM, 0)
        .file_index(Some(destination))
        .build()
        .user_data(DIRECT_SOCKET);
    expect_success(
        ring::Canary::new(&socket, DIRECT_SOCKET).submit(&mut raw.io)?,
        "direct socket",
    )?;

    let fixed = Occupied::new(raw, FIXED_SLOT);
    let value = pin::pin!(1_u32);
    let option = opcode::SetSockOpt::new(
        types::Fixed(FIXED_SLOT),
        libc::SOL_SOCKET as u32,
        libc::SO_KEEPALIVE as u32,
        ptr::from_ref(value.as_ref().get_ref()).cast(),
        size_of::<u32>() as u32,
    )
    .build()
    .user_data(SET_SOCKOPT);
    expect_zero(
        ring::Canary::new(&option, SET_SOCKOPT).submit(&mut fixed.raw.io)?,
        "socket setsockopt command",
    )?;
    fixed.close(CLOSE_DIRECT_SOCKET)
}

fn probe_multishot_receive(raw: &mut ring::Raw) -> io::Result<()> {
    let mut buffers = uring::ffi::CanaryRing::new(&mut raw.io)?;
    let result = probe_recv_multi(&mut buffers).and_then(|()| probe_recvmsg_multi(&mut buffers));
    let released = buffers.finish();
    result.and(released)
}

fn probe_recv_multi(buffers: &mut uring::ffi::CanaryRing<'_>) -> io::Result<()> {
    let pair = ring::DatagramPair::new()?;
    let mut fixed = CanaryFile::register(buffers, fd::AsRawFd::as_raw_fd(&pair.reader))?;
    let receive =
        opcode::RecvMulti::new(types::Fixed(FIXED_SLOT), uring::ffi::CanaryRing::GROUP_ID)
            .build()
            .user_data(RECV_MULTI);
    let result = probe_receive_shots(
        fixed.buffers(),
        &pair,
        receive,
        RECV_MULTI,
        "multishot recv",
        inspect_recv,
    );
    result.and(fixed.release())
}

fn probe_recvmsg_multi(buffers: &mut uring::ffi::CanaryRing<'_>) -> io::Result<()> {
    use uring::ffi::recvmsg::header;

    let pair = ring::DatagramPair::new()?;
    let mut fixed = CanaryFile::register(buffers, fd::AsRawFd::as_raw_fd(&pair.reader))?;
    let header = header::Header::datagram();
    let receive = opcode::RecvMsgMulti::new(
        types::Fixed(FIXED_SLOT),
        header,
        uring::ffi::CanaryRing::GROUP_ID,
    )
    .build()
    .user_data(RECVMSG_MULTI);
    let result = probe_receive_shots(
        fixed.buffers(),
        &pair,
        receive,
        RECVMSG_MULTI,
        "multishot recvmsg",
        |bytes, expected| inspect_recvmsg(bytes, expected, header),
    );
    result.and(fixed.release())
}

fn probe_receive_shots<Inspect>(
    buffers: &mut uring::ffi::CanaryRing<'_>,
    pair: &ring::DatagramPair,
    entry: squeue::Entry,
    user_data: u64,
    name: &'static str,
    inspect: Inspect,
) -> io::Result<()>
where
    Inspect: Fn(&[u8], &[u8]) -> io::Result<()>,
{
    let first = RECEIVE_PAYLOADS[0];
    pair.send(first)?;
    let (active, first) = ring::MultishotCanary::new(entry, user_data).submit(buffers.ring())?;
    let mut result = inspect_shot(buffers, first, name, RECEIVE_PAYLOADS[0], &inspect);
    for payload in &RECEIVE_PAYLOADS[1..] {
        if result.is_err() {
            break;
        }
        result = pair.send(payload).and_then(|()| {
            active
                .next(buffers.ring())
                .and_then(|shot| inspect_shot(buffers, shot, name, payload, &inspect))
        });
    }
    let cancelled = active
        .cancel(buffers.ring())
        .and_then(|terminal| expect_multishot_cancel(terminal, name));
    result.and(cancelled)
}

fn inspect_shot(
    buffers: &mut uring::ffi::CanaryRing<'_>,
    completion: cqueue::Entry,
    name: &'static str,
    expected: &[u8],
    inspect: &impl Fn(&[u8], &[u8]) -> io::Result<()>,
) -> io::Result<()> {
    let flags = completion.flags();
    let len = expect_success(completion, name)? as usize;
    let bid = cqueue::buffer_select(flags).ok_or_else(|| {
        io::Error::other(format!(
            "dope: io_uring {name} canary omitted its selected buffer"
        ))
    })?;
    let inspected = buffers.inspect(bid, len, |bytes| inspect(bytes, expected));
    if !cqueue::more(flags) {
        return Err(io::Error::other(format!(
            "dope: io_uring {name} canary terminated before its next shot"
        )));
    }
    inspected
}

fn inspect_recv(bytes: &[u8], expected: &[u8]) -> io::Result<()> {
    if bytes == expected {
        Ok(())
    } else {
        Err(io::Error::other(
            "dope: io_uring multishot recv corrupted its payload",
        ))
    }
}

fn inspect_recvmsg(bytes: &[u8], expected: &[u8], header: &libc::msghdr) -> io::Result<()> {
    let parsed = types::RecvMsgOut::parse(bytes, header).map_err(|()| {
        io::Error::other("dope: io_uring multishot recvmsg returned an invalid frame")
    })?;
    if parsed.is_payload_truncated() || parsed.is_name_data_truncated() {
        return Err(io::Error::other(
            "dope: io_uring multishot recvmsg truncated its canary frame",
        ));
    }
    let Some(source) = socket::Addr::parse_msg_name(parsed.name_data()) else {
        return Err(io::Error::other(
            "dope: io_uring multishot recvmsg returned an invalid source address",
        ));
    };
    if !source.ip().is_loopback() || parsed.payload_data() != expected {
        return Err(io::Error::other(
            "dope: io_uring multishot recvmsg corrupted its canary frame",
        ));
    }
    Ok(())
}

fn expect_multishot_cancel(completion: cqueue::Entry, name: &'static str) -> io::Result<()> {
    let flags = completion.flags();
    if completion.result() == -libc::ECANCELED
        && !cqueue::more(flags)
        && cqueue::buffer_select(flags).is_none()
    {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "dope: io_uring {name} cancellation was not terminal"
        )))
    }
}

fn probe_direct_accept(raw: &mut ring::Raw, file_slots: settings::FileSlots) -> io::Result<()> {
    let pair = ring::TcpPair::new()?;
    let listener_slot = listener_slot(file_slots)?;
    let mut listener =
        Occupied::register(raw, listener_slot, fd::AsRawFd::as_raw_fd(&pair.listener))?;
    let accept = opcode::Accept::new(
        types::Fixed(listener_slot),
        ptr::null_mut(),
        ptr::null_mut(),
    )
    .file_index(Some(types::DestinationSlot::auto_target()))
    .build()
    .user_data(DIRECT_ACCEPT);
    let result = expect_success(
        ring::Canary::new(&accept, DIRECT_ACCEPT).submit(&mut listener.raw.io)?,
        "direct accept",
    )? as u32;
    validate_accept_slot(result, file_slots.accept(), "direct accept")?;
    listener.close_slot(result, CLOSE_DIRECT_ACCEPT)?;
    listener.release()
}

fn probe_direct_accept_multi(
    raw: &mut ring::Raw,
    file_slots: settings::FileSlots,
) -> io::Result<()> {
    let pair = ring::TcpPair::new()?;
    let listener_slot = listener_slot(file_slots)?;
    let mut listener =
        Occupied::register(raw, listener_slot, fd::AsRawFd::as_raw_fd(&pair.listener))?;
    let accept = opcode::AcceptMulti::new(types::Fixed(listener_slot))
        .allocate_file_index(true)
        .build()
        .user_data(DIRECT_ACCEPT_MULTI);
    let (active, completion) =
        ring::MultishotCanary::new(accept, DIRECT_ACCEPT_MULTI).submit(&mut listener.raw.io)?;
    let more = cqueue::more(completion.flags());
    let result = expect_success(completion, "multishot direct accept")? as u32;
    if !more {
        return Err(io::Error::other(
            "dope: io_uring multishot direct accept canary terminated after its first result",
        ));
    }
    validate_accept_slot(result, file_slots.accept(), "multishot direct accept")?;
    let terminal = active.cancel(&mut listener.raw.io)?;
    expect_errno(
        terminal,
        "multishot direct accept cancellation",
        libc::ECANCELED,
    )?;
    listener.close_slot(result, CLOSE_DIRECT_ACCEPT_MULTI)?;
    listener.release()
}

fn listener_slot(file_slots: settings::FileSlots) -> io::Result<u32> {
    if file_slots.outbound() == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "dope: accept slots require one outbound slot for the listener",
        ));
    }
    Ok(file_slots.accept())
}

fn validate_accept_slot(result: u32, accept_slots: u32, name: &'static str) -> io::Result<()> {
    if result < accept_slots {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "dope: io_uring {name} escaped its registered allocation range"
        )))
    }
}

struct Occupied<'ring> {
    raw: &'ring mut ring::Raw,
    slot: u32,
    occupied: bool,
}

struct CanaryFile<'canary, 'ring> {
    buffers: &'canary mut uring::ffi::CanaryRing<'ring>,
    occupied: bool,
}

impl<'canary, 'ring> CanaryFile<'canary, 'ring> {
    fn register(
        buffers: &'canary mut uring::ffi::CanaryRing<'ring>,
        fd: fd::RawFd,
    ) -> io::Result<Self> {
        ring::Raw::update_file(buffers.ring(), FIXED_SLOT, fd)?;
        Ok(Self {
            buffers,
            occupied: true,
        })
    }

    fn buffers(&mut self) -> &mut uring::ffi::CanaryRing<'ring> {
        self.buffers
    }

    fn release(mut self) -> io::Result<()> {
        ring::Raw::update_file(self.buffers.ring(), FIXED_SLOT, -1)?;
        self.occupied = false;
        Ok(())
    }
}

impl Drop for CanaryFile<'_, '_> {
    fn drop(&mut self) {
        if self.occupied {
            let _ = ring::Raw::update_file(self.buffers.ring(), FIXED_SLOT, -1);
        }
    }
}

impl<'ring> Occupied<'ring> {
    fn new(raw: &'ring mut ring::Raw, slot: u32) -> Self {
        Self {
            raw,
            slot,
            occupied: true,
        }
    }

    fn register(raw: &'ring mut ring::Raw, slot: u32, fd: fd::RawFd) -> io::Result<Self> {
        ring::Raw::update_file(&raw.io, slot, fd)?;
        Ok(Self::new(raw, slot))
    }

    fn close(mut self, user_data: u64) -> io::Result<()> {
        let slot = self.slot;
        self.close_slot(slot, user_data)?;
        self.occupied = false;
        Ok(())
    }

    fn close_slot(&mut self, slot: u32, user_data: u64) -> io::Result<()> {
        let close = opcode::Close::new(types::Fixed(slot))
            .build()
            .user_data(user_data);
        expect_zero(
            ring::Canary::new(&close, user_data).submit(&mut self.raw.io)?,
            "direct fixed-file close",
        )
    }

    fn release(mut self) -> io::Result<()> {
        ring::Raw::update_file(&self.raw.io, self.slot, -1)?;
        self.occupied = false;
        Ok(())
    }
}

impl Drop for Occupied<'_> {
    fn drop(&mut self) {
        if self.occupied {
            let _ = ring::Raw::update_file(&self.raw.io, self.slot, -1);
        }
    }
}

fn expect_success(completion: cqueue::Entry, name: &'static str) -> io::Result<i32> {
    let result = completion.result();
    if result < 0 {
        Err(completion_failure(name, result))
    } else {
        Ok(result)
    }
}

fn expect_zero(completion: cqueue::Entry, name: &'static str) -> io::Result<()> {
    let result = expect_success(completion, name)?;
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "dope: io_uring {name} canary returned {result}"
        )))
    }
}

fn expect_errno(completion: cqueue::Entry, name: &'static str, errno: i32) -> io::Result<()> {
    let result = completion.result();
    if result == -errno && !cqueue::more(completion.flags()) {
        Ok(())
    } else {
        Err(completion_failure(name, result))
    }
}

fn completion_failure(name: &'static str, result: i32) -> io::Error {
    let errno = result
        .checked_neg()
        .filter(|errno| *errno > 0)
        .unwrap_or(libc::EIO);
    context(name, io::Error::from_raw_os_error(errno))
}

fn context(name: &'static str, error: io::Error) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("dope: io_uring {name} admission failed: {error}"),
    )
}

fn require_empty(ring: &mut io_uring::IoUring) -> io::Result<()> {
    if !ring.submission().is_empty() || !ring.completion().is_empty() {
        return Err(io::Error::other(
            "dope: io_uring admission left work in the production ring",
        ));
    }
    Ok(())
}
