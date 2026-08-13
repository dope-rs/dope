use std::{io, mem, os::fd, pin, ptr};

use io_uring::{self, cqueue, opcode, squeue};

struct Handles {
    submitter: io_uring::Submitter<'static>,
    submission: squeue::SubmissionQueue<'static>,
    completion: cqueue::CompletionQueue<'static>,
}

/// Stable owner for the handles split from one pinned ring.
/// Handle fields drop before the pinned ring that backs them.
pub(super) struct Owner {
    handles: Handles,
    _io: pin::Pin<Box<io_uring::IoUring>>,
}

impl Owner {
    pub(super) fn new(io: io_uring::IoUring) -> Self {
        let mut io = Box::pin(io);
        let handles = unsafe { Self::split(&mut io) };
        Self { handles, _io: io }
    }

    /// # Safety
    /// The owner keeps `io` pinned and drops all returned handles first.
    unsafe fn split(io: &mut pin::Pin<Box<io_uring::IoUring>>) -> Handles {
        let ring = unsafe { pin::Pin::get_unchecked_mut(io.as_mut()) };
        let (submitter, submission, completion) = ring.split();
        Handles {
            submitter: unsafe {
                mem::transmute::<io_uring::Submitter<'_>, io_uring::Submitter<'static>>(submitter)
            },
            submission: unsafe {
                mem::transmute::<squeue::SubmissionQueue<'_>, squeue::SubmissionQueue<'static>>(
                    submission,
                )
            },
            completion: unsafe {
                mem::transmute::<cqueue::CompletionQueue<'_>, cqueue::CompletionQueue<'static>>(
                    completion,
                )
            },
        }
    }

    pub(super) fn submitter(&self) -> &io_uring::Submitter<'static> {
        &self.handles.submitter
    }

    pub(super) fn submission(&self) -> &squeue::SubmissionQueue<'static> {
        &self.handles.submission
    }

    pub(super) fn submission_mut(&mut self) -> &mut squeue::SubmissionQueue<'static> {
        &mut self.handles.submission
    }

    pub(super) fn completion_mut(&mut self) -> &mut cqueue::CompletionQueue<'static> {
        &mut self.handles.completion
    }

    pub(super) fn push(&mut self, entry: &squeue::Entry) -> Result<(), squeue::PushError> {
        unsafe { self.handles.submission.push(entry) }
    }

    pub(super) fn push_multiple(
        &mut self,
        entries: &[squeue::Entry],
    ) -> Result<(), squeue::PushError> {
        unsafe { self.handles.submission.push_multiple(entries) }
    }
}

pub(in crate::backend::uring) struct Canary<'entry> {
    entry: &'entry squeue::Entry,
    user_data: u64,
}

#[must_use = "a live multishot canary must be cancelled to its terminal completion"]
pub(in crate::backend::uring) struct MultishotCanary {
    entry: squeue::Entry,
    user_data: u64,
}

pub(in crate::backend::uring) struct DatagramPair {
    pub(super) reader: fd::OwnedFd,
    writer: fd::OwnedFd,
}

pub(in crate::backend::uring::ring) struct TcpPair {
    pub(super) listener: fd::OwnedFd,
    _client: fd::OwnedFd,
}

impl<'entry> Canary<'entry> {
    pub(super) const fn new(entry: &'entry squeue::Entry, user_data: u64) -> Self {
        Self { entry, user_data }
    }

    pub(super) fn submit(self, ring: &mut io_uring::IoUring) -> io::Result<cqueue::Entry> {
        // SAFETY: this proof object retains the entry borrow and does not return
        // until the matching terminal completion has been removed from the ring.
        unsafe { ring.submission().push(self.entry) }
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "dope: canary SQ is full"))?;

        ring.submit_and_wait(1)?;
        take_completion(ring, self.user_data)
    }
}

impl MultishotCanary {
    pub(super) const fn new(entry: squeue::Entry, user_data: u64) -> Self {
        Self { entry, user_data }
    }

    pub(super) fn submit(self, ring: &mut io_uring::IoUring) -> io::Result<(Self, cqueue::Entry)> {
        unsafe { ring.submission().push(&self.entry) }
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "dope: canary SQ is full"))?;
        ring.submit_and_wait(1)?;
        let completion = take_completion(ring, self.user_data)?;
        Ok((self, completion))
    }

    pub(super) fn next(&self, ring: &mut io_uring::IoUring) -> io::Result<cqueue::Entry> {
        ring.submit_and_wait(1)?;
        take_completion(ring, self.user_data)
    }

    pub(super) fn cancel(self, ring: &mut io_uring::IoUring) -> io::Result<cqueue::Entry> {
        let cancel = opcode::AsyncCancel::new(self.user_data)
            .build()
            .flags(squeue::Flags::SKIP_SUCCESS)
            .user_data(0);
        unsafe { ring.submission().push(&cancel) }
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "dope: canary SQ is full"))?;
        ring.submit_and_wait(1)?;
        take_completion(ring, self.user_data)
    }
}

fn take_completion(ring: &mut io_uring::IoUring, user_data: u64) -> io::Result<cqueue::Entry> {
    let mut completions = ring.completion();
    let completion = completions
        .next()
        .ok_or_else(|| io::Error::other("dope: canary completion is missing"))?;
    if completion.user_data() != user_data {
        return Err(io::Error::other("dope: unexpected canary completion"));
    }
    Ok(completion)
}

impl DatagramPair {
    pub(super) fn new() -> io::Result<Self> {
        use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};

        let reader = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))?;
        let writer = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))?;
        writer.connect(reader.local_addr()?)?;
        reader.set_nonblocking(true)?;
        writer.set_nonblocking(true)?;
        Ok(Self {
            reader: reader.into(),
            writer: writer.into(),
        })
    }

    pub(super) fn send(&self, payload: &[u8]) -> io::Result<()> {
        loop {
            let sent = unsafe {
                libc::send(
                    fd::AsRawFd::as_raw_fd(&self.writer),
                    payload.as_ptr().cast(),
                    payload.len(),
                    libc::MSG_NOSIGNAL,
                )
            };
            if sent == payload.len() as isize {
                return Ok(());
            }
            if sent >= 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "dope: incomplete multishot receive canary datagram",
                ));
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
}

impl TcpPair {
    pub(super) fn new() -> io::Result<Self> {
        let listener = tcp_socket()?;
        let mut address = libc::sockaddr_in {
            sin_family: libc::AF_INET as libc::sa_family_t,
            sin_port: 0,
            sin_addr: libc::in_addr {
                s_addr: u32::from_ne_bytes([127, 0, 0, 1]),
            },
            sin_zero: [0; 8],
        };
        // SAFETY: address is a fully initialized IPv4 socket address.
        let result = unsafe {
            libc::bind(
                fd::AsRawFd::as_raw_fd(&listener),
                ptr::from_ref(&address).cast(),
                size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: listener is a live TCP socket owned by this function.
        if unsafe { libc::listen(fd::AsRawFd::as_raw_fd(&listener), 1) } != 0 {
            return Err(io::Error::last_os_error());
        }

        let mut length = size_of::<libc::sockaddr_in>() as libc::socklen_t;
        // SAFETY: address and length name writable storage of the advertised size.
        let result = unsafe {
            libc::getsockname(
                fd::AsRawFd::as_raw_fd(&listener),
                ptr::from_mut(&mut address).cast(),
                &raw mut length,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }

        let client = tcp_socket()?;
        // SAFETY: getsockname initialized address and length for this listener.
        let result = unsafe {
            libc::connect(
                fd::AsRawFd::as_raw_fd(&client),
                ptr::from_ref(&address).cast(),
                length,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            listener,
            _client: client,
        })
    }
}

fn tcp_socket() -> io::Result<fd::OwnedFd> {
    // SAFETY: socket has no borrowed inputs and returns a fresh descriptor.
    let raw = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: socket returned a fresh owned descriptor.
    Ok(unsafe { fd::FromRawFd::from_raw_fd(raw) })
}
