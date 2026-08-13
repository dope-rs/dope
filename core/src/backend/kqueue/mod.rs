mod affinities;
mod descriptor;
mod engine;
pub(in crate::backend::kqueue) mod errno;
mod ffi;
mod file;
pub(crate) mod ops;

use std::{
    convert, io, mem, net,
    os::{fd, fd::AsRawFd as _, unix::fs::OpenOptionsExt as _},
    path, process, time,
};

use o3::buffer::{self, pool::state};

use self::engine::{event, lifecycle, read, receive, runtime, submit, write};
use crate::{
    backend::{self, bound, fixed},
    driver::{
        self, flight,
        lifecycle::routing,
        route::{self, kind},
        settings,
    },
    io::{
        datagram,
        fd::handles,
        recv,
        socket::{self, establishment, option},
    },
    platform::{self, reactor},
};

const WAKE_IDENT: libc::uintptr_t = usize::MAX;

pub struct Kqueue {
    file: file::Lane,
    retries: write::State,
    pub(in crate::backend::kqueue) pending: event::Queue,
    pub(in crate::backend::kqueue) files: lifecycle::Files,
    pub(in crate::backend::kqueue) poll: event::Poll,
    reads: read::Registry,
    pub(in crate::backend::kqueue) recv: receive::Pool,
    pub(crate) routes: routing::Routes,
}

impl Kqueue {
    fn apply_stream_options(
        &self,
        descriptor: &handles::Descriptor<'_>,
        options: option::StreamOptions,
    ) -> Result<(), ()> {
        let Some(fd) = self.files.borrow(descriptor.slot()) else {
            return Err(());
        };
        for option in options.iter() {
            descriptor::Options::new(fd).set(option.level(), option.name(), option.value())?;
        }
        Ok(())
    }

    fn close_slot(&mut self, slot: handles::FixedSlot) {
        self.pending.cancel_create(slot);
        if let Some(fd) = self.files.take_index(slot.raw() as usize) {
            lifecycle::Control::new(self).close_owned(fd);
        }
    }

    pub(crate) fn push_pending(&mut self, completion: event::Completion) {
        let wake = self.pending.is_empty();
        assert!(
            self.pending.push_back(completion),
            "dope-kqueue: pending completion capacity exhausted"
        );
        if wake {
            self.poll.changes.wake();
        }
    }

    fn push_file_completion(
        &mut self,
        budget: &mut event::Budget<'_, '_, event::CompletionLane>,
    ) -> bool {
        let Some(completion) = self.file.pop() else {
            return false;
        };
        let Some(_credit) = budget.take() else {
            process::abort();
        };
        assert!(
            self.pending.push_back(completion),
            "dope-kqueue: pending completion capacity exhausted"
        );
        true
    }

    pub(in crate::backend::kqueue) fn wait(
        &mut self,
        timeout: Option<time::Duration>,
        changes: &mut event::Budget<'_, '_, event::ChangeLane>,
        completions: &mut event::Budget<'_, '_, event::CompletionLane>,
    ) -> io::Result<()> {
        const EVENT_CAPACITY: usize = 64;

        self.poll.check()?;
        let mut events: [mem::MaybeUninit<libc::kevent>; EVENT_CAPACITY] =
            [const { mem::MaybeUninit::uninit() }; EVENT_CAPACITY];
        let available = completions.remaining();
        let ready_files = self.file.ready_count().min(available);
        let starts_file = self.file.starts_batch();
        let balanced_file_share = if starts_file {
            available.div_ceil(2)
        } else {
            available / 2
        };
        let file_reserve = ready_files.min(balanced_file_share);
        let event_limit = available.saturating_sub(file_reserve).min(events.len());
        let change_limit = changes.remaining();
        let admitted_changes = change_limit != 0 && !self.poll.changes.is_empty();
        if event_limit == 0 && !admitted_changes && ready_files == 0 {
            return Ok(());
        }
        let timeout =
            if ready_files != 0 || event_limit == 0 || self.poll.changes.len() > change_limit {
                Some(time::Duration::ZERO)
            } else {
                timeout
            };
        let mut ready = self
            .poll
            .wait(&mut events[..event_limit], timeout, changes)?;
        let mut file_remaining = file_reserve;
        let mut file_turn = starts_file;
        while file_remaining != 0 || !ready.is_empty() {
            let mut progressed = false;
            if file_turn && file_remaining != 0 && self.push_file_completion(completions) {
                file_remaining -= 1;
                progressed = true;
            }
            if !progressed && !ready.is_empty() {
                let Some(kernel_event) = ready.next() else {
                    process::abort();
                };
                if kernel_event.filter() == libc::EVFILT_USER {
                    file_turn = !file_turn;
                    continue;
                }
                let Some(credit) = completions.take() else {
                    process::abort();
                };
                event::Dispatch::new(self).dispatch_event(kernel_event, credit);
                progressed = true;
            }
            if !progressed && file_remaining != 0 {
                if self.push_file_completion(completions) {
                    file_remaining -= 1;
                } else {
                    file_remaining = 0;
                }
            }
            file_turn = !file_turn;
        }
        while completions.remaining() != 0 {
            if !self.push_file_completion(completions) {
                break;
            }
        }
        Ok(())
    }

    pub(crate) fn has_pending_resume(&self) -> bool {
        self.reads.has_pending_resume()
    }
}

impl fixed::Lifecycle for Kqueue {
    fn alloc_slots<'d>(
        &mut self,
        len: u32,
        driver: driver::Reference<'d>,
    ) -> io::Result<fixed::Reservation<'d>> {
        self.files.alloc_slots(len, driver)
    }

    fn release_slots<'d>(&mut self, slots: fixed::Reservation<'d>) {
        self.files.retire(slots);
    }

    fn close<'d>(
        &mut self,
        close: driver::Close<'d>,
        driver: driver::Reference<'d>,
        _phase: fixed::Phase,
    ) {
        let slot = close.into_slot();
        self.close_slot(slot);
        if let Some(retired) = driver.outbound().complete_outbound_close(slot) {
            let slots = driver.outbound().take_retired_slots(retired);
            self.files.retire(slots);
        }
    }

    fn retire<'d>(&mut self, slot: fixed::Slot<'d>, _phase: fixed::Phase) {
        self.close_slot(slot.fixed());
        self.files.retire_slot(slot);
    }
}

impl fixed::Finalize for Kqueue {
    fn settle<'q, 'd>(&mut self, _drain: flight::Drain<'q, 'd>) -> io::Result<()> {
        Ok(())
    }
}

impl reactor::Source for Kqueue {
    type Queue<'a> = submit::Queue<'a>;

    fn queue(&mut self) -> Self::Queue<'_> {
        submit::Queue::new(self)
    }
}

impl platform::Buffer for Kqueue {
    type Token = buffer::Lease<state::Initialized>;

    fn release(&mut self, buffer: Self::Token) {
        drop(buffer);
    }
}

impl platform::Datagram for Kqueue {
    type Gso = convert::Infallible;

    fn project(buffer: &recv::Lease<'_>) -> datagram::Projection {
        let raw = buffer.as_slice();
        let namelen = socket::Addr::STORAGE_CAPACITY;
        if raw.len() < namelen {
            return datagram::Projection::Rejected { truncated: false };
        }
        let (name, bytes) = raw.split_at(namelen);
        let Some(source) = socket::Addr::parse_msg_name(name) else {
            return datagram::Projection::Rejected { truncated: false };
        };
        let payload = buffer.span_of(bytes);
        datagram::Projection::Packet { source, payload }
    }
}

impl platform::Filesystem for Kqueue {
    fn open_directory(path: &path::Path) -> io::Result<fd::OwnedFd> {
        use std::fs;

        let directory = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
            .open(path)?;
        Ok(directory.into())
    }
}

impl platform::Quiesce for Kqueue {
    fn all(&mut self, drain: flight::Drain<'_, '_>) -> io::Result<()> {
        self.file.shutdown();
        self.poll.clear();
        {
            let Kqueue { poll, reads, .. } = self;
            reads.quiesce(&mut poll.changes);
        }
        write::Retry::new(self).quiesce_write_retries();
        self.poll.revoke()?;
        while let Some(completion) = self.pending.pop_for_reclaim() {
            lifecycle::Control::new(self).reclaim(completion, &drain);
        }
        while let Some(completion) = self.file.pop() {
            lifecycle::Control::new(self).reclaim(completion, &drain);
        }
        Ok(())
    }
}

impl platform::Runtime for Kqueue {
    fn build(config: &settings::Config) -> io::Result<Self> {
        let kq = runtime::Setup::open()?.into_fd();
        let file_slots = config.file_slots();
        let queues = config.queue_layout();
        let slots = file_slots.table_capacity().get();
        let recv = receive::Pool::try_new(config.receive())?;
        let hash_capacity = || {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "dope: fixed-file capacity exceeds hash-table layout",
            )
        };
        let reads = read::Registry::try_with_capacity(slots)?;
        let retries = write::State::try_with_capacity(slots)?;
        let pending = event::Queue::try_with_capacity(queues, slots)?;
        let change_capacity = slots.checked_mul(2).ok_or_else(hash_capacity)?;
        let file = file::Lane::new(queues.submissions() as usize, kq.as_raw_fd())?;
        let poll = event::Poll::new(kq, change_capacity)?;
        Ok(Self {
            file,
            retries,
            pending,
            files: lifecycle::Files::new(file_slots)?,
            poll,
            reads,
            recv,
            routes: routing::Routes::new(),
        })
    }

    fn register_shutdown(&mut self, source: driver::Source<'_>) -> io::Result<()> {
        let poll = &mut self.poll;
        poll.register_shutdown(source.into_fd())
    }
}

impl platform::Affinity for Kqueue {
    type Cpus = affinities::Cpus;
    type Binding = affinities::Cpu;
}

impl backend::Socket for Kqueue {
    const MAX_IOVECS: usize = libc::IOV_MAX as usize;
    const KEEP_ALIVE_IDLE: libc::c_int = libc::TCP_KEEPALIVE;
    const KEEP_ALIVE_INTERVAL: libc::c_int = libc::TCP_KEEPINTVL;
    const KEEP_ALIVE_RETRIES: libc::c_int = libc::TCP_KEEPCNT;

    fn encode_v4(addr: net::SocketAddrV4) -> libc::sockaddr_in {
        use libc::{AF_INET, in_addr, sockaddr_in};
        sockaddr_in {
            sin_len: mem::size_of::<libc::sockaddr_in>() as u8,
            sin_family: AF_INET as _,
            sin_port: addr.port().to_be(),
            sin_addr: in_addr {
                s_addr: u32::from_ne_bytes(addr.ip().octets()),
            },
            sin_zero: [0; 8],
        }
    }

    fn encode_v6(addr: net::SocketAddrV6) -> libc::sockaddr_in6 {
        use libc::{AF_INET6, in6_addr, sockaddr_in6};
        sockaddr_in6 {
            sin6_len: mem::size_of::<libc::sockaddr_in6>() as u8,
            sin6_family: AF_INET6 as _,
            sin6_port: addr.port().to_be(),
            sin6_flowinfo: addr.flowinfo(),
            sin6_addr: in6_addr {
                s6_addr: addr.ip().octets(),
            },
            sin6_scope_id: addr.scope_id(),
        }
    }

    fn encode_unix(bytes: &[u8]) -> io::Result<(libc::sockaddr_un, libc::socklen_t)> {
        use libc::sockaddr_un;
        if bytes.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty path"));
        }
        let mut encoded = sockaddr_un {
            sun_len: 0,
            sun_family: libc::AF_UNIX as _,
            sun_path: [0; 104],
        };
        let Some(max) = encoded.sun_path.len().checked_sub(1) else {
            use std::process::abort;
            abort();
        };
        if bytes.len() > max {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "path too long"));
        }
        for (index, byte) in bytes.iter().enumerate() {
            encoded.sun_path[index] = *byte as libc::c_char;
        }
        let len = (mem::size_of::<libc::sa_family_t>() + bytes.len() + 1) as libc::socklen_t;
        encoded.sun_len = len as u8;
        Ok((encoded, len))
    }

    fn submit_tuning<'d, Tag: route::Tag>(
        &mut self,
        _target: route::Target<'d, Tag>,
        descriptor: handles::Descriptor<'d>,
        options: option::StreamOptions,
    ) -> Result<option::Tuning<'d>, handles::Descriptor<'d>> {
        if self.apply_stream_options(&descriptor, options).is_err() {
            return Err(descriptor);
        }
        Ok(option::Tuning::Applied(descriptor))
    }

    fn submit_tuned_connect<'owner, 'd: 'owner, Tag: route::Tag>(
        &mut self,
        flights: &flight::Slots<'d, Tag>,
        options: option::StreamOptions,
        connect: backend::raw::RetainedConnect<'owner, 'd, Tag>,
    ) -> Result<establishment::ConnectionPending<'d>, handles::Descriptor<'d>> {
        let (fd, submission, target) = connect.into_parts();
        if self.apply_stream_options(&fd, options).is_err() {
            return Err(fd);
        }
        let Some(submission) =
            bound::Bound::reserve_retained(submission, target.operation(kind::CONNECT), flights)
        else {
            return Err(fd);
        };
        match reactor::Queue::submit(&mut <Self as reactor::Source>::queue(self), submission) {
            Ok(flight) => Ok(establishment::ConnectionPending::connect(fd, flight)),
            Err(_) => Err(fd),
        }
    }

    fn cancel_establishment(
        &mut self,
        target: establishment::CancelTarget<'_, '_>,
    ) -> Result<(), driver::SubmitError> {
        match target {
            establishment::CancelTarget::Tuning(_) => Ok(()),
            establishment::CancelTarget::Connect(flight) => {
                reactor::Queue::cancel(&mut <Self as reactor::Source>::queue(self), flight)
            }
        }
    }
}

impl backend::WakeFactory for Kqueue {
    fn open_blocking_wake_ends() -> io::Result<(fd::OwnedFd, fd::OwnedFd)> {
        runtime::Pipe::open().map(runtime::Pipe::into_ends)
    }

    fn open_nonblocking_wake_ends() -> io::Result<(fd::OwnedFd, fd::OwnedFd)> {
        runtime::Pipe::open_nonblocking().map(runtime::Pipe::into_ends)
    }
}

impl platform::EntropySource for Kqueue {
    fn acquire() -> io::Result<[u64; 2]> {
        ffi::Entropy::acquire().map(ffi::Entropy::into_words)
    }
}

pub(crate) mod submission;
