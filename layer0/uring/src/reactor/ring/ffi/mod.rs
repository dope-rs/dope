//! Linux v6.12 io_uring UAPI subset used by this crate.
//! Values and layouts are owned here instead of delegated to bindings.
//! Constant assertions pin every kernel-visible ABI layout.

mod fd;
mod map;

use std::{marker, mem};

pub(crate) use fd::{Binding, Fd};
pub(crate) use map::Map;

use crate::reactor::register::Probe;

pub(crate) const NOP: u8 = 0;
pub(crate) const FSYNC: u8 = 3;
pub(crate) const POLL_ADD: u8 = 6;
pub(crate) const SENDMSG: u8 = 9;
pub(crate) const RECVMSG: u8 = 10;
pub(crate) const ACCEPT: u8 = 13;
pub(crate) const ASYNC_CANCEL: u8 = 14;
pub(crate) const CONNECT: u8 = 16;
pub(crate) const CLOSE: u8 = 19;
pub(crate) const WRITE: u8 = 23;
pub(crate) const SEND: u8 = 26;
pub(crate) const RECV: u8 = 27;
pub(crate) const OPENAT2: u8 = 28;
pub(crate) const SHUTDOWN: u8 = 34;
pub(crate) const RENAMEAT: u8 = 35;
pub(crate) const UNLINKAT: u8 = 36;
pub(crate) const SOCKET: u8 = 45;
pub(crate) const URING_CMD: u8 = 46;

pub(crate) const SETUP_CQSIZE: u32 = 1 << 3;
pub(crate) const SETUP_SUBMIT_ALL: u32 = 1 << 7;
pub(crate) const SETUP_TASKRUN_FLAG: u32 = 1 << 9;
pub(crate) const SETUP_SINGLE_ISSUER: u32 = 1 << 12;
pub(crate) const SETUP_DEFER_TASKRUN: u32 = 1 << 13;
pub(crate) const SETUP_NO_MMAP: u32 = 1 << 14;
pub(crate) const SETUP_NO_SQARRAY: u32 = 1 << 16;

pub(crate) const FEATURE_NODROP: u32 = 1 << 1;
pub(crate) const FEATURE_EXT_ARG: u32 = 1 << 8;
pub(crate) const FEATURE_CQE_SKIP: u32 = 1 << 11;

pub(crate) const ENTER_GETEVENTS: u32 = 1 << 0;
pub(crate) const ENTER_EXT_ARG: u32 = 1 << 3;
pub(crate) const ENTER_REGISTERED: u32 = 1 << 4;

pub(crate) const SQ_CQ_OVERFLOW: u32 = 1 << 1;
pub(crate) const SQ_TASKRUN: u32 = 1 << 2;

pub(crate) const RESOURCE_REGISTER_SPARSE: u32 = 1 << 0;
pub(crate) const RECEIVE_MULTISHOT: u16 = 1 << 1;
pub(crate) const ACCEPT_MULTISHOT: u16 = 1 << 0;
pub(crate) const FILE_INDEX_ALLOC: u32 = u32::MAX;
pub(crate) const SOCKET_SETSOCKOPT: u32 = 3;

pub(crate) const REGISTER_FILES_UPDATE: Register<FilesUpdate, Exact<1>, 1, 6> = Register::new();
pub(crate) const REGISTER_PROBE: Register<Probe, Exact<0>, 256, 8> = Register::new();
pub(crate) const REGISTER_FILES: Register<
    Resources,
    Exact<0>,
    { mem::size_of::<Resources>() as u32 },
    13,
> = Register::new();
pub(crate) const REGISTER: Register<Resource, Exact<1>, 1, 20> = Register::new();
pub(crate) const UNREGISTER: Register<Resource, Exact<1>, 1, 21> = Register::new();
pub(crate) const REGISTER_BUFFER: Register<Buffers, Exact<0>, 1, 22> = Register::new();
pub(crate) const UNREGISTER_BUFFER: Register<Buffers, Exact<0>, 1, 23> = Register::new();
pub(crate) const REGISTER_SYNC_CANCEL: Register<SyncCancel, Count, 1, 24> = Register::new();
pub(crate) const REGISTER_FILE_RANGE: Register<FileRange, Exact<0>, 0, 25> = Register::new();

pub(crate) enum Count {}

pub(crate) enum Exact<const VALUE: libc::c_int> {}

#[repr(transparent)]
pub(crate) struct Register<P, O, const COUNT: u32, const OPCODE: u32 = 0>(
    marker::PhantomData<fn(&mut P) -> O>,
);

impl<P, O, const COUNT: u32, const OPCODE: u32> Register<P, O, COUNT, OPCODE> {
    const fn new() -> Self {
        Self(marker::PhantomData)
    }

    pub(crate) const fn opcode(self) -> u32 {
        OPCODE
    }
}

#[derive(Clone, Copy, Default)]
#[repr(C, align(8))]
pub(crate) struct Timespec {
    pub(crate) seconds: i64,
    pub(crate) nanoseconds: i64,
}

#[derive(Clone, Copy, Default)]
#[repr(C, align(8))]
pub(crate) struct SqOffsets {
    pub(crate) head: u32,
    pub(crate) tail: u32,
    pub(crate) ring_mask: u32,
    pub(crate) ring_entries: u32,
    pub(crate) flags: u32,
    pub(crate) dropped: u32,
    pub(crate) array: u32,
    pub(crate) reserved: u32,
    pub(crate) user_address: u64,
}

#[derive(Clone, Copy, Default)]
#[repr(C, align(8))]
pub(crate) struct CqOffsets {
    pub(crate) head: u32,
    pub(crate) tail: u32,
    pub(crate) ring_mask: u32,
    pub(crate) ring_entries: u32,
    pub(crate) overflow: u32,
    pub(crate) cqes: u32,
    pub(crate) flags: u32,
    pub(crate) reserved: u32,
    pub(crate) user_address: u64,
}

#[derive(Clone, Copy, Default)]
#[repr(C, align(8))]
pub(crate) struct Parameters {
    pub(crate) sq_entries: u32,
    pub(crate) cq_entries: u32,
    pub(crate) flags: u32,
    pub(crate) sq_thread_cpu: u32,
    pub(crate) sq_thread_idle: u32,
    pub(crate) features: u32,
    pub(crate) work_queue_fd: u32,
    pub(crate) reserved: [u32; 3],
    pub(crate) sq_off: SqOffsets,
    pub(crate) cq_off: CqOffsets,
}

#[repr(C, align(8))]
pub(crate) struct FilesUpdate {
    pub(crate) offset: u32,
    pub(crate) reserved: u32,
    pub(crate) files: u64,
}

#[repr(C, align(8))]
pub(crate) struct Resources {
    pub(crate) count: u32,
    pub(crate) flags: u32,
    pub(crate) reserved: u64,
    pub(crate) data: u64,
    pub(crate) tags: u64,
}

#[repr(C, align(8))]
pub(crate) struct Resource {
    pub(crate) offset: u32,
    pub(crate) reserved: u32,
    pub(crate) data: u64,
}

#[repr(C, align(8))]
pub(crate) struct Buffers {
    pub(crate) ring_address: u64,
    pub(crate) ring_entries: u32,
    pub(crate) group: u16,
    pub(crate) flags: u16,
    pub(crate) reserved: [u64; 3],
}

#[repr(C, align(8))]
pub(crate) struct SyncCancel {
    pub(crate) address: u64,
    pub(crate) file: i32,
    pub(crate) flags: u32,
    pub(crate) timeout: Timespec,
    pub(crate) opcode: u8,
    pub(crate) reserved: [u8; 7],
    pub(crate) reserved2: [u64; 3],
}

#[repr(C, align(8))]
pub(crate) struct FileRange {
    pub(crate) offset: u32,
    pub(crate) len: u32,
    pub(crate) reserved: u64,
}

const _: () = {
    assert!(mem::size_of::<Register<(), Count, 0>>() == 0);

    assert!(mem::size_of::<Timespec>() == 16);
    assert!(mem::align_of::<Timespec>() == 8);
    assert!(mem::offset_of!(Timespec, seconds) == 0);
    assert!(mem::offset_of!(Timespec, nanoseconds) == 8);

    assert!(mem::size_of::<SqOffsets>() == 40);
    assert!(mem::align_of::<SqOffsets>() == 8);
    assert!(mem::offset_of!(SqOffsets, head) == 0);
    assert!(mem::offset_of!(SqOffsets, tail) == 4);
    assert!(mem::offset_of!(SqOffsets, ring_mask) == 8);
    assert!(mem::offset_of!(SqOffsets, ring_entries) == 12);
    assert!(mem::offset_of!(SqOffsets, flags) == 16);
    assert!(mem::offset_of!(SqOffsets, dropped) == 20);
    assert!(mem::offset_of!(SqOffsets, array) == 24);
    assert!(mem::offset_of!(SqOffsets, reserved) == 28);
    assert!(mem::offset_of!(SqOffsets, user_address) == 32);

    assert!(mem::size_of::<CqOffsets>() == 40);
    assert!(mem::align_of::<CqOffsets>() == 8);
    assert!(mem::offset_of!(CqOffsets, head) == 0);
    assert!(mem::offset_of!(CqOffsets, tail) == 4);
    assert!(mem::offset_of!(CqOffsets, ring_mask) == 8);
    assert!(mem::offset_of!(CqOffsets, ring_entries) == 12);
    assert!(mem::offset_of!(CqOffsets, overflow) == 16);
    assert!(mem::offset_of!(CqOffsets, cqes) == 20);
    assert!(mem::offset_of!(CqOffsets, flags) == 24);
    assert!(mem::offset_of!(CqOffsets, reserved) == 28);
    assert!(mem::offset_of!(CqOffsets, user_address) == 32);

    assert!(mem::size_of::<Parameters>() == 120);
    assert!(mem::align_of::<Parameters>() == 8);
    assert!(mem::offset_of!(Parameters, sq_entries) == 0);
    assert!(mem::offset_of!(Parameters, cq_entries) == 4);
    assert!(mem::offset_of!(Parameters, flags) == 8);
    assert!(mem::offset_of!(Parameters, sq_thread_cpu) == 12);
    assert!(mem::offset_of!(Parameters, sq_thread_idle) == 16);
    assert!(mem::offset_of!(Parameters, features) == 20);
    assert!(mem::offset_of!(Parameters, work_queue_fd) == 24);
    assert!(mem::offset_of!(Parameters, reserved) == 28);
    assert!(mem::offset_of!(Parameters, sq_off) == 40);
    assert!(mem::offset_of!(Parameters, cq_off) == 80);

    assert!(mem::size_of::<FilesUpdate>() == 16);
    assert!(mem::align_of::<FilesUpdate>() == 8);
    assert!(mem::offset_of!(FilesUpdate, offset) == 0);
    assert!(mem::offset_of!(FilesUpdate, reserved) == 4);
    assert!(mem::offset_of!(FilesUpdate, files) == 8);

    assert!(mem::size_of::<Resources>() == 32);
    assert!(mem::align_of::<Resources>() == 8);
    assert!(mem::offset_of!(Resources, count) == 0);
    assert!(mem::offset_of!(Resources, flags) == 4);
    assert!(mem::offset_of!(Resources, reserved) == 8);
    assert!(mem::offset_of!(Resources, data) == 16);
    assert!(mem::offset_of!(Resources, tags) == 24);

    assert!(mem::size_of::<Resource>() == 16);
    assert!(mem::align_of::<Resource>() == 8);
    assert!(mem::offset_of!(Resource, offset) == 0);
    assert!(mem::offset_of!(Resource, reserved) == 4);
    assert!(mem::offset_of!(Resource, data) == 8);

    assert!(mem::size_of::<Buffers>() == 40);
    assert!(mem::align_of::<Buffers>() == 8);
    assert!(mem::offset_of!(Buffers, ring_address) == 0);
    assert!(mem::offset_of!(Buffers, ring_entries) == 8);
    assert!(mem::offset_of!(Buffers, group) == 12);
    assert!(mem::offset_of!(Buffers, flags) == 14);
    assert!(mem::offset_of!(Buffers, reserved) == 16);

    assert!(mem::size_of::<SyncCancel>() == 64);
    assert!(mem::align_of::<SyncCancel>() == 8);
    assert!(mem::offset_of!(SyncCancel, address) == 0);
    assert!(mem::offset_of!(SyncCancel, file) == 8);
    assert!(mem::offset_of!(SyncCancel, flags) == 12);
    assert!(mem::offset_of!(SyncCancel, timeout) == 16);
    assert!(mem::offset_of!(SyncCancel, opcode) == 32);
    assert!(mem::offset_of!(SyncCancel, reserved) == 33);
    assert!(mem::offset_of!(SyncCancel, reserved2) == 40);

    assert!(mem::size_of::<FileRange>() == 16);
    assert!(mem::align_of::<FileRange>() == 8);
    assert!(mem::offset_of!(FileRange, offset) == 0);
    assert!(mem::offset_of!(FileRange, len) == 4);
    assert!(mem::offset_of!(FileRange, reserved) == 8);
};
