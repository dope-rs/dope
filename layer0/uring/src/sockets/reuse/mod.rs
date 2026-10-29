use std::{
    mem,
    os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd},
    ptr,
};

use dope_sys::{
    io::socket::{
        Socket as _,
        route::{
            Attach, Plan,
            count::{Count, Index},
            install::{Error, Operation},
        },
    },
    kernel,
};

use crate::Uring;

mod program;

const MAP_CREATE: libc::c_uint = 0;
const MAP_UPDATE_ELEM: libc::c_uint = 2;
const PROG_LOAD: libc::c_uint = 5;
const MAP_TYPE: u32 = 20;
const PROGRAM_TYPE: u32 = 21;
const ATTACH_TYPE: u32 = 39;

#[repr(C)]
#[derive(Default)]
struct MapShape {
    map_type: u32,
    key_size: u32,
    value_size: u32,
    max_entries: u32,
    map_flags: u32,
    inner_map_fd: u32,
    numa_node: u32,
}

#[repr(C)]
#[derive(Default)]
struct MapCreate {
    shape: MapShape,
    map_name: [u8; 16],
    map_ifindex: u32,
    btf_fd: u32,
    btf_key_type_id: u32,
    btf_value_type_id: u32,
    btf_vmlinux_value_type_id: u32,
    map_extra: u64,
}

#[repr(C)]
struct MapUpdate {
    map_fd: u32,
    padding: u32,
    key: u64,
    value: u64,
    flags: u64,
}

#[repr(C)]
struct ProgSource {
    prog_type: u32,
    insn_cnt: u32,
    insns: u64,
    license: u64,
}

#[repr(C)]
struct ProgLog {
    log_level: u32,
    log_size: u32,
    log_buf: u64,
}

#[repr(C)]
struct ProgMetadata {
    kern_version: u32,
    prog_flags: u32,
    prog_name: [u8; 16],
    prog_ifindex: u32,
    expected_attach_type: u32,
}

#[repr(C)]
struct ProgLoad {
    source: ProgSource,
    log: ProgLog,
    metadata: ProgMetadata,
}

pub struct Installer {
    program: Loaded,
    map: Map,
}

#[repr(transparent)]
struct Map(OwnedFd);

#[repr(transparent)]
struct Loaded(OwnedFd);

impl Installer {
    pub(crate) fn try_new(plan: Plan) -> Result<Self, Error> {
        let count = plan.count();
        let map = Map::create(count)?;
        let program = Loaded::load(&map, plan)?;
        Ok(Self { program, map })
    }
}

impl Attach for Installer {
    fn try_attach(&self, socket: BorrowedFd<'_>) -> Result<(), Error> {
        let value = AsRawFd::as_raw_fd(&self.program.0);
        Uring::set_option::<dope_sys::Errno>(
            socket,
            libc::SOL_SOCKET,
            libc::SO_ATTACH_REUSEPORT_EBPF,
            &value,
        )
        .map_err(|errno| Error {
            operation: Operation::ProgramAttach,
            errno,
        })
    }

    fn try_insert(self, key: Index, socket: BorrowedFd<'_>) -> Result<Self, Error> {
        let key = key.into_raw() as u32;
        let value = AsRawFd::as_raw_fd(&socket) as u32;
        let attr = MapUpdate {
            map_fd: AsRawFd::as_raw_fd(&self.map.0) as u32,
            padding: 0,
            key: address(&key),
            value: address(&value),
            flags: 0,
        };
        bpf(Operation::MapInsert, MAP_UPDATE_ELEM, &attr)?;
        Ok(self)
    }
}

impl Map {
    fn create(count: Count) -> Result<Self, Error> {
        let mut attr = MapCreate {
            shape: MapShape {
                map_type: MAP_TYPE,
                key_size: mem::size_of::<u32>() as u32,
                value_size: mem::size_of::<u32>() as u32,
                max_entries: u32::from(count.encode()),
                ..MapShape::default()
            },
            ..MapCreate::default()
        };
        attr.map_name[..14].copy_from_slice(b"dope_route_map");
        let map = bpf_fd(Operation::MapCreate, MAP_CREATE, &attr)?;
        Ok(Self(map))
    }

    fn encode(&self) -> program::Instruction {
        program::instruction(0x18, 2, 1, 0, AsRawFd::as_raw_fd(&self.0))
    }
}

impl Loaded {
    fn load(map: &Map, plan: Plan) -> Result<Self, Error> {
        let program = program::Program::compile(map, plan);
        let license = b"GPL\0";
        let mut name = [0_u8; 16];
        name[..10].copy_from_slice(b"dope_route");
        let code = program.instructions();
        let attr = ProgLoad {
            source: ProgSource {
                prog_type: PROGRAM_TYPE,
                insn_cnt: code.len() as u32,
                insns: address_slice(code),
                license: address_slice(license),
            },
            log: ProgLog {
                log_level: 0,
                log_size: 0,
                log_buf: 0,
            },
            metadata: ProgMetadata {
                kern_version: 0,
                prog_flags: 0,
                prog_name: name,
                prog_ifindex: 0,
                expected_attach_type: ATTACH_TYPE,
            },
        };
        Ok(Self(bpf_fd(Operation::ProgramLoad, PROG_LOAD, &attr)?))
    }
}

fn address<T>(value: &T) -> u64 {
    ptr::from_ref(value) as u64
}

fn address_slice<T>(values: &[T]) -> u64 {
    values.as_ptr() as u64
}

fn bpf_fd<T>(op: Operation, command: libc::c_uint, attr: &T) -> Result<OwnedFd, Error> {
    let value = bpf(op, command, attr)?;
    // SAFETY: these commands transfer one fresh descriptor on success; bpf
    // already admitted its nonnegative native value without losing ownership.
    Ok(unsafe { OwnedFd::from_raw_fd(value.get()) })
}

fn bpf<T>(op: Operation, command: libc::c_uint, attr: &T) -> Result<kernel::Value, Error> {
    // SAFETY: each caller retains its command's complete native attribute and
    // every referenced map, instruction, key and value through this call.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_bpf,
            command,
            ptr::from_ref(attr).cast::<libc::c_void>(),
            mem::size_of::<T>(),
        )
    };
    // SAFETY: these commands return -1, zero or a live c_int descriptor;
    // failure reads its unchanged errno before any other native call.
    unsafe {
        Uring::status(rc).try_value::<dope_sys::Errno>(|| kernel::raw::Code::last::<Uring>().into())
    }
    .map_err(|errno| Error {
        operation: op,
        errno,
    })
}

const _: () = {
    assert!(mem::size_of::<MapCreate>() == 72);
    assert!(mem::size_of::<MapUpdate>() == 32);
    assert!(mem::size_of::<ProgLoad>() == 72);
    assert!(mem::size_of::<Installer>() == 2 * mem::size_of::<OwnedFd>());
    assert!(mem::align_of::<Installer>() == mem::align_of::<OwnedFd>());
    assert!(mem::align_of::<Map>() == mem::align_of::<OwnedFd>());
    assert!(mem::align_of::<Loaded>() == mem::align_of::<OwnedFd>());
    assert!(mem::size_of::<program::Instruction>() == 8);
};
