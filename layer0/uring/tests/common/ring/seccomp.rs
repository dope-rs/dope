use std::{mem, ptr};

use dope_sys::{Errno, kernel::raw::Status};
use dope_uring::Uring;

#[derive(Debug)]
pub enum Error {
    NoNewPrivileges(Errno),
    Filter(Errno),
}

const LOAD: u32 = libc::BPF_LD | libc::BPF_W | libc::BPF_ABS;
const EQUAL: u32 = libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K;
const RETURN: u32 = libc::BPF_RET | libc::BPF_K;

/// Denies ring-registration release on this calling thread only.
pub fn deny_unregister() -> Result<(), Error> {
    deny_argument(libc::SYS_io_uring_register, 1, 21, libc::EPERM)
}

pub fn deny_timer_settime() -> Result<(), Error> {
    install(&mut [
        instruction(LOAD, 0, 0, mem::offset_of!(libc::seccomp_data, nr) as u32),
        instruction(EQUAL, 0, 1, libc::SYS_timer_settime as u32),
        instruction(RETURN, 0, 0, libc::SECCOMP_RET_ERRNO | libc::EPERM as u32),
        instruction(RETURN, 0, 0, libc::SECCOMP_RET_ALLOW),
    ])
}

/// Rejects population while leaving mapping release available on this test thread.
pub fn deny_populate() -> Result<(), Error> {
    deny_argument(
        libc::SYS_madvise,
        2,
        libc::MADV_POPULATE_WRITE as u64,
        libc::ENOMEM,
    )
}

/// Rejects only completion waits, leaving submission and native cleanup available.
pub fn deny_wait(code: dope_sys::Errno) -> Result<(), Error> {
    deny_argument(libc::SYS_io_uring_enter, 2, 1, code.code().get())
}

fn deny_argument(
    syscall: libc::c_long,
    index: usize,
    value: u64,
    code: libc::c_int,
) -> Result<(), Error> {
    let offset = (mem::offset_of!(libc::seccomp_data, args) + index * mem::size_of::<u64>()) as u32;
    let bytes = value.to_ne_bytes();
    let first = u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let second = u32::from_ne_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    install(&mut [
        instruction(LOAD, 0, 0, mem::offset_of!(libc::seccomp_data, nr) as u32),
        instruction(EQUAL, 0, 5, syscall as u32),
        instruction(LOAD, 0, 0, offset),
        instruction(EQUAL, 0, 3, first),
        instruction(LOAD, 0, 0, offset + 4),
        instruction(EQUAL, 0, 1, second),
        instruction(RETURN, 0, 0, libc::SECCOMP_RET_ERRNO | code as u32),
        instruction(RETURN, 0, 0, libc::SECCOMP_RET_ALLOW),
    ])
}

fn install(filter: &mut [libc::sock_filter]) -> Result<(), Error> {
    let program = libc::sock_fprog {
        len: filter.len() as u16,
        filter: filter.as_mut_ptr(),
    };
    // SAFETY: all prctl arguments are scalars; the flag changes only this thread.
    unsafe {
        Status::new(libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)).try_zero::<Uring, Errno>()
    }
    .map_err(Error::NoNewPrivileges)?;
    // SAFETY: the complete program remains live during installation. Zero flags
    // exclude thread synchronization, and every unmatched syscall is allowed.
    unsafe {
        Status::new(libc::syscall(
            libc::SYS_seccomp,
            libc::SECCOMP_SET_MODE_FILTER,
            0,
            ptr::from_ref(&program),
        ) as libc::c_int)
        .try_zero::<Uring, Errno>()
    }
    .map_err(Error::Filter)
}

const fn instruction(code: u32, jt: u8, jf: u8, k: u32) -> libc::sock_filter {
    libc::sock_filter {
        code: code as u16,
        jt,
        jf,
        k,
    }
}
