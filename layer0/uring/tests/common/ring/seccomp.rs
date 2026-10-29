use std::{io, mem, ptr};

#[derive(Debug)]
pub enum Error {
    NoNewPrivileges(io::Error),
    Filter(io::Error),
}

const LOAD: u32 = libc::BPF_LD | libc::BPF_W | libc::BPF_ABS;
const EQUAL: u32 = libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K;
const RETURN: u32 = libc::BPF_RET | libc::BPF_K;

/// Denies ring-registration release on this calling thread only.
pub fn deny_unregister() -> Result<(), Error> {
    const OPCODE: u32 = (mem::offset_of!(libc::seccomp_data, args) + mem::size_of::<u64>()) as u32;
    let opcode = 21_u64.to_ne_bytes();
    let first = u32::from_ne_bytes([opcode[0], opcode[1], opcode[2], opcode[3]]);
    let second = u32::from_ne_bytes([opcode[4], opcode[5], opcode[6], opcode[7]]);
    install(&mut [
        instruction(LOAD, 0, 0, mem::offset_of!(libc::seccomp_data, nr) as u32),
        instruction(EQUAL, 0, 5, libc::SYS_io_uring_register as u32),
        instruction(LOAD, 0, 0, OPCODE),
        instruction(EQUAL, 0, 3, first),
        instruction(LOAD, 0, 0, OPCODE + 4),
        instruction(EQUAL, 0, 1, second),
        instruction(RETURN, 0, 0, libc::SECCOMP_RET_ERRNO | libc::EPERM as u32),
        instruction(RETURN, 0, 0, libc::SECCOMP_RET_ALLOW),
    ])
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
    const ADVICE: u32 =
        (mem::offset_of!(libc::seccomp_data, args) + 2 * mem::size_of::<u64>()) as u32;
    let advice = (libc::MADV_POPULATE_WRITE as u64).to_ne_bytes();
    let first = u32::from_ne_bytes([advice[0], advice[1], advice[2], advice[3]]);
    let second = u32::from_ne_bytes([advice[4], advice[5], advice[6], advice[7]]);
    install(&mut [
        instruction(LOAD, 0, 0, mem::offset_of!(libc::seccomp_data, nr) as u32),
        instruction(EQUAL, 0, 5, libc::SYS_madvise as u32),
        instruction(LOAD, 0, 0, ADVICE),
        instruction(EQUAL, 0, 3, first),
        instruction(LOAD, 0, 0, ADVICE + 4),
        instruction(EQUAL, 0, 1, second),
        instruction(RETURN, 0, 0, libc::SECCOMP_RET_ERRNO | libc::ENOMEM as u32),
        instruction(RETURN, 0, 0, libc::SECCOMP_RET_ALLOW),
    ])
}

/// Rejects only completion waits, leaving submission and native cleanup available.
pub fn deny_wait(code: dope_sys::Errno) -> Result<(), Error> {
    const COMPLETE: u32 =
        (mem::offset_of!(libc::seccomp_data, args) + 2 * mem::size_of::<u64>()) as u32;
    let complete = 1_u64.to_ne_bytes();
    let first = u32::from_ne_bytes([complete[0], complete[1], complete[2], complete[3]]);
    let second = u32::from_ne_bytes([complete[4], complete[5], complete[6], complete[7]]);
    install(&mut [
        instruction(LOAD, 0, 0, mem::offset_of!(libc::seccomp_data, nr) as u32),
        instruction(EQUAL, 0, 5, libc::SYS_io_uring_enter as u32),
        instruction(LOAD, 0, 0, COMPLETE),
        instruction(EQUAL, 0, 3, first),
        instruction(LOAD, 0, 0, COMPLETE + 4),
        instruction(EQUAL, 0, 1, second),
        instruction(
            RETURN,
            0,
            0,
            libc::SECCOMP_RET_ERRNO | code.code().get() as u32,
        ),
        instruction(RETURN, 0, 0, libc::SECCOMP_RET_ALLOW),
    ])
}

fn install(filter: &mut [libc::sock_filter]) -> Result<(), Error> {
    let program = libc::sock_fprog {
        len: filter.len() as u16,
        filter: filter.as_mut_ptr(),
    };
    // SAFETY: all prctl arguments are scalars; the flag changes only this thread.
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0 {
        return Err(Error::NoNewPrivileges(io::Error::last_os_error()));
    }
    // SAFETY: the complete program remains live during installation. Zero flags
    // exclude thread synchronization, and every unmatched syscall is allowed.
    if unsafe {
        libc::syscall(
            libc::SYS_seccomp,
            libc::SECCOMP_SET_MODE_FILTER,
            0,
            ptr::from_ref(&program),
        )
    } != 0
    {
        return Err(Error::Filter(io::Error::last_os_error()));
    }
    Ok(())
}

const fn instruction(code: u32, jt: u8, jf: u8, k: u32) -> libc::sock_filter {
    libc::sock_filter {
        code: code as u16,
        jt,
        jf,
        k,
    }
}
