use std::{
    mem,
    os::fd::{AsRawFd as _, BorrowedFd},
    ptr,
};

use dope_sys::io::socket::route::{
    Attach, Plan, Source,
    count::Index,
    install::{Error, Operation},
};

use crate::Uring;

mod program;

pub struct Installer<'a> {
    plan: Plan,
    filter: &'a mut [libc::sock_filter; program::MAX],
    length: usize,
}

impl Attach for Installer<'_> {
    fn try_attach(&self, socket: BorrowedFd<'_>) -> Result<(), Error> {
        let mut code = [program::EMPTY; program::MAX];
        let length = program::build(&mut code, self.plan, None);
        attach(socket, &code[..length], Operation::ProgramAttach)
    }

    fn try_filter(&mut self, key: Index, socket: BorrowedFd<'_>) -> Result<(), Error> {
        self.filter[self.length - 3].k = key.into_raw() as u32;
        attach(socket, &self.filter[..self.length], Operation::FilterAttach)
    }
}

impl Source for Uring {
    type Storage = [libc::sock_filter; program::MAX];
    type Install<'a> = Installer<'a>;
    type Ready = ();
    type Lease<'a> = BorrowedFd<'a>;

    const STORAGE: Self::Storage = [program::EMPTY; program::MAX];

    #[inline]
    fn try_prepare(
        filter: &mut Self::Storage,
        plan: Plan,
    ) -> Result<(Self::Install<'_>, Self::Ready), Plan> {
        let length = program::build(filter, plan, Some(Index::ZERO));
        let installer = Installer {
            plan,
            filter,
            length,
        };
        Ok((installer, ()))
    }

    fn lease<'a>((): &(), socket: BorrowedFd<'a>) -> Self::Lease<'a> {
        socket
    }
}

fn attach(
    socket: BorrowedFd<'_>,
    code: &[libc::sock_filter],
    operation: Operation,
) -> Result<(), Error> {
    let program = libc::sock_fprog {
        len: code.len() as u16,
        filter: code.as_ptr().cast_mut(),
    };
    let option = match operation {
        Operation::ProgramAttach => libc::SO_ATTACH_REUSEPORT_CBPF,
        Operation::FilterAttach => libc::SO_ATTACH_FILTER,
    };
    // SAFETY: the program and its complete instruction array outlive setsockopt.
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            option,
            ptr::from_ref(&program).cast(),
            mem::size_of_val(&program) as libc::socklen_t,
        )
    };
    // SAFETY: setsockopt returned zero or -1; errno has not been changed.
    unsafe { dope_sys::kernel::raw::Status::new(result).try_zero::<Uring, dope_sys::Errno>() }
        .map_err(|errno| Error { operation, errno })
}
