use std::marker::PhantomData;

use dope_sys::{
    Buffer as _,
    io::{
        fd::Fd as _,
        reactor::{complete::Vacancy, read, read::Outcome},
        socket,
        socket::{Prepare as _, accept},
    },
    memory,
    memory::buffer::Pool as _,
    table,
    table::Files as _,
};
use libc::socklen_t;

use crate::{
    Kqueue,
    reactor::input::{Call, File, entry::Action},
    store::{
        buffer::{Buffer, Pool},
        table::Files,
    },
};

pub(in crate::reactor::input) struct Drain<T>(PhantomData<fn() -> T>);

impl<C, L, E> Drain<(C, L, E)>
where
    C: read::Context,
    L: memory::raw::Layout,
    E: read::Complete<
            Key = C::Key,
            Flow = C::Flow,
            Accepted = <C::Slots as table::raw::Slots>::Accepted,
            Buffer = Buffer,
        >,
{
    /// # Safety
    /// Accepted address pointers retain writable full storage and length through completion.
    /// Datagram reads retain an admitted metadata and payload pool layout.
    pub(super) unsafe fn resume(
        files: &mut Files<C::Slots>,
        pool: &Pool<L>,
        call: Call<C>,
        vacant: impl Vacancy<Event = E>,
    ) -> Result<Outcome, <C::Slots as table::raw::Slots>::Missing> {
        match call.0 {
            Action::Accept(accept) => unsafe {
                Self::accept(
                    files,
                    accept.header.file,
                    accept.header.key,
                    accept.addr,
                    accept.len,
                    accept.flow,
                    vacant,
                )
            },
            Action::Recv(header) => unsafe {
                Self::recv::<false>(files, pool, header.file, header.key, vacant)
            },
            Action::Message(header) => unsafe {
                Self::recv::<true>(files, pool, header.file, header.key, vacant)
            },
        }
    }

    unsafe fn accept(
        files: &mut Files<C::Slots>,
        file: File<C>,
        key: E::Key,
        addr: *mut libc::sockaddr_storage,
        len: *mut socklen_t,
        flow: E::Flow,
        vacant: impl Vacancy<Event = E>,
    ) -> Result<Outcome, <C::Slots as table::raw::Slots>::Missing> {
        let result = unsafe { socket::raw::Open::accept(files.try_socket(file)?, addr, len) };
        let failure = match result {
            Err(accept::Error::Syscall(errno)) if errno.is_block() => return Ok(Outcome::Rearm),
            Err(error) => error,
            Ok(accepted) => match files.establish(accepted) {
                Ok(accepted) => {
                    vacant.push(E::accepted(key, accepted, flow));
                    return Ok(if flow == E::LAST {
                        Outcome::Terminal
                    } else {
                        Outcome::Yield
                    });
                }
                Err(accepted) => accept::Error::Capacity {
                    close: Kqueue::close(Kqueue::into_fd(accepted)),
                },
            },
        };
        vacant.push(E::rejected(key, failure, flow));
        Ok(if flow == E::LAST {
            Outcome::Terminal
        } else {
            Outcome::Rearm
        })
    }

    /// # Safety
    /// When `MESSAGE` is true, the pool retains the admitted datagram layout.
    unsafe fn recv<const MESSAGE: bool>(
        files: &Files<C::Slots>,
        pool: &Pool<L>,
        file: File<C>,
        key: E::Key,
        vacant: impl Vacancy<Event = E>,
    ) -> Result<Outcome, <C::Slots as table::raw::Slots>::Missing> {
        let file = files.try_socket(file)?;
        let Some(mut buffer) = pool.take() else {
            return Ok(Outcome::Yield);
        };
        let received = if MESSAGE {
            // SAFETY: the caller proves the datagram layout for this specialization.
            unsafe { socket::raw::Receive::receive(file, buffer.output()) }.map(Some)
        } else {
            socket::raw::Receive::recv(file, buffer.output())
        };
        let errno = match received {
            Ok(Some(complete)) => {
                complete.commit();
                vacant.push(E::received(key, buffer, E::MORE));
                return Ok(Outcome::Yield);
            }
            Ok(None) => {
                buffer.recycle();
                vacant.push(E::control(key, Ok(()), E::LAST));
                return Ok(Outcome::Terminal);
            }
            Err(error) => error,
        };
        buffer.recycle();
        if errno.is_block() {
            return Ok(Outcome::Rearm);
        }
        vacant.push(E::control(key, Err(errno), E::MORE));
        Ok(Outcome::Rearm)
    }
}
