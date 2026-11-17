use dope_sys::io::reactor::ready::{Backend as _, store::State as _};

use crate::sockets::ops::{Context, Engine, Operation, change, device, ready, tuning};

impl<C, H, const N: usize> tuning::raw::Apply<Engine<C>, H, N> for Operation<H>
where
    C: ready::Driver,
    H: device::raw::Host<
            Engine<C>,
            N,
            Slot = <<C::Context as Context>::Slots as change::raw::Slots>::Physical,
            Value = (),
        >,
{
    unsafe fn try_apply(self) -> Result<H::Output, H::Error> {
        self.host.direct(|backend, fd, slot, options| {
            match backend.state().set(slot, options.map(Into::into)) {
                Ok(()) => Ok((fd, ())),
                Err(error) => Err((fd, error)),
            }
        })
    }
}

impl<C, H, const N: usize> tuning::raw::Peer<Engine<C>, H, N> for Operation<H>
where
    C: ready::Driver,
    H: device::raw::Host<
            Engine<C>,
            N,
            Slot = <<C::Context as Context>::Slots as change::raw::Slots>::Physical,
            Value = u32,
        >,
{
    fn try_peer(self) -> Result<H::Output, H::Error> {
        use dope_sys::{Errno, table::Files as _};

        self.host.direct(|backend, fd, slot, _options| {
            let Some(native) = backend.state().view().files.borrow(slot) else {
                return Err((fd, Errno::BAD_DESCRIPTOR));
            };
            match peer_uid(native) {
                Ok(uid) => Ok((fd, uid)),
                Err(cause) => Err((fd, cause)),
            }
        })
    }
}

fn peer_uid(fd: std::os::fd::BorrowedFd<'_>) -> Result<u32, dope_sys::Errno> {
    use std::os::fd::AsRawFd as _;
    use dope_sys::{Errno, kernel};

    let mut uid = 0;
    let mut gid = 0;
    // SAFETY: the admitted slot owns this fd; both outputs remain live for the call.
    let status = unsafe { libc::getpeereid(fd.as_raw_fd(), &mut uid, &mut gid) };
    // SAFETY: getpeereid returns zero or -1 and preserves the failing errno.
    unsafe { kernel::raw::Status::new(status).try_zero::<crate::Kqueue, Errno>() }?;
    Ok(uid)
}
