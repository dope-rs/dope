pub(crate) use changes::Changes;
pub(crate) use delete::Delete;
use entry::Entry;
pub(crate) use id::Id;
pub(crate) use interest::Interest;
pub(crate) use tail::{Batch, Tail};

mod changes;
mod delete;
mod entry;
mod id;
mod interest;
pub(crate) mod raw;
mod tail;

fn event(fd: libc::c_int, filter: i16, flags: u16, data: *mut libc::c_void) -> libc::kevent {
    libc::kevent {
        ident: fd as libc::uintptr_t,
        filter,
        flags,
        fflags: 0,
        data: 0,
        udata: data,
    }
}
