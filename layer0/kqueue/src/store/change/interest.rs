use std::ptr;

use dope_sys::table::change::{self, Id as _};

use crate::store::change::{Entry, Id, event};

pub struct Interest<'a, S: change::raw::Slots, const FILTER: i16> {
    pub(super) id: Id<S::Physical>,
    pub(super) entry: Entry<'a>,
}

impl<S: change::raw::Slots, const FILTER: i16> change::Interest for Interest<'_, S, FILTER> {
    type Id = Id<S::Physical>;
    type Data = S::Data;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn arm(self, flags: u16, data: S::Data) {
        self.upsert(flags, ptr::without_provenance_mut(data.into()));
    }

    fn delete(self) {
        self.upsert(libc::EV_DELETE, ptr::null_mut());
    }

    fn discard(self) {
        self.entry.set(None);
    }
}

impl<S: change::raw::Slots> change::Write for Interest<'_, S, { libc::EVFILT_WRITE }> {
    fn cancel(self) {
        if self.entry.get().is_none() {
            self.upsert(libc::EV_DELETE, ptr::null_mut());
        } else {
            change::Interest::discard(self);
        }
    }
}

impl<S: change::raw::Slots, const FILTER: i16> Interest<'_, S, FILTER> {
    fn upsert(self, flags: u16, data: *mut libc::c_void) {
        if let Some(mut current) = self.entry.get()
            && current.flags & libc::EV_ADD != 0
            && flags & (libc::EV_ADD | libc::EV_DELETE) == 0
        {
            current.flags |= flags;
            current.udata = data;
            self.entry.set(Some(current));
            return;
        }
        let event = event(self.id.fd(), FILTER, flags, data);
        self.entry.set(Some(event));
    }
}
