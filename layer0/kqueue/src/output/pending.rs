use std::os::fd;

use dope_sys::{Errno, io::reactor::retry, table::change::Id as _};

use crate::output::{File, Kind, Record};

pub struct Pending<C: retry::Context> {
    pub(super) target: C::Key,
    pub(super) file: File<C>,
    pub(super) kind: Kind<C>,
}

impl<C: retry::Context> retry::Entry for Pending<C> {
    type Target = C::Key;
    type Record = Record<C>;
    type File = File<C>;

    fn target(&self) -> Self::Target {
        self.target
    }

    fn fd(&self) -> fd::RawFd {
        self.file.fd()
    }

    fn descriptor(record: &Self::Record) -> fd::RawFd {
        record.file.fd()
    }

    fn file(record: &Self::Record) -> Self::File {
        record.file
    }

    fn into_parts(self) -> (Self::Target, Self::Record) {
        (
            self.target,
            Record {
                file: self.file,
                kind: self.kind,
            },
        )
    }

    fn from_parts(target: Self::Target, record: Self::Record) -> Self {
        let Record { file, kind } = record;
        Self { target, file, kind }
    }
}

impl<C: retry::Context> Pending<C> {
    pub(super) fn rejected<E: retry::Complete<C>>(self, errno: Errno) -> E {
        match &self.kind {
            Kind::Data(_) => E::sent(self.target, C::sent(Err(errno))),
            Kind::Connect => E::connected(self.target, C::connected(Err(errno))),
        }
    }
}
