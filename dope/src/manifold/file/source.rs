use std::io;
use std::marker::PhantomData;
use std::os::fd::{BorrowedFd, OwnedFd};
use std::rc::Rc;

use dope_core::io::fd::Fd;

#[derive(Debug)]
enum Kind<'d> {
    Direct(Rc<OwnedFd>),
    Fixed(Rc<Fd<'d>>),
}

#[derive(Debug)]
pub struct Source<'d, K = Direct> {
    kind: Kind<'d>,
    marker: PhantomData<K>,
}

#[derive(Debug)]
pub enum Direct {}

#[derive(Debug)]
pub enum Fixed {}

#[doc(hidden)]
pub enum SourceRef<'d> {
    Direct(Rc<OwnedFd>),
    Fixed(Rc<Fd<'d>>),
}

impl<'d> Source<'d, Direct> {
    pub fn from_owned_fd(fd: OwnedFd) -> Self {
        Self::owned(fd)
    }

    pub fn try_from_fd(fd: BorrowedFd<'_>) -> io::Result<Self> {
        fd.try_clone_to_owned().map(Self::from_owned_fd)
    }

    #[doc(hidden)]
    pub fn direct(&self) -> Rc<OwnedFd> {
        match &self.kind {
            Kind::Direct(fd) => Rc::clone(fd),
            Kind::Fixed(_) => unreachable!(),
        }
    }

    #[doc(hidden)]
    pub fn owned(fd: OwnedFd) -> Self {
        Self {
            kind: Kind::Direct(Rc::new(fd)),
            marker: PhantomData,
        }
    }
}

impl<'d> Source<'d, Fixed> {
    #[doc(hidden)]
    pub fn fixed(fd: Fd<'d>) -> Self {
        Self {
            kind: Kind::Fixed(Rc::new(fd)),
            marker: PhantomData,
        }
    }
}

impl<'d, K> Source<'d, K> {
    pub fn is_fixed(&self) -> bool {
        matches!(self.kind, Kind::Fixed(_))
    }

    #[doc(hidden)]
    pub fn lease(&self) -> SourceRef<'d> {
        match &self.kind {
            Kind::Direct(fd) => SourceRef::Direct(Rc::clone(fd)),
            Kind::Fixed(fd) => SourceRef::Fixed(Rc::clone(fd)),
        }
    }
}
