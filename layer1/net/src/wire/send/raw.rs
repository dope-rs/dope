use std::convert::Infallible;

use dope_core::{
    driver::{
        result, retained,
        retained::flight,
        route::{self, kind},
    },
    io::fd::handles,
};

use crate::wire::send::{
    self, Either, Submitted,
    sources::{plain::Slice, vectored::Vectors},
};

pub trait Plain<'a> {
    /// Constructs a retained direct-send view.
    /// # Safety
    /// The bytes stay fixed through completion or quiescence.
    unsafe fn retain(self) -> send::Plain<'a>;
}

impl<'a> Plain<'a> for &'a [u8] {
    unsafe fn retain(self) -> send::Plain<'a> {
        send::Plain::proven(self)
    }
}

/// Native submission which erases the borrowed representation's lifetime.
pub trait Ready<'a> {
    /// # Safety
    /// Retain descriptor authority and the immutable payload/message graph at
    /// fixed addresses through completion or quiescence.
    unsafe fn submit<'owner, 'd: 'owner, Tag: route::Tag + route::Select<kind::Send>>(
        self,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
        slots: &flight::Slots<'d, Tag>,
        driver: retained::Context<'_, 'owner, 'd>,
    ) -> Result<Submitted<'d>, result::SubmitError>
    where
        'owner: 'a;
}

impl<'a> Ready<'a> for Infallible {
    unsafe fn submit<'owner, 'd: 'owner, Tag: route::Tag + route::Select<kind::Send>>(
        self,
        _: &handles::Descriptor<'d>,
        _: route::Target<'d, Tag>,
        _: &flight::Slots<'d, Tag>,
        _: retained::Context<'_, 'owner, 'd>,
    ) -> Result<Submitted<'d>, result::SubmitError>
    where
        'owner: 'a,
    {
        match self {}
    }
}

impl<'a> Ready<'a> for Slice<'a> {
    unsafe fn submit<'owner, 'd: 'owner, Tag: route::Tag + route::Select<kind::Send>>(
        self,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
        slots: &flight::Slots<'d, Tag>,
        driver: retained::Context<'_, 'owner, 'd>,
    ) -> Result<Submitted<'d>, result::SubmitError>
    where
        'owner: 'a,
    {
        let op = retained::raw::Submission::send(fd, self.view, target);
        // SAFETY: the caller retains the exact bytes and descriptor authority
        // through the terminal completion represented by the returned flight.
        let flight = unsafe { op.try_submit(driver, slots) }?;
        Ok(Submitted {
            bytes: self.bytes,
            flight,
        })
    }
}

impl<'a> Ready<'a> for Vectors<'a> {
    unsafe fn submit<'owner, 'd: 'owner, Tag: route::Tag + route::Select<kind::Send>>(
        self,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
        slots: &flight::Slots<'d, Tag>,
        driver: retained::Context<'_, 'owner, 'd>,
    ) -> Result<Submitted<'d>, result::SubmitError>
    where
        'owner: 'a,
    {
        let op = retained::raw::Submission::send_msg(fd, self.message, target);
        // SAFETY: the caller retains the exact message graph, payload and
        // descriptor authority through terminal completion or quiescence.
        let flight = unsafe { op.try_submit(driver, slots) }?;
        Ok(Submitted {
            bytes: self.bytes,
            flight,
        })
    }
}

impl<'a, L: Ready<'a>, R: Ready<'a>> Ready<'a> for Either<L, R> {
    unsafe fn submit<'owner, 'd: 'owner, Tag: route::Tag + route::Select<kind::Send>>(
        self,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
        slots: &flight::Slots<'d, Tag>,
        driver: retained::Context<'_, 'owner, 'd>,
    ) -> Result<Submitted<'d>, result::SubmitError>
    where
        'owner: 'a,
    {
        // SAFETY: the active branch retains the exact graph covered by the
        // caller's submission contract; selecting it changes no addresses.
        match self {
            Self::Left(data) => unsafe { data.submit(fd, target, slots, driver) },
            Self::Right(data) => unsafe { data.submit(fd, target, slots, driver) },
        }
    }
}

pub(crate) struct Submission<R>(pub(crate) R);

impl<R> Submission<R> {
    pub(crate) fn submit<'a, 'owner: 'a, 'd: 'owner, Tag: route::Tag + route::Select<kind::Send>>(
        self,
        fd: &handles::Descriptor<'d>,
        target: route::Target<'d, Tag>,
        slots: &flight::Slots<'d, Tag>,
        driver: retained::Context<'_, 'owner, 'd>,
    ) -> Result<Submitted<'d>, result::SubmitError>
    where
        R: Ready<'a>,
    {
        // SAFETY: the installed pool retains wire and queue storage; direct callers
        // additionally establish the original source graph's retention contract.
        unsafe { self.0.submit(fd, target, slots, driver) }
    }
}
