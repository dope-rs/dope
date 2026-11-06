use std::mem;

use crate::{
    Engine, driver,
    driver::{
        flight,
        life::native::{request, request::bound},
        result, route,
        route::kind,
    },
    io::{
        fd::handles,
        fs, socket,
        socket::{msg, option, pending},
        transfer,
    },
};
use dope_sys::io::socket::{
    addr::{Length, Storage},
    option::tuning,
};

/// A native operation tied to every resource borrowed by its request.
pub struct Submission<'a, 'd, Tag: route::Tag, C: kind::Class = kind::Any> {
    submission: request::Borrowed<'a>,
    target: route::Operation<'d, Tag, C>,
}

impl<'a, 'd, Tag: route::Tag> Submission<'a, 'd, Tag> {
    fn bind<C: kind::Class>(
        raw: request::Borrowed<'a>,
        target: route::Operation<'d, Tag, C>,
    ) -> Submission<'a, 'd, Tag, C> {
        Submission {
            submission: raw,
            target,
        }
    }

    pub fn send(
        fd: &'a handles::Descriptor<'d>,
        buffer: transfer::View<'a>,
        target: route::Target<'d, Tag>,
    ) -> Submission<'a, 'd, Tag, kind::Send>
    where
        Tag: route::Select<kind::Send>,
    {
        Self::bind(
            request::Borrowed::send(fd.slot_ref(), buffer),
            target.operation::<kind::Send>(),
        )
    }

    pub fn send_msg(
        fd: &'a handles::Descriptor<'d>,
        msg: msg::Message<'a>,
        target: route::Target<'d, Tag>,
    ) -> Submission<'a, 'd, Tag, kind::Send>
    where
        Tag: route::Select<kind::Send>,
    {
        Self::bind(
            request::Borrowed::send_msg(fd.slot_ref(), msg),
            target.operation::<kind::Send>(),
        )
    }

    pub(in crate::driver::retained) fn accept(
        fd: &'a handles::Descriptor<'d>,
        peer: &'a mut Storage<Length>,
        target: route::Operation<'d, Tag, kind::Accept>,
    ) -> Submission<'a, 'd, Tag, kind::Accept> {
        Self::bind(request::Borrowed::accept(fd.slot_ref(), peer), target)
    }
}

impl<'a, 'd, Tag: route::Tag, C: kind::Class> Submission<'a, 'd, Tag, C> {
    pub(crate) fn into_parts(self) -> (request::Borrowed<'a>, route::Operation<'d, Tag, C>) {
        (self.submission, self.target)
    }
}

/// A connect request owning its descriptor and borrowing its address through
/// terminal completion.
#[must_use = "connect must be submitted or its exact descriptor is closed"]
pub struct Connect<'addr, 'd, Tag: route::Tag> {
    fd: handles::Descriptor<'d>,
    submission: request::Borrowed<'addr>,
    target: route::Target<'d, Tag>,
}

impl<'borrow, 'd, Tag, C> Submission<'borrow, 'd, Tag, C>
where
    Tag: route::Tag,
    C: kind::Flight,
{
    /// # Safety
    /// Handles, target and slots belong to `context`'s driver. Retain the exact
    /// borrowed resources in place through terminal completion or quiescence.
    pub unsafe fn try_submit<'a>(
        self,
        mut context: driver::Context<'a, 'd>,
        slots: &'a flight::Slots<'d, Tag>,
    ) -> Result<flight::Flight<'d, C>, result::SubmitError>
    where
        'd: 'borrow,
        Tag: route::Tag,
        C: kind::Flight,
    {
        let (raw, target) = self.into_parts();
        let (permit, space) = slots.try_admit(&mut context, target)?;
        let entry = bound::Bound::new(raw, space);
        // SAFETY: the caller retains the exact inputs for this driver's flight.
        Ok(unsafe { permit.retain(entry) })
    }
}

impl<'borrow, 'd, Tag, C> Submission<'borrow, 'd, Tag, C>
where
    Tag: route::Tag,
    C: kind::Flight,
{
    /// # Safety
    /// Target and slots belong to `context`'s driver. Retain every descriptor,
    /// path and buffer borrow through terminal completion or quiescence.
    pub unsafe fn try_file<'a>(
        mut context: driver::Context<'a, 'd>,
        slots: &'a flight::Slots<'d, Tag>,
        entry: fs::Submission<'borrow, 'd, Tag, C>,
    ) -> Result<flight::Flight<'d, C>, result::SubmitError>
    where
        'd: 'borrow,
        Tag: route::Tag,
        C: kind::Flight,
    {
        let (raw, target) = entry.into_parts();
        let (permit, space) = slots.try_admit(&mut context, target)?;
        let entry = bound::Bound::new(raw, space);
        // SAFETY: the caller retains the exact file borrows for this flight.
        Ok(unsafe { permit.file(entry) })
    }
}

impl<'borrow, 'd, Tag> Connect<'borrow, 'd, Tag>
where
    Tag: route::Tag + route::Select<kind::Connect>,
{
    /// # Safety
    /// The descriptor, target and slots belong to `context`'s driver. Retain the
    /// exact address in place through terminal completion or quiescence.
    pub unsafe fn try_connect<'a>(
        self,
        mut context: driver::Context<'a, 'd>,
        slots: &'a flight::Slots<'d, Tag>,
        options: &'a option::Set,
    ) -> Result<pending::Connection<'d>, pending::Rejection<'d>>
    where
        'd: 'borrow,
        Tag: route::Tag + route::Select<kind::Connect>,
    {
        use tuning::device::{Owner as _, Source};
        // SAFETY: the caller retains this address for the same driver's flight.
        let host = unsafe { request::Connect::new(context.native(), slots, options, self) };
        let socket = <crate::Backend as Source>::Socket::new(host);
        // SAFETY: the caller retains the address through completion or quiescence;
        // the affine descriptor and native borrow share this driver's slot table.
        unsafe {
            tuning::raw::Connect::<Engine, _, _, { option::MAX_STREAM_OPTIONS }>::try_connect(
                socket,
            )
        }
    }
}

impl<'addr, 'd, Tag: route::Tag> Connect<'addr, 'd, Tag> {
    pub fn new<A: socket::Prepared>(
        fd: handles::Descriptor<'d>,
        addr: &'addr A,
        target: route::Target<'d, Tag>,
    ) -> Self {
        let raw = request::Borrowed::connect(fd.slot(), addr);
        Self {
            fd,
            submission: raw,
            target,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        handles::Descriptor<'d>,
        request::Borrowed<'addr>,
        route::Target<'d, Tag>,
    ) {
        (self.fd, self.submission, self.target)
    }
}

const _: () = {
    assert!(
        mem::size_of::<Submission<'_, '_, route::KeyTag<1>>>()
            == mem::size_of::<(
                request::Borrowed<'_>,
                route::Operation<'_, route::KeyTag<1>>,
            )>()
    );
    assert!(
        mem::align_of::<Submission<'_, '_, route::KeyTag<1>>>()
            == mem::align_of::<(
                request::Borrowed<'_>,
                route::Operation<'_, route::KeyTag<1>>,
            )>()
    );
};
const _: () = {
    assert!(
        mem::size_of::<Connect<'static, 'static, route::KeyTag<1>>>()
            == mem::size_of::<(
                handles::Descriptor<'static>,
                request::Borrowed<'static>,
                route::Target<'static, route::KeyTag<1>>,
            )>()
    );
    assert!(
        mem::align_of::<Connect<'static, 'static, route::KeyTag<1>>>()
            == mem::align_of::<(
                handles::Descriptor<'static>,
                request::Borrowed<'static>,
                route::Target<'static, route::KeyTag<1>>,
            )>()
    );
};
