//! Raw construction of retained-owner authority.
use std::{marker, mem};

use dope_sys::io::socket::{
    addr::{Length, Storage},
    option::tuning,
};
use o3::base::marker::ThreadBound;

use crate::{
    Engine, driver,
    driver::{
        flight,
        life::native::{request, request::bound},
        result, retained, route,
        route::kind,
    },
    io::{
        fd::handles,
        fs, socket,
        socket::{msg, option, pending},
        transfer,
    },
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
    /// Retain these exact borrowed resources at their submitted addresses until
    /// completion or quiescence; the context brand alone does not prove ownership.
    pub unsafe fn try_submit<'a, 'owner>(
        self,
        mut context: retained::Context<'a, 'owner, 'd>,
        slots: &'a flight::Slots<'d, Tag>,
    ) -> Result<flight::Flight<'d, C>, result::SubmitError>
    where
        'd: 'owner,
        'owner: 'borrow,
        Tag: route::Tag,
        C: kind::Flight,
    {
        // SAFETY: try_submit requires the exact pinned owner to retain every
        // resource borrowed by this entry through quiescence.
        let request::raw::Submission {
            submission: raw,
            target,
        } = unsafe { request::raw::Submission::bind(context.token(), self) };
        let (permit, space) = slots.try_admit(&mut context.driver, target)?;
        let entry = bound::Bound::new(raw, space);
        Ok(permit.retain(entry))
    }
}

impl<'borrow, 'd, Tag, C> Submission<'borrow, 'd, Tag, C>
where
    Tag: route::Tag,
    C: kind::Flight,
{
    /// # Safety
    /// Retain these exact borrowed resources at their submitted addresses until
    /// completion or quiescence; the context brand alone does not prove ownership.
    pub unsafe fn try_file<'a, 'owner>(
        mut context: retained::Context<'a, 'owner, 'd>,
        slots: &'a flight::Slots<'d, Tag>,
        entry: fs::Submission<'borrow, 'd, Tag, C>,
    ) -> Result<flight::Flight<'d, C>, result::SubmitError>
    where
        'd: 'owner,
        'owner: 'borrow,
        Tag: route::Tag,
        C: kind::Flight,
    {
        let (raw, target) = entry.into_parts();
        let raw = bound::Captured::new(raw, context.token());
        let (permit, space) = slots.try_admit(&mut context.driver, target)?;
        let entry = bound::Bound::new(raw, space);
        Ok(permit.file(entry))
    }
}

impl<'borrow, 'd, Tag> Connect<'borrow, 'd, Tag>
where
    Tag: route::Tag + route::Select<kind::Connect>,
{
    /// # Safety
    /// Retain this exact address storage in place until completion or quiescence;
    /// the context brand alone does not prove that ownership relation.
    pub unsafe fn try_connect<'a, 'owner>(
        self,
        mut context: retained::Context<'a, 'owner, 'd>,
        slots: &'a flight::Slots<'d, Tag>,
        options: &'a option::Set,
    ) -> Result<pending::Connection<'d>, pending::Rejection<'d>>
    where
        'd: 'owner,
        'owner: 'borrow,
        Tag: route::Tag + route::Select<kind::Connect>,
    {
        use tuning::device::{Owner as _, Source};
        // SAFETY: try_connect requires the exact pinned owner to retain the
        // address borrowed by this entry through quiescence.
        let (fd, entry) = unsafe { request::raw::Connect::bind(&context, self) };
        let host = request::Connect::new(context.native(), slots, fd, options, entry);
        let socket = <crate::Backend as Source>::Socket::new(host);
        // SAFETY: Captured retains the address through completion or quiescence;
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

/// Zero-sized, thread-bound authority for retained driver storage.
/// Each raw submission must additionally prove that its borrowed resources
/// belong to an owner retained through completion or quiescence.
#[derive(Clone, Copy)]
pub struct Owner<'owner, 'd: 'owner> {
    proof: Proof<'owner, 'd>,
}

#[derive(Clone, Copy)]
pub(super) struct Proof<'owner, 'd: 'owner> {
    owner: marker::PhantomData<(&'owner (), ThreadBound)>,
    _driver: driver::Invariant<'d>,
}

/// One affine capture of a retained owner's driver-bound proof.
pub(crate) struct Token<'owner, 'd: 'owner>(Proof<'owner, 'd>);

impl<'owner, 'd: 'owner> Token<'owner, 'd> {
    pub(super) const fn new(proof: Proof<'owner, 'd>) -> Self {
        Self(proof)
    }
}

impl<'d> Token<'d, 'd> {
    pub(crate) fn scoped() -> Self {
        Self(Proof {
            owner: marker::PhantomData,
            _driver: driver::Invariant(marker::PhantomData),
        })
    }
}

impl<'owner, 'd: 'owner> Owner<'owner, 'd> {
    pub(super) fn into_proof(self) -> Proof<'owner, 'd> {
        self.proof
    }
}

impl<'d> Owner<'d, 'd> {
    /// Carries one driver's brand into its retained submission context.
    /// Each raw submission still requires its exact resource-retention proof.
    pub fn from_driver(driver: driver::Reference<'d>) -> Self {
        Self {
            proof: Proof {
                owner: marker::PhantomData,
                _driver: driver.brand,
            },
        }
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
const _: () = {
    assert!(mem::size_of::<Owner<'static, 'static>>() == 0);
    assert!(mem::align_of::<Owner<'static, 'static>>() == 1);
    assert!(mem::size_of::<Token<'static, 'static>>() == 0);
    assert!(mem::align_of::<Token<'static, 'static>>() == 1);
};
