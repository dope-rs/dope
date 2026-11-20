use std::mem;

use crate::{
    Backend, Engine, driver,
    driver::{
        flight,
        life::native::{
            host::Host,
            request,
            request::{bound, permits},
        },
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
use dope_sys::{
    engine::admit::raw::Retain,
    io::socket::{addr, option::tuning},
};

impl<'d, Tag: route::Tag> flight::Slots<'d, Tag> {
    fn try_retain<C: kind::Flight>(
        &self,
        mut context: driver::Context<'_, 'd>,
        target: route::Operation<'d, Tag, C>,
        submit: impl FnOnce(permits::Guard<'_>, flight::raw::Echo),
    ) -> Result<flight::Flight<'d, C>, result::SubmitError> {
        let (permit, space) = self.try_admit(&mut context, target)?;
        Ok(permit.retain(bound::Bound::new(submit, space)))
    }

    /// # Safety
    /// Descriptor, target and slots belong to this driver. Keep the descriptor alive and the
    /// bytes fixed and immutable through terminal completion or quiescence.
    pub unsafe fn try_send<'r>(
        &self,
        context: driver::Context<'_, 'd>,
        fd: &'r handles::Descriptor<'d>,
        buffer: transfer::View<'r>,
        target: route::Target<'d, Tag>,
    ) -> Result<flight::Flight<'d, kind::Send>, result::SubmitError>
    where
        'd: 'r,
        Tag: route::Select<kind::Send>,
    {
        self.try_retain(
            context,
            target.operation::<kind::Send>(),
            |native, key| unsafe {
                Retain::<Host<Backend>>::send(
                    native,
                    request::Lowered::target(fd.slot_ref()),
                    buffer.into_prefix(),
                    key,
                );
            },
        )
    }

    /// # Safety
    /// Descriptor, target and slots belong to this driver. Keep the descriptor alive and the
    /// message graph fixed and immutable through terminal completion or quiescence.
    pub unsafe fn try_send_msg<'r>(
        &self,
        context: driver::Context<'_, 'd>,
        fd: &'r handles::Descriptor<'d>,
        message: msg::Message<'r>,
        target: route::Target<'d, Tag>,
    ) -> Result<flight::Flight<'d, kind::Send>, result::SubmitError>
    where
        'd: 'r,
        Tag: route::Select<kind::Send>,
    {
        self.try_retain(
            context,
            target.operation::<kind::Send>(),
            |native, key| unsafe {
                Retain::<Host<Backend>>::send_msg(
                    native,
                    request::Lowered::target(fd.slot_ref()),
                    message,
                    key,
                );
            },
        )
    }

    /// # Safety
    /// Listener, target and slots belong to this driver. Keep the listener alive and the
    /// exclusive peer output fixed through terminal completion or quiescence.
    pub(in crate::driver::retained) unsafe fn try_accept_one(
        &self,
        context: driver::Context<'_, 'd>,
        fd: &handles::Descriptor<'d>,
        peer: &mut addr::Storage,
        target: route::Operation<'d, Tag, kind::Accept>,
    ) -> Result<flight::Flight<'d, kind::Accept>, result::SubmitError> {
        self.try_retain(context, target, |native, key| unsafe {
            Retain::<Host<Backend>>::accept(
                native,
                request::Lowered::target(fd.slot_ref()),
                peer,
                key,
            );
        })
    }

    /// # Safety
    /// Target and slots belong to this driver. Retain every file borrow through completion or quiescence.
    pub unsafe fn try_file<'r, C: kind::Flight>(
        &self,
        context: driver::Context<'_, 'd>,
        entry: fs::Submission<'r, 'd, Tag, C>,
    ) -> Result<flight::Flight<'d, C>, result::SubmitError>
    where
        'd: 'r,
    {
        let (raw, target) = entry.into_parts();
        self.try_retain(context, target, |native, key| unsafe {
            Retain::<Host<Backend>>::file(native, raw, key);
        })
    }
}

/// A connect request owning its descriptor and borrowing its address through
/// terminal completion.
#[must_use = "connect must be submitted or its exact descriptor is closed"]
pub struct Connect<'addr, 'd, Tag: route::Tag> {
    fd: handles::Descriptor<'d>,
    address: addr::Addr<'addr>,
    target: route::Target<'d, Tag>,
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
        Self {
            fd,
            address: addr.view().native(),
            target,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        handles::Descriptor<'d>,
        addr::Addr<'addr>,
        route::Target<'d, Tag>,
    ) {
        (self.fd, self.address, self.target)
    }
}

const _: () = {
    assert!(
        mem::size_of::<Connect<'static, 'static, route::KeyTag<1>>>()
            == mem::size_of::<(
                handles::Descriptor<'static>,
                addr::Addr<'static>,
                route::Target<'static, route::KeyTag<1>>,
            )>()
    );
    assert!(
        mem::align_of::<Connect<'static, 'static, route::KeyTag<1>>>()
            == mem::align_of::<(
                handles::Descriptor<'static>,
                addr::Addr<'static>,
                route::Target<'static, route::KeyTag<1>>,
            )>()
    );
};
