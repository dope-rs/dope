//! Raw construction of retained-owner authority.

use std::{io, marker, mem, pin, rc};

use crate::{
    backend::{self, bound, operations},
    driver::{
        self, flight, retained,
        route::{self, kind},
    },
    io::{
        fd::handles,
        fs,
        socket::{self, establishment, msg, option},
        transfer,
    },
};

type Native = backend::RawSubmission;
type IoResult<T> = io::Result<T>;

/// A native operation tied to every resource borrowed by its request.
pub struct Submission<'a, 'd, Tag: route::Tag> {
    submission: Native,
    target: route::Operation<'d, Tag>,
    borrow: marker::PhantomData<&'a mut ()>,
}

const _: () = {
    type Parts = (Native, route::Operation<'static, route::KeyTag<1>>);
    assert!(
        mem::size_of::<Submission<'static, 'static, route::KeyTag<1>>>() == mem::size_of::<Parts>()
    );
    assert!(
        mem::align_of::<Submission<'static, 'static, route::KeyTag<1>>>()
            == mem::align_of::<Parts>()
    );
};

impl<'a, 'd, Tag: route::Tag> Submission<'a, 'd, Tag> {
    fn bind(submission: Native, target: route::Operation<'d, Tag>) -> Self {
        Self {
            submission,
            target,
            borrow: marker::PhantomData,
        }
    }

    pub fn send(
        fd: &'a handles::Descriptor<'d>,
        buffer: &'a [u8],
        target: route::Target<'d, Tag>,
    ) -> IoResult<Self> {
        let operation = operations::Send {
            slot: fd.slot_ref(),
            buffer,
            len: transfer::Len::try_io(buffer.len())?,
        };
        Ok(Self::bind(
            <Native as backend::raw::Lower>::send(backend::raw::Prepared::new(operation)),
            target.operation(kind::SEND),
        ))
    }

    pub fn send_msg(
        fd: &'a handles::Descriptor<'d>,
        message: msg::Message<'a>,
        target: route::Target<'d, Tag>,
    ) -> Self {
        let operation = operations::SendMsg {
            slot: fd.slot_ref(),
            message,
        };
        Self::bind(
            <Native as backend::raw::Lower>::send_msg(backend::raw::Prepared::new(operation)),
            target.operation(kind::SEND),
        )
    }

    pub fn accept_oneshot(
        listener: &'a handles::Descriptor<'d>,
        peer: pin::Pin<&'a mut socket::raw::AcceptAddr>,
        identity: route::Operation<'d, Tag>,
    ) -> Self {
        let operation = operations::AcceptOneshot {
            listener: listener.slot_ref(),
            peer: unsafe { peer.as_addr_mut() },
        };
        Self::bind(
            <Native as backend::raw::Lower>::accept_oneshot(backend::raw::Prepared::new(operation)),
            identity.with_kind(kind::ACCEPT),
        )
    }

    pub(crate) fn into_parts(self) -> (Native, route::Operation<'d, Tag>) {
        (self.submission, self.target)
    }
}

/// A connect request owning its descriptor and borrowing its address through
/// terminal completion.
#[must_use = "connect must be submitted or its exact descriptor is closed"]
pub struct Connect<'addr, 'd, Tag: route::Tag> {
    fd: handles::Descriptor<'d>,
    submission: Native,
    target: route::Target<'d, Tag>,
    borrow: marker::PhantomData<&'addr socket::Addr>,
}

const _: () = assert!(
    mem::size_of::<Connect<'static, 'static, route::KeyTag<1>>>()
        == mem::size_of::<(
            handles::Descriptor<'static>,
            Native,
            route::Target<'static, route::KeyTag<1>>,
        )>()
);

impl<'addr, 'd, Tag: route::Tag> Connect<'addr, 'd, Tag> {
    pub fn new(
        fd: handles::Descriptor<'d>,
        addr: &'addr socket::Addr,
        target: route::Target<'d, Tag>,
    ) -> Self {
        let operation = operations::Connect {
            slot: fd.slot(),
            addr,
        };
        Self {
            fd,
            submission: <Native as backend::raw::Lower>::connect(backend::raw::Prepared::new(
                operation,
            )),
            target,
            borrow: marker::PhantomData,
        }
    }

    pub(crate) fn into_parts(self) -> (handles::Descriptor<'d>, Native, route::Target<'d, Tag>) {
        (self.fd, self.submission, self.target)
    }
}

/// Zero-sized, thread-bound proof that one exact owner remains pinned while
/// retained backend operations may refer to its storage.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct Owner<'owner, 'd: 'owner> {
    /// Covariant owner lifetime.
    _owner: marker::PhantomData<&'owner ()>,
    _driver: marker::PhantomData<fn(&'d mut ()) -> &'d mut ()>,
    _thread: marker::PhantomData<rc::Rc<()>>,
}

impl<'owner, 'd: 'owner> Owner<'owner, 'd> {
    /// # Safety
    /// `owner` remains pinned through retained completion or quiescence.
    pub unsafe fn new<T: ?Sized>(_owner: pin::Pin<&'owner T>) -> Self {
        Self::proof()
    }

    fn proof() -> Self {
        Self {
            _owner: marker::PhantomData,
            _driver: marker::PhantomData,
            _thread: marker::PhantomData,
        }
    }

    /// Submits an operation which retains owner-backed storage.
    /// # Safety
    /// The exact pinned owner retains captured resources through quiescence.
    pub unsafe fn submit<'borrow, Tag: route::Tag>(
        context: &mut retained::Context<'_, 'owner, 'd>,
        slots: &flight::Slots<'d, Tag>,
        submission: Submission<'borrow, 'd, Tag>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError>
    where
        'owner: 'borrow,
    {
        let backend::raw::RetainedSubmission { submission, target } =
            unsafe { backend::raw::RetainedSubmission::bind(context, submission) };
        let submission =
            bound::Bound::reserve_retained(submission, target, slots).ok_or(driver::SubmitError)?;
        context.submit_bound(submission)
    }

    /// Submits an operation supported by one exact native file capability.
    /// # Safety
    /// The exact pinned owner retains captured file resources through quiescence.
    pub unsafe fn submit_file<'borrow, F, Tag>(
        context: &mut retained::Context<'_, 'owner, 'd>,
        slots: &flight::Slots<'d, Tag>,
        submission: fs::Submission<'borrow, 'd, F, Tag>,
    ) -> Result<flight::Flight<'d>, driver::SubmitError>
    where
        'owner: 'borrow,
        F: fs::Mode,
        Tag: route::Tag,
    {
        let submission: fs::raw::Retained<'owner, 'd, Tag, <F as fs::raw::Mode>::Raw> =
            unsafe { fs::raw::Retained::bind(context, submission) };
        let (raw, target) = submission.into_parts();
        let submission =
            bound::Bound::reserve_retained(raw, target, slots).ok_or(driver::SubmitError)?;
        <F as fs::raw::Mode>::submit(context.backend(), submission)
    }

    /// Submits a connect that retains its exact descriptor and address.
    /// # Safety
    /// The pinned owner retains the address through completion or quiescence.
    pub unsafe fn submit_connect<'borrow, Tag: route::Tag>(
        context: &mut retained::Context<'_, 'owner, 'd>,
        slots: &flight::Slots<'d, Tag>,
        options: option::StreamOptions,
        connect: Connect<'borrow, 'd, Tag>,
    ) -> Result<establishment::ConnectionPending<'d>, handles::Descriptor<'d>>
    where
        'owner: 'borrow,
    {
        let connect = unsafe { backend::raw::RetainedConnect::bind(context, connect) };
        backend::Socket::submit_tuned_connect(context.backend(), slots, options, connect)
    }
}

const _: () = assert!(std::mem::size_of::<Owner<'static, 'static>>() == 0);
