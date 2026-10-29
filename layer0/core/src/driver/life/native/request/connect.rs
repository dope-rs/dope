use dope_sys::io::{
    request::{Lower, Source},
    socket::option::tuning::device,
};
use o3::depot::arena::slab::Vacancy;

use crate::{
    Backend,
    driver::{
        life::{native, native::request},
        retained::flight,
        route,
        route::{indices, indices::SlotIndex, kind},
    },
    io::{
        fd::handles,
        socket::{option, pending},
    },
};

type Terminal<'a> = <<Backend as Source<request::Context>>::Raw as Lower>::Borrowed<'a>;

/// Retains the affine descriptor, captured address and exact typed route until submission.
pub(crate) struct Connect<'a, 'borrow, 'owner: 'borrow, 'd: 'owner, Tag: route::Tag> {
    native: native::Access<'a, 'd>,
    slots: &'a flight::Slots<'d, Tag>,
    fd: handles::Descriptor<'d>,
    opts: &'a option::Set,
    request: request::raw::Connect<'borrow, 'owner, 'd, Tag>,
}

impl<'a, 'borrow, 'owner: 'borrow, 'd: 'owner, Tag: route::Tag>
    Connect<'a, 'borrow, 'owner, 'd, Tag>
{
    pub(crate) fn new(
        native: native::Access<'a, 'd>,
        slots: &'a flight::Slots<'d, Tag>,
        fd: handles::Descriptor<'d>,
        opts: &'a option::Set,
        request: request::raw::Connect<'borrow, 'owner, 'd, Tag>,
    ) -> Self {
        Self {
            native,
            slots,
            fd,
            opts,
            request,
        }
    }
}

// SAFETY: the invariant descriptor and native Local borrow share one driver and
// immutable FileSlots extent. Domain checked the index at first acquisition;
// both targets below come from that descriptor. A ring backend creates its
// pinned tuning table with that same extent. There pending::Connection holds
// the fd until cqe::Tuned::complete clears its state before completion. Drop
// leaves Domain retirement queued until Engine::retire observes that terminal.
// Only then can close recycle the slot. Readiness options settle synchronously. Captured
// retains this terminal's address and Local the backend until completion or
// quiescence.
unsafe impl<'a, 'borrow, 'owner, 'd, Tag, B>
    device::raw::Connect<B, Terminal<'borrow>, { option::MAX_STREAM_OPTIONS }>
    for Connect<'a, 'borrow, 'owner, 'd, Tag>
where
    Tag: route::Tag + route::Select<kind::Connect>,
    'owner: 'borrow,
    'd: 'owner,
    native::Access<'a, 'd>: AsMut<B>,
{
    type File = handles::Descriptor<'d>;
    type Slot = <<Backend as Source<request::Context>>::Raw as Lower>::Target;
    type Index = indices::SlotIndex;
    type Reserve = flight::Request<'a, 'd, Tag, kind::Connect>;
    type Options<'o> = option::NonEmpty<'o>;
    type Output = pending::Connection<'d>;
    type Error = pending::Rejection<'d>;

    fn slot(file: &Self::File) -> Self::Slot {
        <Backend as Source<request::Context>>::target(file.slot())
    }

    fn target(file: &Self::File) -> (Self::Slot, Self::Index) {
        let slot = file.slot();
        (
            <Backend as Source<request::Context>>::target(slot),
            SlotIndex::from(slot),
        )
    }

    fn with(
        self,
        run: impl for<'o> FnOnce(
            &mut B,
            Self::File,
            Option<Self::Options<'o>>,
            Self::Reserve,
            Terminal<'borrow>,
        ) -> Result<
            (Self::File, flight::Reserved<'d, kind::Connect>),
            device::Failure<Self::File, Vacancy>,
        >,
    ) -> Result<Self::Output, Self::Error> {
        let Self {
            mut native,
            slots,
            fd,
            opts,
            request,
        } = self;
        let (last, target) = request.into_parts();
        let reserve = flight::Request::new(slots, target.operation::<kind::Connect>());
        let opts = opts.non_empty();
        let last = last.into_inner().into_inner();
        let result = run(native.as_mut(), fd, opts, reserve, last);
        let (fd, target) = native.submitted(result)?;
        Ok(pending::Connection::connect(fd, target.commit()))
    }
}
