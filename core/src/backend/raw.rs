use std::mem;

use crate::{
    backend::{self, operations},
    driver::{retained, route},
    io::{fd::handles, socket},
};

type Submission = backend::RawSubmission;

pub(crate) struct Retainer<'borrow, 'context, 'owner, 'd: 'owner> {
    _context: &'borrow retained::Context<'context, 'owner, 'd>,
}

impl<'borrow, 'context, 'owner, 'd: 'owner> Retainer<'borrow, 'context, 'owner, 'd> {
    pub(crate) fn new(context: &'borrow retained::Context<'context, 'owner, 'd>) -> Self {
        Self { _context: context }
    }

    /// # Safety
    /// Every resource in `raw` must remain live until completion or quiescence.
    pub(crate) unsafe fn bind<R>(self, raw: R) -> backend::Captured<'owner, R> {
        backend::Captured::scoped(raw)
    }
}

#[repr(transparent)]
pub(crate) struct Prepared<T>(T);

impl<T> Prepared<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self(inner)
    }

    pub(super) fn into_inner(self) -> T {
        self.0
    }
}

impl<'a> Prepared<operations::Socket<'a>> {
    pub(crate) fn lower(slot: &'a handles::FixedSlot, socket: socket::StreamSpec) -> Submission {
        let operation = operations::Socket { slot, socket };
        <Submission as Lower>::socket(Self::new(operation))
    }
}

pub(crate) trait Lower {
    fn socket(op: Prepared<operations::Socket<'_>>) -> Submission;
    fn send(op: Prepared<operations::Send<'_>>) -> Submission;
    fn send_msg(op: Prepared<operations::SendMsg<'_>>) -> Submission;
    fn accept_oneshot(op: Prepared<operations::AcceptOneshot<'_>>) -> Submission;
    fn accept_multishot(op: Prepared<operations::AcceptMultishot<'_>>) -> Submission;
    fn recv(op: Prepared<operations::Recv<'_>>) -> Submission;
    fn recv_message(op: Prepared<operations::RecvMsgMulti<'_>>) -> Submission;
    fn connect(op: Prepared<operations::Connect<'_>>) -> Submission;
}

/// A submission whose borrowed resources are retained through terminal
/// completion or quiescence.
pub(crate) struct RetainedSubmission<'owner, 'd: 'owner, Tag: route::Tag> {
    pub(crate) submission: backend::Captured<'owner, Submission>,
    pub(crate) target: route::Operation<'d, Tag>,
}

const _: () = {
    assert!(
        mem::size_of::<RetainedSubmission<'static, 'static, route::KeyTag<1>>>()
            == mem::size_of::<retained::raw::Submission<'static, 'static, route::KeyTag<1>>>()
    );
    assert!(
        mem::align_of::<RetainedSubmission<'static, 'static, route::KeyTag<1>>>()
            == mem::align_of::<retained::raw::Submission<'static, 'static, route::KeyTag<1>>>()
    );
    assert!(
        mem::size_of::<backend::Captured<'static, Submission>>() == mem::size_of::<Submission>()
    );
};

impl<'owner, 'd: 'owner, Tag: route::Tag> RetainedSubmission<'owner, 'd, Tag> {
    /// # Safety
    /// Captured storage belongs to `context` through completion or quiescence.
    pub(crate) unsafe fn bind(
        context: &retained::Context<'_, 'owner, 'd>,
        submission: retained::raw::Submission<'_, 'd, Tag>,
    ) -> Self {
        let (submission, target) = submission.into_parts();
        Self {
            submission: unsafe { Retainer::new(context).bind(submission) },
            target,
        }
    }
}

/// A retained connect, distinct from unrelated raw submissions accepted by a
/// tuning transaction.
pub(crate) struct RetainedConnect<'owner, 'd: 'owner, Tag: route::Tag> {
    fd: handles::Descriptor<'d>,
    submission: backend::Captured<'owner, Submission>,
    target: route::Target<'d, Tag>,
}

const _: () = assert!(
    mem::size_of::<RetainedConnect<'static, 'static, route::KeyTag<1>>>()
        == mem::size_of::<(
            handles::Descriptor<'static>,
            backend::Captured<'static, Submission>,
            route::Target<'static, route::KeyTag<1>>,
        )>()
);

impl<'owner, 'd: 'owner, Tag: route::Tag> RetainedConnect<'owner, 'd, Tag> {
    /// # Safety
    /// Captured address storage belongs to `context` through completion or
    /// quiescence. The exact descriptor is retained structurally.
    pub(crate) unsafe fn bind(
        context: &retained::Context<'_, 'owner, 'd>,
        connect: retained::raw::Connect<'_, 'd, Tag>,
    ) -> Self {
        let (fd, submission, target) = connect.into_parts();
        Self {
            fd,
            submission: unsafe { Retainer::new(context).bind(submission) },
            target,
        }
    }

    pub(in crate::backend) fn into_parts(
        self,
    ) -> (
        handles::Descriptor<'d>,
        backend::Captured<'owner, Submission>,
        route::Target<'d, Tag>,
    ) {
        (self.fd, self.submission, self.target)
    }
}
