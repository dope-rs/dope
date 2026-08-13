use std::{io, os::fd, process};

use dope_core::{driver::route, io::fs};

use crate::file::{
    self, open,
    sealed::{buffer, events, operation},
};

pub(super) struct Opening<F>
where
    F: fs::Mode,
{
    phase: Phase<F>,
}

/// Allocation-free open phase with storage bounded by the operation table.
#[allow(clippy::large_enum_variant)]
enum Phase<F>
where
    F: fs::Mode,
{
    Open(fs::OpenRequest<F>),
    Inspect {
        fd: Option<fd::OwnedFd>,
        output: buffer::Buffer<fs::Metadata<F>>,
    },
}

impl<F> Opening<F>
where
    F: fs::Mode,
{
    pub(super) fn new(path: fs::OpenPath) -> Self {
        Self {
            phase: Phase::Open(path.regular_request::<F>()),
        }
    }

    fn submission<'a, 'd, Tag: route::Tag>(
        &'a mut self,
        target: route::Target<'d, Tag>,
    ) -> fs::Submission<'a, 'd, F, Tag> {
        match &mut self.phase {
            Phase::Open(request) => request.submission(target),
            Phase::Inspect { fd, output } => {
                let Some(fd) = fd.as_ref() else {
                    process::abort();
                };
                fs::Submission::stat_fd(fd::AsFd::as_fd(fd), output.as_uninit_mut(), target)
            }
        }
    }
}

// SAFETY: Opening owns both OpenRequest and the inspection output. The
// operation table keeps the active phase fixed and inaccessible until its
// terminal completion before allowing the phase transition.
unsafe impl<F> operation::Contract for Opening<F>
where
    F: fs::Mode,
{
    type Mode = F;
    type Event = events::Opening;
    type Output = open::Done;
    type Prepared = Self;

    fn prepare(self) -> Result<Self::Prepared, (Self, io::Error)> {
        Ok(self)
    }

    fn submission<'a, 'd, Tag: route::Tag>(
        prepared: &'a mut Self::Prepared,
        target: route::Target<'d, Tag>,
    ) -> io::Result<fs::Submission<'a, 'd, F, Tag>> {
        Ok(prepared.submission(target))
    }

    fn into_hold(prepared: Self::Prepared) -> Self {
        prepared
    }

    fn target<'d, Tag: route::Tag>(
        prepared: &Self::Prepared,
        target: route::Target<'d, Tag>,
    ) -> route::Operation<'d, Tag> {
        use dope_core::driver::route::kind;
        match &prepared.phase {
            Phase::Open(_) => target.operation(kind::OPEN),
            Phase::Inspect { .. } => target.operation(kind::STAT),
        }
    }

    fn complete(
        prepared: &mut Self::Prepared,
        event: Self::Event,
    ) -> operation::Step<Self::Output> {
        use dope_core::io::StatEvent;

        match (&mut prepared.phase, event) {
            (phase @ Phase::Open(_), events::Opening::Opened(opened)) => {
                *phase = Phase::Inspect {
                    fd: Some(opened.into_owned()),
                    output: buffer::Buffer::zeroed(),
                };
                operation::Step::Submit
            }
            (Phase::Open(_), events::Opening::OpenFailed(errno)) => {
                operation::Step::Done(open::Done::Failed(io::Error::from_raw_os_error(errno)))
            }
            (Phase::Inspect { fd, output }, events::Opening::Stat(StatEvent::Done)) => {
                let raw = output.take_initialized();
                let raw = match raw.parse() {
                    Ok(raw) => raw,
                    Err(error) => return operation::Step::Done(open::Done::Failed(error)),
                };
                if !raw.regular {
                    return operation::Step::Done(open::Done::Failed(io::Error::new(
                        io::ErrorKind::NotFound,
                        "dope: confined path is not a regular file",
                    )));
                }
                let Some(fd) = fd.take() else {
                    process::abort();
                };
                let metadata = file::Metadata::from_raw(raw);
                operation::Step::Done(open::Done::Opened(file::Regular::verified(fd, metadata)))
            }
            (Phase::Inspect { .. }, events::Opening::Stat(StatEvent::Failed(errno))) => {
                operation::Step::Done(open::Done::Failed(io::Error::from_raw_os_error(errno)))
            }
            (Phase::Open(_), events::Opening::Stat(_))
            | (
                Phase::Inspect { .. },
                events::Opening::Opened(_) | events::Opening::OpenFailed(_),
            ) => process::abort(),
        }
    }

    fn rejected(_prepared: &mut Self::Prepared, error: io::Error) -> Self::Output {
        open::Done::Failed(error)
    }
}
