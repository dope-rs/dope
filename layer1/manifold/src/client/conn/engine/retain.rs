use dope_core::driver::route::{Identity, table::conn::Capacity};
use dope_net::wire::domain;

use crate::{
    app::{creation, receive},
    client::app::{self, resume},
};

/// Prepares receive retention for an exact input and continuation pair.
pub trait Policy<'w, 'd: 'w, ID: Identity, A>
where
    A: app::Receive<'d, ID>,
{
    fn prepare(
        app: &A,
        capacity: Capacity,
    ) -> Result<(domain::Retained, domain::Resident), creation::Error>;
}

impl<'w, 'd: 'w, ID: Identity, A, C> Policy<'w, 'd, ID, A> for (receive::Borrowed, C)
where
    A: app::Borrowed<'d, ID, Continuation = C>,
    C: resume::Mode<Retention = resume::Absent>,
{
    fn prepare(
        _: &A,
        _: Capacity,
    ) -> Result<(domain::Retained, domain::Resident), creation::Error> {
        Ok((domain::Retained::ZERO, domain::Resident::ZERO))
    }
}

impl<'w, 'd: 'w, ID: Identity, A, R: Default> Policy<'w, 'd, ID, A>
    for (receive::Owned<R>, resume::Complete)
where
    A: app::Owned<'w, 'd, ID, R, Continuation = resume::Complete>,
{
    fn prepare(
        app: &A,
        capacity: Capacity,
    ) -> Result<(domain::Retained, domain::Resident), creation::Error> {
        let retain = app
            .retention()
            .capacity(capacity)
            .map_err(creation::Error::Config)?;
        Ok((retain, domain::Resident::ZERO))
    }
}

impl<'w, 'd: 'w, ID: Identity, A, R: Default> Policy<'w, 'd, ID, A>
    for (receive::Owned<R>, resume::Retained)
where
    A: app::Owned<'w, 'd, ID, R, Continuation = resume::Retained>
        + app::Resumable<'d, ID, Continuation = resume::Retained>,
{
    fn prepare(
        app: &A,
        capacity: Capacity,
    ) -> Result<(domain::Retained, domain::Resident), creation::Error> {
        let retain = app
            .retention()
            .capacity(capacity)
            .map_err(creation::Error::Config)?;
        let resident = app::Resumable::resident(app);
        Ok((retain, resident))
    }
}
