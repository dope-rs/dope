use dope_core::driver::route::{Identity, table::conn::Capacity};
use dope_net::{Transport, wire::domain};

use crate::{
    app::{creation, receive},
    client::{
        app::{self, resume},
        attempt,
    },
};

/// Prepares receive retention for an exact input and continuation pair.
pub trait Policy<'w, 'd: 'w, ID: Identity, A>
where
    A: app::Receive<'d, ID>,
{
    fn prepare<S, T>(
        app: A,
        control: S,
        capacity: Capacity,
    ) -> Result<(A, S, domain::Retained, domain::Resident), creation::Error>
    where
        S: attempt::Control<'d, T, ID>,
        T: Transport;
}

impl<'w, 'd: 'w, ID: Identity, A, C> Policy<'w, 'd, ID, A> for (receive::Borrowed, C)
where
    A: app::Borrowed<'d, ID, Continuation = C>,
    C: resume::Mode<Retention = resume::Absent>,
{
    fn prepare<S, T>(
        app: A,
        mut control: S,
        capacity: Capacity,
    ) -> Result<(A, S, domain::Retained, domain::Resident), creation::Error>
    where
        S: attempt::Control<'d, T, ID>,
        T: Transport,
    {
        attempt::Control::resize(&mut control, capacity.scale::<1>())
            .map_err(creation::Error::Attempt)?;
        Ok((app, control, domain::Retained::ZERO, domain::Resident::ZERO))
    }
}

impl<'w, 'd: 'w, ID: Identity, A> Policy<'w, 'd, ID, A> for (receive::Owned, resume::Complete)
where
    A: app::Owned<'w, 'd, ID, Continuation = resume::Complete>,
{
    fn prepare<S, T>(
        app: A,
        mut control: S,
        capacity: Capacity,
    ) -> Result<(A, S, domain::Retained, domain::Resident), creation::Error>
    where
        S: attempt::Control<'d, T, ID>,
        T: Transport,
    {
        let retain = app
            .retention()
            .capacity(capacity)
            .map_err(creation::Error::Config)?;
        attempt::Control::resize(&mut control, capacity.scale::<1>())
            .map_err(creation::Error::Attempt)?;
        Ok((app, control, retain, domain::Resident::ZERO))
    }
}

impl<'w, 'd: 'w, ID: Identity, A> Policy<'w, 'd, ID, A> for (receive::Owned, resume::Retained)
where
    A: app::Owned<'w, 'd, ID, Continuation = resume::Retained>
        + app::Resumable<'d, ID, Continuation = resume::Retained>,
{
    fn prepare<S, T>(
        app: A,
        mut control: S,
        capacity: Capacity,
    ) -> Result<(A, S, domain::Retained, domain::Resident), creation::Error>
    where
        S: attempt::Control<'d, T, ID>,
        T: Transport,
    {
        let retain = app
            .retention()
            .capacity(capacity)
            .map_err(creation::Error::Config)?;
        let resident = app::Resumable::resident(&app);
        attempt::Control::resize(&mut control, capacity.scale::<1>())
            .map_err(creation::Error::Attempt)?;
        Ok((app, control, retain, resident))
    }
}
