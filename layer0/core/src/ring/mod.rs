use dope_sys::{
    engine::{Complete, ring::Backend as _},
    io::reactor::complete::resolve::Reclaimed,
    probe::Stopped,
};

mod cqe;
use cqe::Envelope;

use crate::{
    Engine, Events,
    driver::{
        life::native::resolver::Resolver, receive::tokens, result::poll::fault, retained::flight,
    },
    io,
};

type Rejected<'d> = Stopped<Option<tokens::Token<'d, <Engine as Complete>::Pending>>, fault::Fault>;

pub(crate) fn resolve<'d, S>(
    root: &mut Resolver<'_, 'd, S>,
    entry: tokens::Entry<'d>,
) -> Result<Option<io::Event<'d>>, Rejected<'d>> {
    let local = &mut root.local;
    let drain = flight::Drain::new(local.flights, local.driver);
    Envelope::resolve(local.backend.tuning(), &drain, entry.decode::<Events, _>())
        .map_err(Rejected::from)
}

pub(crate) fn reclaim<'d, S>(
    root: &mut Resolver<'_, 'd, S>,
    entry: tokens::Entry<'d>,
) -> Result<(), Rejected<'d>> {
    let local = &mut root.local;
    let drain = flight::Drain::new(local.flights, local.driver);
    let Reclaimed {
        buffer,
        report,
        result,
    } = Envelope::reclaim(
        local.backend.tuning(),
        &drain,
        entry.decode::<Events, _>(),
        local.lifecycle.source.is_some(),
    )?;
    if let Some(buffer) = buffer {
        buffer.release(local);
    }
    if let Some(report) = report {
        local.lifecycle.report(report);
    }
    result.map_err(Rejected::from)
}
