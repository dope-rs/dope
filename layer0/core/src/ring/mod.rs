use dope_sys::{
    engine::{Complete, ring::Backend as _},
    io::reactor::complete::resolve::Reclaimed,
    memory::buffer::provided,
    probe::Stopped,
    ring,
    table::token,
};

mod cqe;
use cqe::Envelope;

use crate::{
    Backend, Engine, Events,
    driver::{
        life::native::{host::context::Context, resolver::Resolver},
        receive::tokens,
        result::poll::fault,
        retained::flight,
    },
    io::{self, transfer},
};

type Rejected<'d> = Stopped<Option<tokens::Token<'d, <Engine as Complete>::Pending>>, fault::Fault>;

type Packet<'d> = <Backend as ring::Source>::Packet<
    <Backend as token::Source>::Token<Context>,
    tokens::Token<'d, <Backend as provided::Source>::Pending<transfer::Extent>>,
>;

pub(crate) fn resolve<'d, S>(
    root: &mut Resolver<'_, 'd, S>,
    entry: tokens::Entry<'d>,
) -> Result<Option<io::Event<'d>>, Rejected<'d>> {
    let local = &mut root.local;
    let drain = flight::Drain::new(local.flights, local.driver);
    Envelope::<Packet<'d>>::resolve(local.backend.tuning(), &drain, entry.decode::<Events, _>())
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
    } = Envelope::<Packet<'d>>::reclaim(
        local.backend.tuning(),
        &drain,
        entry.decode::<Events, _>(),
        local.lifecycle.source.is_some(),
    )?;
    if let Some(buffer) = buffer {
        buffer.release(local);
    }
    if let Some(report) = report {
        local.lifecycle.report(report.0);
    }
    result.map_err(Rejected::from)
}
