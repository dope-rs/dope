use dope_sys::{
    engine::{Complete, life::Resolver as _},
    io::reactor::{
        complete::{self, resolve::Reclaimed},
        ready::{Backend as _, delivery::entry, store::State as _},
    },
    probe::Stopped,
};

use crate::{
    Backend, Engine, Events,
    driver::{
        life::native::{self, host::poll::events, resolver::Resolver},
        receive::tokens,
        result::poll::fault,
        retained::flight,
    },
    io,
};

type Buffer<'d> = tokens::Token<'d, <Engine as Complete>::Pending>;
type Rejected<'d> = Stopped<Option<Buffer<'d>>, fault::Fault>;

pub(crate) fn resolve<'d, S>(
    root: &mut Resolver<'_, 'd, S>,
    entry: tokens::Entry<'d>,
) -> Result<Option<io::Event<'d>>, Rejected<'d>> {
    let local = &mut root.local;
    let drain = flight::Drain::new(local.flights, local.driver);
    let done = match entry.decode::<Events, _>() {
        complete::Dequeued::Emit(done) => {
            match entry::Event::<events::Context<Backend>, _>::resolve(
                done,
                local.backend.state_mut().edit().files,
                &drain,
            ) {
                entry::Resolved::Public(done) => {
                    return Ok(Some(done));
                }
                entry::Resolved::Rejected(done) => {
                    return reclaimed(root, done.map_report(native::Report::from)).map(|()| None);
                }
                entry::Resolved::Reclaim(done) => done,
            }
        }
        complete::Dequeued::Reclaim(done) => done,
    };
    let done = entry::Event::<events::Context<Backend>, _>::reclaim(done, &drain);
    reclaimed(root, done.map_report(native::Report::from)).map(|()| None)
}

pub(crate) fn reclaim<'d, S>(
    root: &mut Resolver<'_, 'd, S>,
    entry: tokens::Entry<'d>,
) -> Result<(), Rejected<'d>> {
    let drain = flight::Drain::new(root.local.flights, root.local.driver);
    let (complete::Dequeued::Emit(done) | complete::Dequeued::Reclaim(done)) =
        entry.decode::<Events, _>();
    let done = entry::Event::<events::Context<Backend>, _>::reclaim(done, &drain);
    reclaimed(root, done.map_report(native::Report::from))
}

fn reclaimed<'d, S>(
    root: &mut Resolver<'_, 'd, S>,
    done: Reclaimed<Buffer<'d>, native::Report, fault::Fault>,
) -> Result<(), Rejected<'d>> {
    let Reclaimed {
        buffer,
        report,
        result,
    } = done;
    let result = Stopped::attach(buffer, result).map(|(buffer, ())| {
        if let Some(buffer) = buffer {
            buffer.release(root.local);
        }
    });
    root.retire();
    if let Some(report) = report {
        root.local.lifecycle.report(report.0);
    }
    result
}
