use dope_sys::{
    io::socket::option::tuning,
    probe::Stopped,
    table::token::{self, host::Flight as _},
};

use crate::{
    Errno,
    driver::{
        life::native,
        result::poll::fault,
        retained::flight,
        route::{indices, kind, kind::Shape},
    },
    io,
    io::{
        event::{Kind, batch::Flow},
        outputs,
        socket::option,
    },
};

pub(crate) struct Tuned<'q, 'd, S> {
    state: S,
    flight: Terminal<'q, 'd>,
}

enum Terminal<'q, 'd> {
    Connect(flight::Complete<'q, 'd, kind::Connect>),
    Tuning(flight::Complete<'q, 'd, kind::Tuning>),
}

pub(crate) enum Failure<'q, 'd, S> {
    Lookup(tuning::Missing),
    Stale(Stopped<S, flight::raw::Echo>),
    Class(Stopped<(S, flight::Complete<'q, 'd>), kind::Native>),
}

impl<'q, 'd, S: tuning::Occupied<flight::raw::Echo>> Tuned<'q, 'd, S> {
    pub(super) fn complete(self, outcome: Result<(), Errno>) -> io::Event<'d> {
        let Self { state, flight } = self;
        state.clear();
        match flight {
            Terminal::Connect(bound) => outputs::connected(bound.complete(), outcome),
            Terminal::Tuning(bound) => Kind::Tuning((bound, outcome).into()).into(),
        }
    }

    pub(super) fn reclaim(self, result: Result<(), Errno>) -> native::Report {
        let Self { state, flight } = self;
        state.clear();
        native::Report(match flight {
            Terminal::Connect(bound) => io::Reclaimed::Connect {
                echo: fault::Echo::new(bound.end(Flow::Last)),
                result,
            },
            Terminal::Tuning(bound) => io::Reclaimed::Tuning {
                echo: fault::Echo::new(bound.end(Flow::Last)),
                result,
            },
        })
    }
    pub(super) fn bind<'state, T, C>(
        states: &'state mut T,
        drain: &'q flight::Drain<'q, 'd>,
        txn: C,
    ) -> Result<Self, Failure<'q, 'd, S>>
    where
        T: tuning::Table<flight::raw::Echo, { option::MAX_STREAM_OPTIONS }, Occupied<'state> = S>
            + 'state,
        C: token::Tuning<Slot = indices::SlotIndex>,
    {
        let state = states.bind(txn.slot().raw()).map_err(Failure::Lookup)?;
        let target = state.target();
        use dope_sys::table::token::host::Drain as _;
        let (state, flight) =
            Stopped::attach(state, drain.bind(target).ok_or(target)).map_err(Failure::Stale)?;
        let observed = flight.kind();
        let flight = match observed.into() {
            Shape::Connect(proof) => Terminal::Connect(flight.bind(proof)),
            Shape::Tuning(proof) => Terminal::Tuning(flight.bind(proof)),
            _ => {
                return Err(Failure::Class(Stopped {
                    state: (state, flight),
                    cause: observed,
                }));
            }
        };
        Ok(Tuned { state, flight })
    }
}
