use dope_sys::{
    io::socket::option::{
        tuning,
        tuning::{Occupied as _, Table as _},
    },
    probe::Stopped,
    table::token::{Tuning as _, host::Flight as _},
};

use crate::{
    Errno,
    driver::{
        result::poll::fault,
        retained::flight,
        route::{self, kind, kind::Shape},
    },
    io,
    io::{
        event::{Kind, batch::Flow, net::connect},
        outputs,
    },
    ring::cqe::{Envelope, Occupied, Rejected, Tune, Tuning},
};

pub(crate) struct Tuned<'state, 'q, 'd> {
    state: Occupied<'state>,
    flight: Terminal<'q, 'd>,
}

enum Terminal<'q, 'd> {
    Connect(flight::Complete<'q, 'd, kind::Connect>),
    Tuning(flight::Complete<'q, 'd, kind::Tuning>),
}

impl<'state, 'q, 'd> Tuned<'state, 'q, 'd> {
    pub(super) fn complete(self, outcome: Result<(), Errno>) -> io::Event<'d> {
        let Self { state, flight } = self;
        let option_failed = state.option_failed();
        state.clear();
        match flight {
            Terminal::Connect(bound) => outputs::connected(
                bound.complete(),
                outcome.map_err(|errno| classify(errno, option_failed)),
            ),
            Terminal::Tuning(bound) => Kind::Tuning((bound, outcome).into()).into(),
        }
    }

    pub(super) fn reclaim(self, result: Result<(), Errno>) -> io::Reclaimed {
        let Self { state, flight } = self;
        let option_failed = state.option_failed();
        state.clear();
        match flight {
            Terminal::Connect(bound) => io::Reclaimed::Connect {
                echo: fault::Echo::new(bound.end(Flow::Last)),
                result: result.map_err(|errno| classify(errno, option_failed)),
            },
            Terminal::Tuning(bound) => io::Reclaimed::Tuning {
                echo: fault::Echo::new(bound.end(Flow::Last)),
                result,
            },
        }
    }
    pub(super) fn bind(
        states: &'state mut Tuning,
        drain: &'q flight::Drain<'q, 'd>,
        txn: Tune,
        packet: Envelope<'d>,
        control: route::Control,
    ) -> Result<(Self, Result<(), Errno>), Rejected<'d>> {
        use dope_sys::table::token::host::Drain as _;
        let outcome = packet.unit();
        let ((packet, _), outcome) =
            Stopped::attach((packet, fault::Subject::Control(control)), outcome)?;
        let state = match states.bind(txn.slot().raw()) {
            Ok(state) => state,
            Err(missing) => {
                let cause = match missing {
                    tuning::Missing::Slot => fault::Violation::Slot,
                    tuning::Missing::Vacant => fault::Violation::Control,
                };
                return Err(packet.reject(fault::Subject::Control(control), cause));
            }
        };
        let target = state.target();
        let Some(flight) = drain.bind(target) else {
            state.clear();
            let echo = fault::Echo::new(target);
            return Err(packet.reject(
                fault::Subject::Tuning { echo, control },
                fault::Violation::Stale(echo),
            ));
        };
        let observed = flight.kind();
        let flight = match observed.into() {
            Shape::Connect(proof) => Terminal::Connect(flight.bind(proof)),
            Shape::Tuning(proof) => Terminal::Tuning(flight.bind(proof)),
            _ => {
                state.clear();
                let echo = fault::Echo::new(flight.end(Flow::Last));
                return Err(packet.reject(
                    fault::Subject::Tuning { echo, control },
                    fault::Violation::Tuning(observed),
                ));
            }
        };
        Ok((Tuned { state, flight }, outcome))
    }
}

fn classify(errno: Errno, option_failed: bool) -> connect::Error {
    if option_failed {
        connect::Error::Option(errno)
    } else {
        connect::Error::Connect(errno)
    }
}
