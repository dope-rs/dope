use std::{marker, os::fd};

use dope_sys::{
    Errno, engine,
    io::{
        reactor::{complete, complete::Capture as _, ready::delivery::entry},
        socket,
        socket::Prepare,
    },
    table,
};
use o3::mem::Rejected;

use crate::{
    driver,
    driver::{
        life::{
            native,
            native::host::{Files, Trace, poll::files},
        },
        receive::tokens,
        result::poll::fault,
        retained::flight,
        route,
        route::{indices, kind},
    },
    io,
    io::{
        event::{
            batch::Flow,
            net::{accept, recv::Completion},
        },
        fd::{
            domain,
            handles::{Accepted, Admission, Created, OutboundSlot, PhysicalSlot, Reclaimed},
        },
        outputs, recv,
        socket::Create,
        transfer,
    },
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Context<N>(marker::PhantomData<N>);

impl<N: Prepare> entry::Context for Context<N> {
    type Reader = files::Slots;
    type Writer = files::Slots;
    type Outbound = OutboundSlot;
    type Handle = N::Handle;
    type Native = N;
    type Failure = accept::Failure;
    type Reclaimed = Reclaimed;

    const LAST: Flow = Flow::Last;
    const MORE: Flow = Flow::More;
}

impl<N: Prepare> From<entry::Retired<Context<N>>> for native::Report {
    fn from(retired: entry::Retired<Context<N>>) -> Self {
        Self(match retired {
            entry::Retired::Accept { key, result, flow } => io::Reclaimed::Accept {
                echo: fault::Echo::new(key),
                result,
                flow,
            },
            entry::Retired::RecvData { key, len, flow } => io::Reclaimed::Recv {
                echo: fault::Echo::new(key),
                result: Ok(len),
                flow,
            },
            entry::Retired::RecvControl { key, result, flow } => io::Reclaimed::Recv {
                echo: fault::Echo::new(key),
                result: result.map(|()| 0),
                flow,
            },
            entry::Retired::Send { key, result } => io::Reclaimed::Send {
                echo: fault::Echo::new(key),
                result,
            },
            entry::Retired::Connect { key, result } => io::Reclaimed::Connect {
                echo: fault::Echo::new(key),
                result,
            },
            entry::Retired::Create {
                key,
                slot,
                result,
                close,
            } => io::Reclaimed::Socket {
                echo: fault::Echo::new(key),
                slot: slot.physical(),
                result: result.map_err(Create::from_native),
                close,
            },
            entry::Retired::Open { key, result, close } => io::Reclaimed::Open {
                echo: fault::Echo::new(key),
                result,
                close,
            },
            entry::Retired::Write { key, result } => io::Reclaimed::Write {
                echo: fault::Echo::new(key),
                result,
            },
            entry::Retired::Sync { key, result } => io::Reclaimed::Sync {
                echo: fault::Echo::new(key),
                result,
            },
            entry::Retired::Shutdown => io::Reclaimed::Shutdown,
        })
    }
}

impl<'q, 'd, N, B> entry::Effects<'q, 'd, tokens::Token<'d, B>> for Context<N>
where
    B: dope_sys::Buffer + Into<<crate::Engine as engine::Buffer>::Token>,
    N: Prepare + table::Source + complete::Observe<fault::Echo, PhysicalSlot, Flow>,
    Files<N>: table::Files<files::Slots, Handle = N::Handle>,
    Trace<N>: complete::Capture<fault::Echo, PhysicalSlot, Flow>
        + Into<<crate::Engine as complete::Observe>::Observation>,
{
    type Files = Files<N>;
    type Bound = flight::Complete<'q, 'd>;
    type Drain = flight::Drain<'q, 'd>;
    type Driver = driver::Reference<'d>;
    type Output = io::Event<'d>;
    type Error = fault::Fault;
    type Mismatch = flight::Mismatch<'q, 'd>;
    type Socket = flight::Complete<'q, 'd, kind::Socket>;
    type Open = flight::Complete<'q, 'd, kind::Open>;
    type Accept = flight::Complete<'q, 'd, kind::Accept>;
    type Recv = flight::Complete<'q, 'd, kind::Recv>;
    type Send = flight::Complete<'q, 'd, kind::Send>;
    type Connect = flight::Complete<'q, 'd, kind::Connect>;
    type Write = flight::Complete<'q, 'd, kind::Write>;
    type Sync = flight::Complete<'q, 'd, kind::Sync>;
    type Target = route::Erased<'d, kind::Socket>;
    type Creation = domain::Creation;

    const SHUTDOWN: Self::Output = outputs::shutdown();

    fn report_recv(
        key: flight::raw::Echo,
        flow: Flow,
        buffer: &tokens::Token<'d, B>,
    ) -> entry::Retired<Self> {
        entry::Retired::RecvData {
            key,
            len: buffer.len(),
            flow,
        }
    }

    fn accepted(bound: Self::Accept, accepted: Accepted, flow: Flow) -> Self::Output {
        let driver = bound.driver();
        let token = bound.settle(flow);
        outputs::accepted(token, accepted, flow, driver)
    }

    fn accept_error(bound: Self::Accept, failure: accept::Failure, flow: Flow) -> Self::Output {
        let token = bound.settle(flow);
        outputs::accept_error(token, failure, flow)
    }

    fn received(
        bound: Self::Bound,
        driver: Self::Driver,
        key: flight::raw::Echo,
        flow: Flow,
        buffer: tokens::Token<'d, B>,
    ) -> Result<Self::Output, Rejected<tokens::Token<'d, B>, Self::Error>> {
        let bound = match Self::Recv::try_from(bound) {
            Ok(bound) => bound,
            Err(cause) => {
                return Err(Rejected::new(
                    buffer,
                    <Self as entry::Effects<'q, 'd, tokens::Token<'d, B>>>::mismatched(key, cause),
                ));
            }
        };
        let vacant = match tokens::Vacant::try_new(driver) {
            Ok(vacant) => vacant,
            Err(full) => {
                let len = buffer.len();
                return Err(Rejected::new(
                    buffer,
                    fault::Fault {
                        observation: fault::Observation::new(
                            Trace::<N>::receive(fault::Echo::new(key), len, flow).into(),
                        ),
                        subject: fault::Subject::Target(bound.complete().into_token()),
                        violation: fault::Violation::Provided(full),
                    },
                ));
            }
        };
        let token = bound.settle(flow);
        Ok(outputs::selected(
            token,
            flow,
            recv::Lease::new(buffer.bind(vacant)),
        ))
    }

    fn control(bound: Self::Recv, result: Result<(), Errno>, flow: Flow) -> Self::Output {
        let driver = bound.driver();
        let token = bound.settle(flow);
        Completion::control(token, result, flow, driver).into()
    }

    fn sent(bound: Self::Send, result: Result<transfer::Len, Errno>) -> Self::Output {
        let token = bound.complete();
        outputs::sent(token, result)
    }

    fn connected(bound: Self::Connect, result: Result<(), Errno>) -> Self::Output {
        let token = bound.complete();
        outputs::connected(token, result)
    }

    fn opened(bound: Self::Open, result: Result<fd::OwnedFd, Errno>) -> Self::Output {
        let token = bound.complete();
        match result {
            Ok(fd) => outputs::opened(token, fd),
            Err(errno) => outputs::open_failed(token, errno),
        }
    }

    fn written(bound: Self::Write, result: Result<transfer::Len, Errno>) -> Self::Output {
        let token = bound.complete();
        outputs::written(token, result)
    }

    fn synced(bound: Self::Sync, result: Result<(), Errno>) -> Self::Output {
        let token = bound.complete();
        outputs::synced(token, result)
    }

    #[cold]
    fn mismatched(key: flight::raw::Echo, cause: Self::Mismatch) -> Self::Error {
        let flight::Mismatch {
            bound,
            expected,
            observed,
        } = cause;
        fault::Fault {
            observation: fault::Observation::new(
                Trace::<N>::operation(fault::Echo::new(key)).into(),
            ),
            subject: fault::Subject::Target(bound.complete().into_token()),
            violation: fault::Violation::Operation { expected, observed },
        }
    }

    fn abandon(driver: Self::Driver, accepted: Accepted) -> Reclaimed {
        driver.descriptors().abandon(accepted)
    }
}

impl<'q, 'd, N, B> entry::Socket<'q, 'd, tokens::Token<'d, B>> for Context<N>
where
    B: dope_sys::Buffer + Into<<crate::Engine as engine::Buffer>::Token>,
    N: Prepare + table::Source + complete::Observe<fault::Echo, PhysicalSlot, Flow>,
    Files<N>: table::Files<files::Slots, Handle = N::Handle>,
    Trace<N>: complete::Capture<fault::Echo, PhysicalSlot, Flow>
        + Into<<crate::Engine as complete::Observe>::Observation>,
{
    fn creation(driver: Self::Driver, target: Self::Target) -> Option<Self::Creation> {
        driver.outbound().creation(target)
    }

    fn reject(
        key: flight::raw::Echo,
        queued: PhysicalSlot,
        resolved: Option<PhysicalSlot>,
        target: Self::Target,
    ) -> Self::Error {
        fault::Fault {
            observation: fault::Observation::new(
                Trace::<N>::creation(fault::Echo::new(key), queued, resolved).into(),
            ),
            subject: fault::Subject::Target(target.into_token()),
            violation: fault::Violation::Slot,
        }
    }

    fn bad_install(
        key: flight::raw::Echo,
        slot: PhysicalSlot,
        target: Self::Target,
        cause: table::Install<indices::SlotIndex>,
    ) -> Self::Error {
        fault::Fault {
            observation: fault::Observation::new(
                Trace::<N>::creation(fault::Echo::new(key), slot, Some(slot)).into(),
            ),
            subject: fault::Subject::Target(target.into_token()),
            violation: fault::Violation::Install(fault::Install::from_native(cause)),
        }
    }

    fn created(
        driver: Self::Driver,
        target: Self::Target,
        creation: Self::Creation,
    ) -> Self::Output {
        match Created::complete(creation, driver) {
            Admission::Ready(created) => outputs::socket(target, created, driver),
            Admission::Retired(()) => outputs::socket_gone(target),
        }
    }

    fn failed(
        driver: Self::Driver,
        target: Self::Target,
        creation: Self::Creation,
        error: socket::Create,
    ) -> Self::Output {
        driver.outbound().fail_create(creation);
        outputs::socket_error(target, Create::from_native(error))
    }

    fn fail_create(driver: Self::Driver, creation: Self::Creation) {
        driver.outbound().fail_create(creation);
    }

    fn complete(driver: Self::Driver, creation: Self::Creation) -> Option<OutboundSlot> {
        let slot = *creation.as_ref();
        match driver.outbound().complete(creation) {
            domain::Create::Deliver => Some(slot),
            domain::Create::Retired => None,
        }
    }

    fn discard(driver: Self::Driver, slot: OutboundSlot) {
        driver.descriptors().discard(slot);
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Udata {
    Public(flight::raw::Echo),
    Shutdown,
}

impl From<Udata> for usize {
    fn from(data: Udata) -> Self {
        match data {
            Udata::Public(key) => u64::from(key) as usize,
            Udata::Shutdown => usize::MAX,
        }
    }
}

impl From<flight::raw::Echo> for Udata {
    fn from(key: flight::raw::Echo) -> Self {
        Self::Public(key)
    }
}
