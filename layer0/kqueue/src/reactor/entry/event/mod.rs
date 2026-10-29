mod deliver;
use std::os::fd;

use deliver::try_deliver;
use dope_sys::{
    Errno,
    io::{
        fd::Fd as _,
        reactor::{
            complete::{self, resolve},
            read,
            ready::delivery::entry,
            retry::{self, Context as _},
        },
        socket::{self, Prepare as _, accept},
    },
    table::token::host::{Drain as _, Flight as _},
};

use crate::{
    Kqueue,
    reactor::entry::{Accepted, Bytes, Flow, Key, Unit, create::Create},
};

#[repr(transparent)]
pub struct Event<C: entry::Context, B>(Option<Keyed<C, B>>);

struct Keyed<C: entry::Context, B> {
    ud: Key<C>,
    state: State<C, B>,
}

impl<C: entry::Context, B> Event<C, B> {
    fn new(ud: Key<C>, state: State<C, B>) -> Self {
        Self(Some(Keyed { ud, state }))
    }
}

enum State<C: entry::Context, B> {
    AcceptSuccess {
        accepted: Accepted<C>,
        flow: Flow<C>,
    },
    AcceptFailure {
        failure: C::Failure,
        flow: Flow<C>,
    },
    RecvData {
        flow: Flow<C>,
        buffer: B,
    },
    RecvControl {
        result: Unit<C>,
        flow: Flow<C>,
    },
    Send {
        result: Bytes<C>,
    },
    Connect {
        result: Unit<C>,
    },
    Create {
        outcome: Create<C>,
    },
    Open {
        result: Result<fd::OwnedFd, Errno>,
    },
    Write {
        result: Bytes<C>,
    },
    Sync {
        result: Unit<C>,
    },
}

impl<C: entry::Context, B> complete::Event for Event<C, B> {
    type Key = Key<C>;

    fn token(&self) -> Option<Self::Key> {
        self.0.as_ref().map(|entry| entry.ud)
    }
    fn is_terminal(&self) -> bool {
        self.0.as_ref().is_some_and(|entry| match &entry.state {
            State::AcceptSuccess { flow, .. }
            | State::AcceptFailure { flow, .. }
            | State::RecvData { flow, .. }
            | State::RecvControl { flow, .. } => *flow == C::LAST,
            State::Send { .. }
            | State::Connect { .. }
            | State::Create { .. }
            | State::Open { .. }
            | State::Write { .. }
            | State::Sync { .. } => true,
        })
    }
}

impl<C: entry::Context, B> complete::Map for Event<C, B> {
    type Entry<T> = Event<C, T>;

    fn map<T, U>(entry: Self::Entry<T>, map: impl FnOnce(T) -> U) -> Self::Entry<U> {
        Event(entry.0.map(|Keyed { ud, state }| Keyed {
            ud,
            state: match state {
                State::AcceptSuccess { accepted, flow } => State::AcceptSuccess { accepted, flow },
                State::AcceptFailure { failure, flow } => State::AcceptFailure { failure, flow },
                State::RecvData { flow, buffer } => State::RecvData {
                    flow,
                    buffer: map(buffer),
                },
                State::RecvControl { result, flow } => State::RecvControl { result, flow },
                State::Send { result } => State::Send { result },
                State::Connect { result } => State::Connect { result },
                State::Create { outcome } => State::Create { outcome },
                State::Open { result } => State::Open { result },
                State::Write { result } => State::Write { result },
                State::Sync { result } => State::Sync { result },
            },
        }))
    }
}

impl<C: entry::Context, B: dope_sys::Buffer> read::Complete for Event<C, B> {
    type Accepted = Accepted<C>;
    type Buffer = B;
    type Flow = Flow<C>;

    const LAST: Self::Flow = C::LAST;
    const MORE: Self::Flow = C::MORE;

    fn accepted(ud: Key<C>, accepted: Self::Accepted, flow: Flow<C>) -> Self {
        Self::new(ud, State::AcceptSuccess { accepted, flow })
    }

    fn rejected(ud: Key<C>, cause: accept::Error, flow: Flow<C>) -> Self {
        Self::new(
            ud,
            State::AcceptFailure {
                failure: cause.into(),
                flow,
            },
        )
    }

    fn received(ud: Key<C>, buffer: B, flow: Flow<C>) -> Self {
        Self::new(ud, State::RecvData { flow, buffer })
    }

    fn control(ud: Key<C>, result: Result<(), Errno>, flow: Flow<C>) -> Self {
        Self::new(
            ud,
            State::RecvControl {
                result: C::Writer::connected(result),
                flow,
            },
        )
    }
}

impl<C: entry::Context, B: dope_sys::Buffer> retry::Complete<C::Writer> for Event<C, B> {
    fn sent(ud: Key<C>, result: Bytes<C>) -> Self {
        Self::new(ud, State::Send { result })
    }

    fn connected(ud: Key<C>, result: Unit<C>) -> Self {
        Self::new(ud, State::Connect { result })
    }
}

impl<C: entry::Context, B> entry::Create<C::Outbound, C::Handle> for Event<C, B> {
    type Key = Key<C>;
    type Output = Self;

    fn created(ud: Key<C>, slot: C::Outbound, result: Result<C::Handle, socket::Create>) -> Self {
        let outcome = Create { slot, result };
        Self::new(ud, State::Create { outcome })
    }
}

impl<C: entry::Context, B> entry::Event<C, B> for Event<C, B> {
    fn opened(ud: Key<C>, result: Result<fd::OwnedFd, Errno>) -> Self {
        Self::new(ud, State::Open { result })
    }

    fn written(ud: Key<C>, result: Bytes<C>) -> Self {
        Self::new(ud, State::Write { result })
    }

    fn synced(ud: Key<C>, result: Unit<C>) -> Self {
        Self::new(ud, State::Sync { result })
    }

    const SHUTDOWN: Self = Self(None);

    fn resolve<'q, 'd>(
        self,
        files: &mut C::Files,
        drain: &C::Drain,
    ) -> entry::Resolved<C::Output, Self, resolve::Reclaimed<B, entry::Retired<C>, C::Error>>
    where
        C: entry::Socket<'q, 'd, B>,
    {
        let Some(Keyed { ud, state }) = self.0 else {
            return entry::Resolved::Public(C::SHUTDOWN);
        };
        let Some(bound) = drain.bind(ud) else {
            return entry::Resolved::Reclaim(Self::new(ud, state));
        };
        let driver = drain.driver();
        match try_deliver::<C, B>(state, bound, ud, files, driver) {
            Ok(done) => entry::Resolved::Public(done),
            Err(Failure::Operation { state, cause }) => {
                let fault = C::mismatched(ud, cause);
                entry::Resolved::Rejected(dispose::<C, B>(state, ud, driver, Err(fault)))
            }
            Err(Failure::Completion { state, fault }) => {
                entry::Resolved::Rejected(dispose::<C, B>(state, ud, driver, Err(fault)))
            }
            Err(Failure::Retired { report, fault }) => {
                entry::Resolved::Rejected(resolve::Reclaimed {
                    buffer: None,
                    report: Some(report),
                    result: Err(fault),
                })
            }
        }
    }

    fn reclaim<'q, 'd>(self, drain: &C::Drain) -> resolve::Reclaimed<B, entry::Retired<C>, C::Error>
    where
        C: entry::Socket<'q, 'd, B>,
    {
        use entry::Retired;

        let Some(Keyed { ud, state }) = self.0 else {
            return resolve::Reclaimed {
                buffer: None,
                report: Some(Retired::Shutdown),
                result: Ok(()),
            };
        };
        let driver = drain.driver();
        let mut result = Ok(());
        match &state {
            State::AcceptSuccess { flow, .. }
            | State::AcceptFailure { flow, .. }
            | State::RecvControl { flow, .. }
            | State::RecvData { flow, .. } => end::<C, B>(ud, *flow, drain),
            State::Send { .. }
            | State::Connect { .. }
            | State::Write { .. }
            | State::Sync { .. }
            | State::Open { .. } => finish::<C, B>(ud, drain),
            State::Create { outcome } => {
                if let Some(bound) = drain.bind(ud) {
                    match C::Socket::try_from(bound) {
                        Ok(bound) => {
                            let target = C::Target::from(&bound);
                            let creation =
                                Create::<C>::try_creation::<B>(outcome.slot, ud, driver, target);
                            bound.finish();
                            match creation {
                                Ok(creation) => C::fail_create(driver, creation),
                                Err(rejected) => {
                                    let (creation, fault) = rejected.into_parts();
                                    if let Some(creation) = creation {
                                        C::fail_create(driver, creation);
                                    }
                                    result = Err(fault);
                                }
                            }
                        }
                        Err(cause) => result = Err(C::mismatched(ud, cause)),
                    }
                }
            }
        }
        dispose::<C, B>(state, ud, driver, result)
    }
}

enum Failure<C: entry::Context, B, M, E> {
    Operation { state: State<C, B>, cause: M },
    Completion { state: State<C, B>, fault: E },
    Retired { report: entry::Retired<C>, fault: E },
}

fn dispose<'q, 'd, C: entry::Socket<'q, 'd, B>, B>(
    state: State<C, B>,
    key: Key<C>,
    driver: C::Driver,
    result: Result<(), C::Error>,
) -> resolve::Reclaimed<B, entry::Retired<C>, C::Error> {
    use entry::Retired;

    let mut buffer = None;
    let report = match state {
        State::AcceptSuccess { accepted, flow } => {
            let retired = C::abandon(driver, accepted);
            Retired::Accept {
                key,
                result: Ok(retired),
                flow,
            }
        }
        State::AcceptFailure { failure, flow } => Retired::Accept {
            key,
            result: Err(failure),
            flow,
        },
        State::RecvData {
            flow,
            buffer: received,
        } => {
            let report = C::report_recv(key, flow, &received);
            buffer = Some(received);
            report
        }
        State::RecvControl { result, flow } => Retired::RecvControl { key, result, flow },
        State::Send { result } => Retired::Send { key, result },
        State::Connect { result } => Retired::Connect { key, result },
        State::Open { result } => {
            let (result, close) = match result {
                Ok(fd) => (Ok(()), Some(Kqueue::close(fd))),
                Err(errno) => (Err(errno), None),
            };
            Retired::Open { key, result, close }
        }
        State::Write { result } => Retired::Write { key, result },
        State::Sync { result } => Retired::Sync { key, result },
        State::Create {
            outcome: Create { slot, result },
        } => {
            let (result, close) = match result {
                Ok(fd) => (Ok(()), Some(Kqueue::close(C::Native::into_fd(fd)))),
                Err(error) => (Err(error), None),
            };
            Retired::Create {
                key,
                slot,
                result,
                close,
            }
        }
    };
    resolve::Reclaimed {
        buffer,
        report: Some(report),
        result,
    }
}

fn finish<'q, 'd, C: entry::Socket<'q, 'd, B>, B>(key: Key<C>, drain: &C::Drain) {
    if let Some(bound) = drain.bind(key) {
        bound.finish();
    }
}

fn end<'q, 'd, C: entry::Socket<'q, 'd, B>, B>(key: Key<C>, flow: Flow<C>, drain: &C::Drain) {
    if let Some(bound) = drain.bind(key) {
        bound.end(flow);
    }
}
