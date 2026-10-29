use std::{convert, net, time};

use dope_core::{
    io::{socket, socket::Listener},
    platform::wake,
};
use dope_runtime::shutdown;

use crate::harness::scenario::run::{self, Task as _};

const STARTUP_TIMEOUT: time::Duration = time::Duration::from_secs(2);
const E2E_DEADLINE: time::Duration = time::Duration::from_secs(30);

#[derive(Debug)]
pub enum Error {
    Bind(socket::BindError),
    InvalidTimeouts {
        startup: time::Duration,
        deadline: time::Duration,
    },
}

pub struct Pair {
    listener: Listener,
    startup: time::Duration,
    deadline: time::Duration,
}

impl Pair {
    pub fn bind() -> Result<Self, Error> {
        Self::listen(net::SocketAddr::from(([127, 0, 0, 1], 0)))
    }

    pub fn listen(bind: net::SocketAddr) -> Result<Self, Error> {
        Ok(Self {
            listener: Listener::bind(bind).map_err(Error::Bind)?,
            startup: STARTUP_TIMEOUT,
            deadline: E2E_DEADLINE,
        })
    }

    pub fn timeouts(
        mut self,
        startup: time::Duration,
        deadline: time::Duration,
    ) -> Result<Self, Error> {
        if startup.is_zero() || deadline <= startup {
            return Err(Error::InvalidTimeouts { startup, deadline });
        }
        self.startup = startup;
        self.deadline = deadline;
        Ok(self)
    }

    pub fn addr(&self) -> net::SocketAddr {
        self.listener.local_addr()
    }

    pub fn run<S, C, R, E>(self, server: S, client: C) -> Result<R, Failure<E>>
    where
        S: FnOnce(shutdown::ReadySource, Listener) -> Result<(), E> + Send,
        C: FnOnce(net::SocketAddr) -> R,
        E: Send,
    {
        let Self {
            listener,
            startup,
            deadline,
        } = self;
        let bind = listener.local_addr();
        Self::execute(
            bind,
            startup,
            deadline,
            move |source| server(source, listener),
            client,
        )
    }

    /// Consumes the exact server outcome in its owning thread's final assertion.
    pub fn check<S, C, F, R, T, E>(
        self,
        server: S,
        client: C,
        check: F,
    ) -> Result<R, Failure<convert::Infallible>>
    where
        S: FnOnce(shutdown::ReadySource, Listener) -> Result<T, E> + Send,
        C: FnOnce(net::SocketAddr) -> R,
        F: FnOnce(Result<T, E>) + Send,
    {
        self.run(
            move |source, listener| {
                check(server(source, listener));
                Ok(())
            },
            client,
        )
    }

    /// Checks a server outcome without passing the reserved listener to it.
    pub fn verify<S, C, F, R, T, E>(
        self,
        server: S,
        client: C,
        check: F,
    ) -> Result<R, Failure<convert::Infallible>>
    where
        S: FnOnce(shutdown::ReadySource) -> Result<T, E> + Send,
        C: FnOnce(net::SocketAddr) -> R,
        F: FnOnce(Result<T, E>) + Send,
    {
        self.run_unbound(
            move |source| {
                check(server(source));
                Ok(())
            },
            client,
        )
    }

    pub fn run_unbound<S, C, R, E>(self, server: S, client: C) -> Result<R, Failure<E>>
    where
        S: FnOnce(shutdown::ReadySource) -> Result<(), E> + Send,
        C: FnOnce(net::SocketAddr) -> R,
        E: Send,
    {
        let (bind, startup, deadline) = {
            let Self {
                listener,
                startup,
                deadline,
            } = self;
            (listener.local_addr(), startup, deadline)
        };
        Self::execute(bind, startup, deadline, server, client)
    }

    fn execute<S, C, R, E>(
        bind: net::SocketAddr,
        startup: time::Duration,
        deadline: time::Duration,
        server: S,
        client: C,
    ) -> Result<R, Failure<E>>
    where
        S: FnOnce(shutdown::ReadySource) -> Result<(), E> + Send,
        C: FnOnce(net::SocketAddr) -> R,
        E: Send,
    {
        use crate::harness::scenario::process::Deadline;

        Deadline::with(deadline, |_| {
            let (source, trigger, ready) = shutdown::ReadyPair::new()
                .map_err(run::Error::ShutdownCreate)?
                .split();
            let ((outcome, fired), served) = run::Entry(move || server(source))
                .with(|| {
                    let outcome = ready.wait_timeout(startup).map(|()| client(bind));
                    (outcome, trigger.fire())
                })
                .map_err(run::Error::Start)?;
            let outcome = outcome.map_err(run::Error::Readiness)?;
            served
                .map_err(run::Error::Join)?
                .map_err(run::Error::Server)?;
            fired.map_err(run::Error::ShutdownFire)?;
            Ok(outcome)
        })
        .map_err(run::Error::Deadline)?
    }
}

/// Preserves the runtime's exact creation, shutdown and readiness failures.
type Failure<E> = run::Error<E, wake::CreateError, shutdown::FireError, wake::WaitError>;
