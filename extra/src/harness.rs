use std::io::{self, Error, ErrorKind};
use std::marker::PhantomData;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::panic::{self, AssertUnwindSafe};
use std::thread;
use std::time::Duration;

use dope::runtime::{Launcher, WorkerContext, WorkerEntry};

mod fiber;
mod tcp;

pub use fiber::{Elapsed, expect_pending, poll_once, within};
pub use tcp::{TcpScript, TcpScriptConfig};

pub struct Harness {
    bind: SocketAddr,
}

struct HarnessEntry<S>(PhantomData<fn(S)>);

struct TriggerHarnessEntry<S>(PhantomData<fn(S)>);

impl<S> WorkerEntry for HarnessEntry<S>
where
    S: FnOnce(WorkerContext) -> io::Result<()> + Send,
{
    type Input = S;

    fn run(server: Self::Input, context: WorkerContext) -> io::Result<()> {
        server(context)
    }
}

impl<S> WorkerEntry for TriggerHarnessEntry<S>
where
    S: FnOnce(WorkerContext, &dope::runtime::ShutdownTrigger) -> io::Result<()> + Send,
{
    type Input = S;

    fn run(server: Self::Input, context: WorkerContext) -> io::Result<()> {
        let trigger = context.shutdown_trigger()?;
        server(context, &trigger)
    }
}

impl Harness {
    pub const fn new(bind: SocketAddr) -> Self {
        Self { bind }
    }

    pub fn bind() -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let bind = listener.local_addr()?;
        drop(listener);
        Ok(Self::new(bind))
    }

    pub fn addr(&self) -> SocketAddr {
        self.bind
    }

    pub fn run<S, C, R>(&self, server: S, client: C) -> io::Result<R>
    where
        S: FnOnce(WorkerContext) -> io::Result<()> + Send,
        C: FnOnce(SocketAddr) -> R,
    {
        let bind = self.bind;
        let launcher = Launcher::unbound(1)?;
        let trigger = launcher.shutdown_trigger()?;
        thread::scope(|s| {
            let server_handle = s.spawn(move || launcher.run::<HarnessEntry<S>>(vec![server]));
            let ready = Self::wait_for_ready(bind);
            let outcome = ready.map(|()| panic::catch_unwind(AssertUnwindSafe(|| client(bind))));
            trigger.fire()?;
            let _ = TcpStream::connect(bind);
            let served = server_handle.join();
            let value = match outcome? {
                Ok(value) => value,
                Err(payload) => panic::resume_unwind(payload),
            };
            match served {
                Ok(run) => run.map(|()| value),
                Err(payload) => panic::resume_unwind(payload),
            }
        })
    }

    /// Runs a server that accepts both its worker context and the launcher's
    /// shared shutdown trigger.
    pub fn run_with_trigger<S, C, R>(&self, server: S, client: C) -> io::Result<R>
    where
        S: FnOnce(WorkerContext, &dope::runtime::ShutdownTrigger) -> io::Result<()> + Send,
        C: FnOnce(SocketAddr) -> R,
    {
        let bind = self.bind;
        let launcher = Launcher::unbound(1)?;
        let trigger = launcher.shutdown_trigger()?;
        thread::scope(|s| {
            let server_handle =
                s.spawn(move || launcher.run::<TriggerHarnessEntry<S>>(vec![server]));
            let ready = Self::wait_for_ready(bind);
            let outcome = ready.map(|()| panic::catch_unwind(AssertUnwindSafe(|| client(bind))));
            trigger.fire()?;
            let _ = TcpStream::connect(bind);
            let served = server_handle.join();
            let value = match outcome? {
                Ok(value) => value,
                Err(payload) => panic::resume_unwind(payload),
            };
            match served {
                Ok(run) => run.map(|()| value),
                Err(payload) => panic::resume_unwind(payload),
            }
        })
    }

    pub fn wait_for_ready(addr: SocketAddr) -> io::Result<()> {
        for _ in 0..200 {
            if TcpStream::connect(addr).is_ok() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(10));
        }
        Err(Error::new(
            ErrorKind::TimedOut,
            format!("server did not start: {addr}"),
        ))
    }
}
