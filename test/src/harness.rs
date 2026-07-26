use std::io::{self, Error, ErrorKind};
use std::marker::PhantomData;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::thread::{scope, sleep};
use std::time::Duration;

use dope::runtime::launcher::{Launcher, WorkerContext, WorkerEntry};

pub struct Harness {
    bind: SocketAddr,
}

struct HarnessEntry<S>(PhantomData<fn(S)>);

impl<S> WorkerEntry for HarnessEntry<S>
where
    S: FnOnce(WorkerContext) -> io::Result<()> + Send,
{
    type Input = S;

    fn run(server: Self::Input, context: WorkerContext) -> io::Result<()> {
        server(context)
    }
}

impl Harness {
    fn new(bind: SocketAddr) -> Self {
        Self { bind }
    }

    pub fn bind() -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let bind = listener.local_addr()?;
        drop(listener);
        Ok(Self::new(bind))
    }

    pub const fn addr(&self) -> SocketAddr {
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
        scope(|scope| {
            let server = scope.spawn(move || launcher.run::<HarnessEntry<S>>(vec![server]));
            let ready = Self::wait_for_ready(bind);
            let outcome = ready.map(|()| catch_unwind(AssertUnwindSafe(|| client(bind))));
            trigger.fire()?;
            let _ = TcpStream::connect(bind);
            let served = server.join();
            let value = match outcome? {
                Ok(value) => value,
                Err(payload) => resume_unwind(payload),
            };
            match served {
                Ok(run) => run.map(|()| value),
                Err(payload) => resume_unwind(payload),
            }
        })
    }

    fn wait_for_ready(addr: SocketAddr) -> io::Result<()> {
        for _ in 0..200 {
            if TcpStream::connect(addr).is_ok() {
                return Ok(());
            }
            sleep(Duration::from_millis(10));
        }
        Err(Error::new(
            ErrorKind::TimedOut,
            format!("server did not start: {addr}"),
        ))
    }
}
