use std::{io, net, panic, thread};

use dope::runtime::shutdown;

pub struct Harness {
    bind: net::SocketAddr,
}

impl Harness {
    pub const fn new(bind: net::SocketAddr) -> Self {
        Self { bind }
    }

    pub fn bind() -> io::Result<Self> {
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let bind = listener.local_addr()?;
        drop(listener);
        Ok(Self::new(bind))
    }

    pub const fn addr(&self) -> net::SocketAddr {
        self.bind
    }

    pub fn run<S, C, R>(&self, server: S, client: C) -> io::Result<R>
    where
        S: FnOnce(shutdown::Source) -> io::Result<shutdown::Requested> + Send,
        C: FnOnce(net::SocketAddr) -> R,
    {
        let bind = self.bind;
        let (source, trigger) = shutdown::Pair::new()?.split();
        thread::scope(|scope| {
            let server = scope.spawn(move || server(source).map(drop));
            let ready = Self::wait_for_ready(bind);
            let outcome =
                ready.map(|()| panic::catch_unwind(panic::AssertUnwindSafe(|| client(bind))));
            trigger.fire()?;
            let _ = net::TcpStream::connect(bind);
            let served = server.join();
            let value = match outcome? {
                Ok(value) => value,
                Err(payload) => panic::resume_unwind(payload),
            };
            match served {
                Ok(run) => run.map(|_| value),
                Err(payload) => panic::resume_unwind(payload),
            }
        })
    }

    fn wait_for_ready(addr: net::SocketAddr) -> io::Result<()> {
        use std::io::{Error, ErrorKind};
        for _ in 0..200 {
            use std::{thread::sleep, time::Duration};
            if net::TcpStream::connect(addr).is_ok() {
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
