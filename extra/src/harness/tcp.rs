use std::any::Any;
use std::io::{self, Error, ErrorKind};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const ACCEPT_POLL: Duration = Duration::from_millis(1);

#[derive(Clone, Copy, Debug)]
pub struct TcpScriptConfig {
    pub accept_timeout: Duration,
    pub io_timeout: Duration,
    pub finish_timeout: Duration,
}

impl Default for TcpScriptConfig {
    fn default() -> Self {
        Self {
            accept_timeout: Duration::from_secs(5),
            io_timeout: Duration::from_secs(5),
            finish_timeout: Duration::from_secs(10),
        }
    }
}

struct Shared {
    cancelled: AtomicBool,
    stream: Mutex<Option<TcpStream>>,
}

impl Shared {
    fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            stream: Mutex::new(None),
        }
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        let stream = self
            .stream
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(stream) = stream.as_ref() {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

enum Outcome<T> {
    Complete(T),
    Io(io::Error),
    Panicked(Box<dyn Any + Send>),
}

/// Owns one loopback TCP listener and runs one scripted peer on a worker
/// thread. Each instance binds its own ephemeral port and therefore composes
/// safely with Rust's parallel test runner.
///
/// Accept, socket I/O, and result collection are independently bounded. A
/// dropped or timed-out handle shuts down the accepted socket so a blocked
/// script can leave its I/O operation without holding up another test.
#[must_use = "keep the script alive and call finish to observe its outcome"]
pub struct TcpScript<T> {
    addr: SocketAddr,
    config: TcpScriptConfig,
    shared: Arc<Shared>,
    outcome: Receiver<Outcome<T>>,
    thread: Option<JoinHandle<()>>,
}

impl<T: Send + 'static> TcpScript<T> {
    pub fn spawn(script: impl FnOnce(&mut TcpStream) -> T + Send + 'static) -> io::Result<Self> {
        Self::spawn_with(TcpScriptConfig::default(), script)
    }

    pub fn spawn_with(
        config: TcpScriptConfig,
        script: impl FnOnce(&mut TcpStream) -> T + Send + 'static,
    ) -> io::Result<Self> {
        validate(config)?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let shared = Arc::new(Shared::new());
        let worker_shared = shared.clone();
        let (tx, outcome) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("dope-tcp-script".into())
            .spawn(move || {
                let outcome = run(listener, config, &worker_shared, script);
                let _ = tx.send(outcome);
            })?;
        Ok(Self {
            addr,
            config,
            shared,
            outcome,
            thread: Some(thread),
        })
    }

    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Waits for the script's bounded outcome. Script panics resume on the
    /// caller so the test runner reports their original payload and location.
    pub fn finish(mut self) -> io::Result<T> {
        let outcome = match self.outcome.recv_timeout(self.config.finish_timeout) {
            Ok(outcome) => outcome,
            Err(RecvTimeoutError::Timeout) => {
                self.shared.cancel();
                return Err(Error::new(
                    ErrorKind::TimedOut,
                    format!("TCP script did not finish: {}", self.addr),
                ));
            }
            Err(RecvTimeoutError::Disconnected) => {
                self.join_finished();
                return Err(Error::new(
                    ErrorKind::BrokenPipe,
                    format!("TCP script exited without an outcome: {}", self.addr),
                ));
            }
        };
        self.join_finished();
        match outcome {
            Outcome::Complete(value) => Ok(value),
            Outcome::Io(error) => Err(error),
            Outcome::Panicked(payload) => panic::resume_unwind(payload),
        }
    }

    fn join_finished(&mut self) {
        if let Some(thread) = self.thread.take()
            && let Err(payload) = thread.join()
        {
            panic::resume_unwind(payload);
        }
    }
}

impl<T> Drop for TcpScript<T> {
    fn drop(&mut self) {
        self.shared.cancel();
        if self.thread.as_ref().is_some_and(JoinHandle::is_finished) {
            let _ = self.thread.take().map(JoinHandle::join);
        }
    }
}

fn validate(config: TcpScriptConfig) -> io::Result<()> {
    if config.accept_timeout.is_zero()
        || config.io_timeout.is_zero()
        || config.finish_timeout.is_zero()
    {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "TCP script timeouts must be greater than zero",
        ));
    }
    Ok(())
}

fn run<T>(
    listener: TcpListener,
    config: TcpScriptConfig,
    shared: &Shared,
    script: impl FnOnce(&mut TcpStream) -> T,
) -> Outcome<T> {
    let mut stream = match accept(&listener, shared, config.accept_timeout) {
        Ok(stream) => stream,
        Err(error) => return Outcome::Io(error),
    };
    if let Err(error) = configure(&stream, config.io_timeout) {
        return Outcome::Io(error);
    }
    let cancel_stream = match stream.try_clone() {
        Ok(stream) => stream,
        Err(error) => return Outcome::Io(error),
    };
    *shared
        .stream
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(cancel_stream);
    let outcome = match panic::catch_unwind(AssertUnwindSafe(|| script(&mut stream))) {
        Ok(value) => Outcome::Complete(value),
        Err(payload) => Outcome::Panicked(payload),
    };
    shared
        .stream
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take();
    outcome
}

fn accept(listener: &TcpListener, shared: &Shared, timeout: Duration) -> io::Result<TcpStream> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "accept timeout is too large"))?;
    loop {
        match listener.accept() {
            Ok((stream, _)) => return Ok(stream),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
        if shared.is_cancelled() {
            return Err(Error::new(ErrorKind::Interrupted, "TCP script cancelled"));
        }
        if Instant::now() >= deadline {
            return Err(Error::new(
                ErrorKind::TimedOut,
                "TCP script timed out waiting for a connection",
            ));
        }
        thread::sleep(ACCEPT_POLL);
    }
}

fn configure(stream: &TcpStream, timeout: Duration) -> io::Result<()> {
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    stream.set_nodelay(true)
}
