//! Thread-per-core deployment.
//!
//! The recommended production model is thread-per-core: one [`Executor`] per
//! CPU, with the worker count equal to the number of cores. [`Launcher::run`]
//! spawns one OS thread per CPU in `cpus`, pins each via [`Launcher::pin_to_cpu`]
//! ([`Ctx::cpu`]), and runs the closure on every core. Each worker builds its
//! own driver from the [`Production`] profile via
//! [`DriverCfg::for_tcp_profile::<Production>(max_conn)`][for_tcp_profile],
//! tagging the ring with [`with_cpu_id`]. Cores share one listening port through
//! the `SO_REUSEPORT` listener, enabled with
//! [`ListenerOpts::reuseport`][reuseport]. Adapt to the host with the `max_conn`,
//! `backlog`, and CPU-pinning knobs.
//!
//! ```ignore
//! use dope::{DriverConfig, Executor, runtime::profile::Production};
//! use dope::launcher::{Ctx, Launcher};
//!
//! let cores = std::thread::available_parallelism()?.get() as u16;
//! let cpus = (0..cores).collect::<Vec<u16>>();
//!
//! Launcher::new(cpus).run(|ctx: Ctx| {
//!     let cfg = dope::DriverCfg::for_tcp_profile::<Production>(8192)
//!         .with_cpu_id(Some(ctx.cpu));
//!     let _exec = Executor::new(cfg)?;
//!     // bind a SO_REUSEPORT listener and run the dispatcher loop here
//!     Ok(())
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! [`Executor`]: crate::Executor
//! [`Production`]: crate::runtime::profile::Production
//! [for_tcp_profile]: crate::DriverConfig::for_tcp_profile
//! [`with_cpu_id`]: crate::DriverConfig::with_cpu_id
//! [reuseport]: crate::transport::config::tcp::ListenerOpts::reuseport

use std::io;

use crate::backend;

#[derive(Clone, Copy, Debug)]
pub struct Ctx {
    pub cpu: u16,
}

impl Ctx {
    fn enter<F>(self, run: &F) -> io::Result<()>
    where
        F: Fn(Ctx) -> io::Result<()>,
    {
        <backend::Default as backend::Backend>::init_thread(self.cpu)?;
        run(self)
    }
}

pub struct Launcher {
    cpus: Vec<u16>,
}

impl Launcher {
    pub fn new(cpus: Vec<u16>) -> Self {
        Self { cpus }
    }

    pub fn pin_to_cpu(cpu: u16) -> io::Result<()> {
        <backend::Default as backend::Backend>::init_thread(cpu)
    }

    pub fn run<F>(self, run_thread: F) -> io::Result<()>
    where
        F: Fn(Ctx) -> io::Result<()> + Send + Sync,
    {
        let Self { cpus } = self;
        let (last, rest) = match cpus.split_last() {
            Some(split) => split,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Launcher::run requires at least one cpu",
                ));
            }
        };

        let run_ref = &run_thread;
        std::thread::scope(|s| -> io::Result<()> {
            let mut handles = Vec::with_capacity(rest.len());
            for &cpu in rest {
                let ctx = Ctx { cpu };
                handles.push(s.spawn(move || ctx.enter(run_ref)));
            }
            let leader_result = Ctx { cpu: *last }.enter(run_ref);
            for h in handles {
                h.join()
                    .map_err(|_| io::Error::other("launcher worker panicked"))??;
            }
            leader_result
        })
    }
}
