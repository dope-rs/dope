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
