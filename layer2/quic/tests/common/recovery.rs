use std::{convert::Infallible, io, os::fd::AsFd};

use dope::{
    core::{
        Backend, Errno,
        driver::{
            life::{creation::Attempt, recovery::{Policy, Recovery}},
            route, settings, storage,
        },
        io::Reclaimed,
    },
    runtime::{
        executor::{Build, Executor, session},
        shutdown,
    },
};
use dope_sys::io::fd::Fd;

pub fn executor(
    source: shutdown::Source,
    config: settings::Config,
) -> Executor<(), shutdown::Source> {
    let mut attempt = Attempt::new();
    let admitted = source.try_build(&mut attempt, config);
    match admitted {
        Ok(executor) => Ok(executor),
        Err(ref error) => Err(error.kind()),
    }
    .expect("driver admission")
}

fn recover(failure: Recovery<'_, '_>) -> Infallible {
    let message = format!("native recovery: {:?}\n", failure.cause());
    let stderr = io::stderr();
    let mut pending = message.as_bytes();
    while !pending.is_empty() {
        match Backend::write::<Errno>(stderr.as_fd(), pending) {
            Ok([]) => break,
            Ok(written) => pending = &pending[written.len()..],
            Err(error) if error.is_intr() => continue,
            Err(_) => break,
        }
    }
    failure.abort()
}

pub fn run<S: storage::Direct, R>(
    executor: Executor<S, shutdown::Source>,
    scenario: impl for<'scope, 'life, 'p, 'd> FnOnce(
        session::Scope<'scope, 'life, 'p, 'd, S, shutdown::Source>,
    ) -> R,
) -> (R, Vec<Reclaimed>) {
    let mut reports = Vec::new();
    let output = executor.run(|executor| {
        let policy = Policy::new(|event| reports.push(event), recover);
        let retired = executor.enter_with(scenario, policy);
        match retired {
            Ok(output) => Ok(output),
            Err(ref error) => Err(error.cause()),
        }
        .expect("driver retirement")
    });
    (output, reports)
}

#[derive(Debug)]
pub struct Teardown<const N: usize> {
    routes: [(route::Route, bool); N],
    shutdown: bool,
}

#[derive(Debug)]
pub enum Error {
    Unexpected(Reclaimed),
    Duplicate(route::Route),
    DuplicateShutdown,
    Missing(route::Route),
}

impl<const N: usize> Teardown<N> {
    pub fn new(expected: [route::Id; N]) -> Self {
        Self {
            routes: expected.map(|id| (id.into(), false)),
            shutdown: false,
        }
    }

    pub fn verify(mut self, reports: Vec<Reclaimed>) -> Result<(), Error> {
        use dope::core::{Errno, io::event::batch::Flow};
        for event in reports {
            match event {
                Reclaimed::RecvRoute {
                    target,
                    result: Err(error),
                    flow: Flow::Last,
                } if error == Errno::CANCELED => {
                    let Some((route, seen)) = self
                        .routes
                        .iter_mut()
                        .find(|(route, _)| *route == target.route())
                    else {
                        return Err(Error::Unexpected(Reclaimed::RecvRoute {
                            target,
                            result: Err(error),
                            flow: Flow::Last,
                        }));
                    };
                    if *seen {
                        return Err(Error::Duplicate(*route));
                    }
                    *seen = true;
                }
                Reclaimed::Shutdown if !self.shutdown => self.shutdown = true,
                Reclaimed::Shutdown => return Err(Error::DuplicateShutdown),
                event => return Err(Error::Unexpected(event)),
            }
        }
        if let Some((route, _)) = self.routes.iter().find(|(_, seen)| !*seen) {
            return Err(Error::Missing(*route));
        }
        Ok(())
    }
}
