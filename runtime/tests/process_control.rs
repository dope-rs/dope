use std::{fs, io, thread, time::Duration};

use dope_core::driver::settings;
use dope_runtime::{
    executor::Factory as _,
    process::{self, Cpus},
    shutdown,
};

#[test]
fn delayed_control_stops_the_running_process() -> io::Result<()> {
    if std::env::consts::OS == "linux" {
        let (soft, hard) = nofile_limit()?;
        assert_eq!(soft, hard);
    }

    let cpu = Cpus::current()?
        .next()
        .ok_or_else(|| io::Error::other("process has no available CPU"))?;
    let (runtime, control) = process::Runtime::controlled([(cpu, cpu)])?;
    thread::scope(|scope| {
        let firing = scope.spawn(move || {
            thread::sleep(Duration::from_millis(100));
            control.fire()
        });
        let run = runtime.run(run_worker, standalone);
        let fired = firing
            .join()
            .map_err(|_| io::Error::other("control thread panicked"))?;
        run?;
        fired
    })
}

fn standalone() -> io::Result<()> {
    Ok(())
}

fn nofile_limit() -> io::Result<(u64, u64)> {
    let limits = fs::read_to_string("/proc/self/limits")?;
    let values = limits
        .lines()
        .find_map(|line| line.strip_prefix("Max open files"))
        .ok_or_else(|| io::Error::other("process limits have no open-file entry"))?;
    let mut columns = values.split_whitespace();
    let soft = columns
        .next()
        .ok_or_else(|| io::Error::other("open-file soft limit is missing"))?;
    let hard = columns
        .next()
        .ok_or_else(|| io::Error::other("open-file hard limit is missing"))?;
    Ok((
        soft.parse().map_err(io::Error::other)?,
        hard.parse().map_err(io::Error::other)?,
    ))
}

fn run_worker(
    expected_cpu: u16,
    context: process::Context,
) -> io::Result<shutdown::Requested<process::Shutdown>> {
    assert_eq!(context.cpu(), expected_cpu);
    let config = settings::Config::for_quic_udp(2, 8)?;
    context
        .executor(config)?
        .enter(|mut session| session.with_app((), |mut application| application.run())?)
}
