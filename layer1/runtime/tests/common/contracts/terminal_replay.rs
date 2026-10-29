use dope_runtime::{process, shutdown};
use process::failure::bus;

fn replay<'run>(
    terminal: process::Terminal<'run>,
    first: shutdown::Requested<process::Shutdown<'run>>,
    second: shutdown::Requested<process::Shutdown<'run>>,
) -> (
    Result<bus::Event, bus::Fault>,
    Result<bus::Event, bus::Fault>,
) {
    let first = terminal.finish(first);
    let second = terminal.finish(second);
    (first, second)
}

fn main() {}
