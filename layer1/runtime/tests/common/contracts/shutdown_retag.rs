use dope_runtime::{process, shutdown};

fn retag<'run>(request: shutdown::Requested) -> shutdown::Requested<process::Shutdown<'run>> {
    request
}

fn main() {}
