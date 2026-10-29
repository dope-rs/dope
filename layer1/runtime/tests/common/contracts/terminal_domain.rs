use dope_runtime::{process, shutdown};
use process::failure::bus;

fn foreign<'a: 'b, 'b>() {
    let _ = |terminal: process::Terminal<'a>,
             request: shutdown::Requested<process::Shutdown<'b>>|
     -> Result<bus::Event, bus::Fault> { terminal.finish(request) };
}

fn main() {}
