use std::net::{SocketAddr, TcpStream};
use std::time::Duration;
use std::{io, thread};

use dope::launcher::{Ctx, Launcher};

use crate::Trigger;

pub fn run_with_trigger<S, C, R>(bind: SocketAddr, server: S, client: C) -> R
where
    S: Fn(Ctx, &Trigger) -> io::Result<()> + Send + Sync,
    C: FnOnce(SocketAddr) -> R,
{
    let trigger = Trigger::new().expect("shutdown trigger");
    thread::scope(|s| {
        let server_handle = s.spawn(|| {
            Launcher::new(vec![0u16])
                .run(|ctx| server(ctx, &trigger))
                .expect("server thread")
        });
        wait_for_ready(bind);
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| client(bind)));
        trigger.fire();
        let _ = TcpStream::connect(bind);
        let _ = server_handle.join();
        match r {
            Ok(v) => v,
            Err(p) => std::panic::resume_unwind(p),
        }
    })
}

pub fn wait_for_ready(addr: SocketAddr) {
    for _ in 0..200 {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("server did not start: {addr}");
}

pub fn ephemeral_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    addr
}
