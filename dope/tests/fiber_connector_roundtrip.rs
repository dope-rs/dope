use std::io::{Read, Write};
use std::net::TcpListener;
use std::pin::pin;
use std::ptr::NonNull;
use std::time::Duration;

use dope::fiber::{Connector, Fiber, Holding};
use dope::runtime::profile::Production;
use dope::transport::Tcp;
use dope::transport::wire::Identity;
use dope::{DriverConfig, Executor};

const ID: u8 = 0;
const MAX_CONN: usize = 8;

type Conn = Connector<ID, Tcp, Identity>;

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct App {
    #[pin]
    #[manifold]
    connector: Conn,
}

// The async client connector must actually move bytes: a `connect_held` that
// resolves to an `Io` whose `read_into`/`write_all` route to the live
// connection state. The regression this guards: the `Io`'s connection id failed
// to resolve, so every `read_into` returned `Ok(0)` (phantom EOF) and
// `write_all` silently sent nothing.
#[test]
fn fiber_connector_roundtrip() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind echo server");
    let addr = listener.local_addr().expect("local addr");
    let response: &[u8] = b"PONG-fiber-connector-roundtrip";

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        // Best-effort drain of the request; do not block the test forever if
        // the client (under a bug) never sends anything.
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set read timeout");
        let mut req = [0u8; 64];
        let _ = stream.read(&mut req);
        stream.write_all(response).expect("server write");
        stream.flush().expect("server flush");
        // Drop closes the connection, producing EOF on the client side.
    });

    let cfg = <dope::DriverCfg as DriverConfig>::for_tcp_profile::<Production>(MAX_CONN);
    let mut exec = Executor::new(cfg).expect("executor");
    let connector = Conn::new(MAX_CONN, exec.driver_mut());
    let mut app = pin!(App { connector });

    let conn_ptr: NonNull<Conn> = NonNull::from(&app.connector);
    // SAFETY: `app` is pinned for the whole test; the connector field never moves.
    let hold: Holding<'_, Conn> = unsafe { Holding::from_raw(conn_ptr) };

    let got = dope_extra::block_on(
        &mut exec,
        app.as_mut(),
        Fiber::new(async move {
            let mut io = Conn::connect_held(hold, addr, Default::default()).await?;
            io.write_all(b"ping").await?;
            let mut acc = Vec::new();
            let mut buf = [0u8; 16];
            loop {
                let n = io.read_into(&mut buf).await?;
                if n == 0 {
                    break;
                }
                acc.extend_from_slice(&buf[..n]);
            }
            Ok::<_, std::io::Error>(acc)
        }),
    )
    .expect("connector roundtrip");

    server.join().expect("server thread");
    assert_eq!(
        got, response,
        "fiber connector must deliver the server's response bytes"
    );
}
