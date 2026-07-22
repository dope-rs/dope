use std::io::{Read, Write};
use std::net::TcpStream;
use std::panic::{self, AssertUnwindSafe};
use std::pin::{Pin, pin};
use std::task::Poll;
use std::time::{Duration, Instant};

use dope::manifold::timer::Timer;
use dope_extra::harness::{TcpScript, TcpScriptConfig, poll_once, within};
use dope_fiber::{Context, Fiber};

struct PendingProbe(u8);

impl<'d> Fiber<'d> for PendingProbe {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _: Pin<&mut Context<'_, 'd>>) -> Poll<()> {
        self.0 += 1;
        Poll::Pending
    }
}

#[test]
fn poll_once_never_drives_a_pending_fiber_twice() {
    let mut probe = PendingProbe(0);
    dope_test::with_context(|mut cx| {
        let mut once = pin!(poll_once(&mut probe));
        assert!(dope_test::poll_ready(once.as_mut(), cx.as_mut()).is_pending());
    });
    assert_eq!(probe.0, 1);
}

#[test]
fn within_expires_a_pending_fiber_at_its_deadline() {
    dope_test::with_session(|mut session| {
        let timer: Timer<'_, 0> = Timer::with_capacity(1, session.driver());
        let slot = pin!(session.driver().make_ready_slot(dope_test::tok(0)));
        let pending = dope_fiber::poll_fn(|_| Poll::<()>::Pending);
        let mut bounded = pin!(within(&timer, Duration::from_millis(10), pending));
        assert!(
            dope_test::poll_with_slot(&mut session, slot.as_ref(), bounded.as_mut()).is_pending()
        );
        std::thread::sleep(Duration::from_millis(10));
        timer.expire(Instant::now());
        let outcome = dope_test::poll_with_slot(&mut session, slot.as_ref(), bounded.as_mut());
        assert!(matches!(outcome, Poll::Ready(Err(_))));
    });
}

#[test]
fn tcp_script_round_trips_and_returns_output() {
    let script = TcpScript::spawn(|stream| {
        let mut byte = [0];
        stream.read_exact(&mut byte).expect("read request");
        byte[0] += 1;
        stream.write_all(&byte).expect("write reply");
        byte[0]
    })
    .expect("spawn script");
    let mut client = TcpStream::connect(script.addr()).expect("connect");
    client.write_all(&[41]).expect("write request");
    let mut reply = [0];
    client.read_exact(&mut reply).expect("read reply");
    assert_eq!(reply, [42]);
    assert_eq!(script.finish().expect("finish script"), 42);
}

#[test]
fn tcp_scripts_are_isolated_under_parallel_load() {
    const PEERS: u8 = 16;
    let scripts = (0..PEERS)
        .map(|expected| {
            TcpScript::spawn(move |stream| {
                let mut byte = [0];
                stream.read_exact(&mut byte).expect("read request");
                assert_eq!(byte[0], expected);
                stream.write_all(&byte).expect("write reply");
                expected
            })
            .expect("spawn script")
        })
        .collect::<Vec<_>>();
    let clients = scripts
        .iter()
        .enumerate()
        .map(|(expected, script)| {
            let addr = script.addr();
            std::thread::spawn(move || {
                let mut stream = TcpStream::connect(addr).expect("connect");
                stream.write_all(&[expected as u8]).expect("write request");
                let mut reply = [0];
                stream.read_exact(&mut reply).expect("read reply");
                assert_eq!(reply[0], expected as u8);
            })
        })
        .collect::<Vec<_>>();
    for client in clients {
        client.join().expect("client");
    }
    for (expected, script) in scripts.into_iter().enumerate() {
        assert_eq!(script.finish().expect("finish script"), expected as u8);
    }
}

#[test]
fn tcp_script_propagates_the_original_panic() {
    let script = TcpScript::spawn(|_| panic!("script failed")).expect("spawn script");
    TcpStream::connect(script.addr()).expect("connect");
    let panic = panic::catch_unwind(AssertUnwindSafe(|| {
        script.finish().expect("finish script");
    }));
    let payload = panic.expect_err("script panic must propagate");
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"script failed"));
}

#[test]
fn tcp_script_accept_is_bounded() {
    let config = TcpScriptConfig {
        accept_timeout: Duration::from_millis(20),
        io_timeout: Duration::from_millis(20),
        finish_timeout: Duration::from_secs(1),
    };
    let script = TcpScript::spawn_with(config, |_| ()).expect("spawn script");
    let error = script.finish().expect_err("accept must time out");
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
}

#[test]
fn tcp_script_rejects_unbounded_configuration() {
    let config = TcpScriptConfig {
        accept_timeout: Duration::ZERO,
        ..Default::default()
    };
    let error = TcpScript::spawn_with(config, |_| ())
        .err()
        .expect("zero timeout must be rejected");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}
