use dope_test as common;

use std::cell::Cell;
use std::io;
use std::net::SocketAddr;
use std::rc::{Rc, Weak};
use std::time::{Duration, Instant};

extern crate dope;
use dope::manifold::connector::source::explicit::{Explicit, ExplicitDialer};
use dope::manifold::connector::source::health::Static;
use dope::manifold::connector::source::{Action, DialKey, Dialer};
use dope_net::Transport;
use dope_net::tcp::Tcp;

use common::assert_unwinds;

fn expect_connect<T: Transport>(dialer: &mut impl Dialer<T>, now: Instant) -> DialKey {
    match dialer.poll_connect(now) {
        Action::Connect { key, .. } => key,
        _ => panic!("expected Connect"),
    }
}

#[test]
fn explicit_dialer_is_a_zero_overhead_borrowed_view() {
    assert_eq!(
        std::mem::size_of::<ExplicitDialer<'static, Tcp>>(),
        std::mem::size_of::<&'static Explicit<Tcp>>()
    );
}

#[derive(Clone, Copy, Default)]
struct ConversionStreamConfig;

#[derive(Clone, Copy, Default)]
enum ConversionMode {
    #[default]
    Return,
    PanicAddr,
    PanicParams,
    PollAddr,
    ReplaceAddr,
    ReplaceDrop,
    PanicDrop,
}

#[derive(Default)]
struct ConversionControl {
    mode: Cell<ConversionMode>,
    key: Cell<Option<DialKey>>,
    replacement: Cell<Option<DialKey>>,
}

struct ConversionAddr {
    socket: SocketAddr,
    dialer: Weak<Explicit<ConversionTransport>>,
    control: Rc<ConversionControl>,
}

impl Drop for ConversionAddr {
    fn drop(&mut self) {
        match self.control.mode.get() {
            ConversionMode::ReplaceDrop => ConversionTransport::replace(self),
            ConversionMode::PanicDrop => {
                self.control.mode.set(ConversionMode::Return);
                panic!("address drop panic");
            }
            _ => {}
        }
    }
}

struct ConversionTransport;

impl ConversionTransport {
    fn replace(addr: &ConversionAddr) {
        let dialer = addr.dialer.upgrade().expect("dialer");
        let key = addr.control.key.get().expect("dial key");
        addr.control.mode.set(ConversionMode::Return);
        let mut shared = dialer.dialer();
        shared.kill(key);
        let replacement = shared
            .dial(
                ConversionAddr {
                    socket: addr.socket,
                    dialer: addr.dialer.clone(),
                    control: addr.control.clone(),
                },
                ConversionStreamConfig,
            )
            .expect("replacement dial");
        addr.control.replacement.set(Some(replacement));
    }
}

impl Transport for ConversionTransport {
    type Addr = ConversionAddr;
    type StreamConfig = ConversionStreamConfig;

    fn to_sock_addr(addr: &Self::Addr) -> io::Result<dope::io::socket::addr::Addr> {
        match addr.control.mode.get() {
            ConversionMode::PanicAddr => panic!("address conversion panic"),
            ConversionMode::PollAddr => {
                let dialer = addr.dialer.upgrade().expect("dialer");
                let mut shared = dialer.dialer();
                assert!(matches!(shared.poll_connect(Instant::now()), Action::Idle));
                Ok(dope::io::socket::addr::Addr::from_std(addr.socket))
            }
            ConversionMode::ReplaceAddr => {
                Self::replace(addr);
                panic!("address conversion reentry panic");
            }
            _ => Ok(dope::io::socket::addr::Addr::from_std(addr.socket)),
        }
    }

    fn socket_params(addr: &Self::Addr) -> (i32, i32, i32) {
        if matches!(addr.control.mode.get(), ConversionMode::PanicParams) {
            panic!("socket params panic");
        }
        (libc::AF_INET, libc::SOCK_STREAM, 0)
    }
}

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

fn static_dialer(upstreams: Vec<SocketAddr>, backoff: Duration, cap: usize) -> Static<Tcp> {
    let mut d = Static::new(upstreams, backoff, dope::hash::Seed::new([1, 2]).state());
    d.resize(cap);
    d
}

fn settle(d: &mut Static<Tcp>, now: Instant) {
    while d.has_pending() {
        if !matches!(d.poll_connect(now), Action::Connect { .. }) {
            break;
        }
    }
}

fn conv_addr(
    port: u16,
    dialer: &Rc<Explicit<ConversionTransport>>,
    control: &Rc<ConversionControl>,
) -> ConversionAddr {
    ConversionAddr {
        socket: addr(port),
        dialer: Rc::downgrade(dialer),
        control: control.clone(),
    }
}

fn conversion_env() -> (
    Rc<Explicit<ConversionTransport>>,
    Rc<ConversionControl>,
    DialKey,
) {
    let mut dialer = Explicit::<ConversionTransport>::default();
    dialer.resize(1);
    let dialer = Rc::new(dialer);
    let control = Rc::new(ConversionControl::default());
    let mut shared = dialer.dialer();
    let key = shared
        .dial(conv_addr(9501, &dialer, &control), ConversionStreamConfig)
        .expect("dial");
    control.key.set(Some(key));
    (dialer, control, key)
}

fn conversion_dialer() -> (
    Rc<Explicit<ConversionTransport>>,
    Rc<ConversionControl>,
    DialKey,
) {
    let (dialer, control, key) = conversion_env();
    let mut shared = dialer.dialer();
    assert_eq!(expect_connect(&mut shared, Instant::now()), key);
    (dialer, control, key)
}

#[test]
fn explicit_drop_reentry_reuses_one_slot_without_corrupting_free_list() {
    let (dialer, control, key) = conversion_env();
    control.mode.set(ConversionMode::ReplaceDrop);

    let mut shared = dialer.dialer();
    shared.kill(key);

    let replacement = control.replacement.get().expect("replacement");
    assert_ne!(replacement, key);
    assert!(
        shared
            .dial(conv_addr(9503, &dialer, &control), ConversionStreamConfig)
            .is_none()
    );
    assert_eq!(
        expect_connect(&mut shared, Instant::now()),
        replacement,
        "replacement must remain queued"
    );
}

#[test]
fn explicit_panicking_drop_commits_release_before_unwind() {
    let (dialer, control, key) = conversion_dialer();
    control.mode.set(ConversionMode::PanicDrop);

    assert_unwinds(|| {
        let mut shared = dialer.dialer();
        shared.kill(key);
    });

    let mut shared = dialer.dialer();
    let replacement = shared
        .dial(conv_addr(9504, &dialer, &control), ConversionStreamConfig)
        .expect("released slot");
    assert_ne!(replacement, key);
    assert!(shared.sock_addr(key).is_none());
    assert!(shared.sock_addr(replacement).is_some());
    assert_eq!(
        expect_connect(&mut shared, Instant::now()),
        replacement,
        "replacement must remain queued"
    );
    assert!(matches!(shared.poll_connect(Instant::now()), Action::Idle));
}

#[test]
fn empty_static_idles_until_dialed() {
    let mut d = static_dialer(vec![], Duration::from_millis(50), 4);
    let now = Instant::now();
    assert!(matches!(d.poll_connect(now), Action::Idle));

    let key = d
        .dial(addr(9001), Default::default())
        .expect("dial accepted");
    assert_eq!(
        expect_connect(&mut d, now),
        key,
        "expected Connect after dial"
    );
    assert!(d.sock_addr(key).is_some());
    assert!(matches!(d.poll_connect(now), Action::Idle));

    for port in 9002..9005 {
        assert!(d.dial(addr(port), Default::default()).is_some());
    }
    assert!(
        d.dial(addr(9005), Default::default()).is_none(),
        "dial past capacity must be refused"
    );
}

#[test]
fn dialed_slot_is_one_shot_and_reused() {
    let mut d = static_dialer(vec![], Duration::from_millis(50), 2);
    let now = Instant::now();
    assert!(
        !d.has_pending(),
        "no upstreams and nothing dialed: gate shut"
    );

    let key = d.dial(addr(9002), Default::default()).expect("dial");
    assert!(d.has_pending(), "a queued dial opens the gate");
    assert_eq!(expect_connect(&mut d, now), key);
    d.connect_outcome(key, true, now);
    settle(&mut d, now);
    assert!(!d.has_pending(), "a connected slot leaves nothing to drive");

    d.disconnect(key, now);
    assert!(
        matches!(d.poll_connect(now), Action::Idle),
        "one-shot override slot must go dead on disconnect, not reconnect"
    );
    settle(&mut d, now);
    assert!(
        !d.has_pending(),
        "spent one-shot slot is dead, gate stays shut"
    );

    let next = d.dial(addr(9003), Default::default()).expect("redial");
    assert_eq!(
        key.index(),
        next.index(),
        "spent override slot must be reused"
    );
    assert_ne!(key, next, "reused slot must advance its generation");
    assert!(d.sock_addr(next).is_some());
}

#[test]
fn failed_connect_keeps_gate_open_until_backoff_is_armed() {
    let window = Duration::from_millis(20);
    let mut d = static_dialer(vec![addr(9301)], window, 1);
    let now = Instant::now();
    assert!(d.has_pending(), "static upstream is drivable at startup");

    let key = expect_connect(&mut d, now);
    d.connect_outcome(key, false, now);
    assert!(
        d.has_pending(),
        "failed connect must keep the gate open to arm the backoff timer"
    );

    let retry_at = match d.poll_connect(now) {
        Action::Backoff { min_retry_at } => min_retry_at,
        _ => panic!("expected Backoff after failed connect"),
    };
    assert!(retry_at > now, "retry must be in the future");
    assert!(
        !d.has_pending(),
        "once Backoff is observed the armed timer owns the retry"
    );

    let due = retry_at + Duration::from_millis(1);
    assert!(
        matches!(d.poll_connect(due), Action::Connect { .. }),
        "backoff must retry once its deadline is due"
    );
}

#[test]
fn revive_reopens_the_gate() {
    let mut d = static_dialer(vec![addr(9401)], Duration::from_millis(20), 1);
    let now = Instant::now();

    let key = expect_connect(&mut d, now);
    d.connect_outcome(key, false, now);
    assert!(matches!(d.poll_connect(now), Action::Backoff { .. }));
    assert!(!d.has_pending(), "backed-off slot waits on the timer");

    d.revive();
    assert!(d.has_pending(), "revive makes the upstream drivable again");
    assert!(matches!(d.poll_connect(now), Action::Connect { .. }));
}

#[test]
fn explicit_conversion_panics_restore_slot_state() {
    let (dialer, control, key) = conversion_dialer();

    control.mode.set(ConversionMode::PanicAddr);
    assert_unwinds(|| dialer.dialer().sock_addr(key));
    control.mode.set(ConversionMode::Return);
    assert!(dialer.dialer().sock_addr(key).is_some());

    control.mode.set(ConversionMode::PanicParams);
    assert_unwinds(|| dialer.dialer().socket_params(key));
    control.mode.set(ConversionMode::Return);
    assert_eq!(
        dialer.dialer().socket_params(key),
        Some((libc::AF_INET, libc::SOCK_STREAM, 0))
    );
}

#[test]
fn explicit_conversion_panic_preserves_reentrant_generation() {
    let (dialer, control, key) = conversion_dialer();

    control.mode.set(ConversionMode::ReplaceAddr);
    assert_unwinds(|| dialer.dialer().sock_addr(key));
    let replacement = control.replacement.get().expect("replacement key");
    assert_ne!(replacement, key);
    assert!(dialer.dialer().sock_addr(key).is_none());
    assert!(dialer.dialer().sock_addr(replacement).is_some());
    let mut shared = dialer.dialer();
    assert_eq!(
        expect_connect(&mut shared, Instant::now()),
        replacement,
        "expected replacement Connect"
    );
}

#[test]
fn explicit_conversion_reentry_preserves_pending_membership() {
    let (dialer, control, key) = conversion_env();

    control.mode.set(ConversionMode::PollAddr);
    assert!(dialer.dialer().sock_addr(key).is_some());
    control.mode.set(ConversionMode::Return);
    let mut shared = dialer.dialer();
    assert_eq!(expect_connect(&mut shared, Instant::now()), key);
}

#[test]
fn explicit_growth_preserves_generation_safe_pending_links() {
    let mut dialer = Explicit::<Tcp>::default();
    dialer.resize(1);
    let first = dialer
        .dialer()
        .dial(addr(9601), Default::default())
        .unwrap();
    dialer.resize(3);
    let mut shared = dialer.dialer();
    let stale = shared.dial(addr(9602), Default::default()).unwrap();
    let third = shared.dial(addr(9603), Default::default()).unwrap();
    shared.kill(stale);
    let replacement = shared.dial(addr(9604), Default::default()).unwrap();

    assert_eq!(stale.index(), replacement.index());
    assert_ne!(stale, replacement);
    assert!(shared.sock_addr(stale).is_none());
    let now = Instant::now();
    assert_eq!(expect_connect(&mut shared, now), first);
    assert_eq!(expect_connect(&mut shared, now), third);
    assert_eq!(expect_connect(&mut shared, now), replacement);
    assert!(matches!(shared.poll_connect(now), Action::Idle));
}
