use std::{cell::Cell, pin::Pin, rc::Rc};

use dope_manifold::{
    Bundle, Outcome,
    listener::{connection, handler::Application},
    timing::Throughput,
};
use dope_net::{link::egress, tcp::Tcp, wire::Identity};
use dope_test::{fibers::Gate, scenario::scenarios::Listener};
use o3::buffer::{self, bytes::Retainable, storage::Shared};

const STATIC_HEADER: &[u8] = b"static:";
const STATIC_BODY: &[u8] = b"body;";
const SHARED_HEADER: &[u8] = b"shared:";
const SHARED_BODY: &[u8] = b"owner;";
const FROZEN_HEADER: &[u8] = b"frozen:";
const FROZEN_BODY: &[u8] = b"lease;";

fn frozen_body() -> buffer::Frozen {
    let pool = buffer::Pool::try_new(1, FROZEN_BODY.len()).expect("frozen body pool");
    let mut body = pool.try_acquire().expect("frozen body lease");
    body.try_extend(FROZEN_BODY).expect("fill frozen body");
    body.freeze()
}

fn resp_a() -> Vec<u8> {
    vec![0xA1; 8000]
}

fn resp_b() -> Vec<u8> {
    vec![0xB2; 9000]
}

struct PipelineApp {
    gate: Gate,
}

impl<'d, const ID: u8> Application<'d, ID> for PipelineApp {
    type Conn = ();
    type Wire = Identity;
    type Input = dope_manifold::receive::Borrowed;

    fn deadline(self: Pin<&Self>) -> Option<std::time::Instant> {
        None
    }

    fn close(self: Pin<&mut Self>, _connection: connection::Ctx<'_, 'd, ID, Identity, ()>) {
        self.get_mut().gate.hit();
    }
}

impl<'d, const ID: u8> dope_manifold::listener::handler::BorrowedApplication<'d, ID>
    for PipelineApp
{
    fn chunk<R: Retainable>(
        self: Pin<&mut Self>,
        mut connection: connection::Ctx<'_, 'd, ID, Identity, ()>,
        _chunk: R,
        driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let _ = self;
        assert!(
            connection
                .try_write()
                .expect("empty write slot")
                .submit_borrowed(0, b"", driver),
            "empty vectored writes must complete without arming a send"
        );
        let mut write = connection.try_write().expect("static write slot");
        write[..STATIC_HEADER.len()].copy_from_slice(STATIC_HEADER);
        assert!(write.submit_borrowed(STATIC_HEADER.len(), STATIC_BODY, driver));
        assert!(connection.has_pending_egress());
        let mut write = connection.try_write().expect("shared write slot");
        write[..SHARED_HEADER.len()].copy_from_slice(SHARED_HEADER);
        assert!(write.submit_shared(
            SHARED_HEADER.len(),
            Shared::copy_from_slice(SHARED_BODY),
            driver,
        ));
        let mut write = connection.try_write().expect("frozen write slot");
        write[..FROZEN_HEADER.len()].copy_from_slice(FROZEN_HEADER);
        assert!(write.submit_frozen(FROZEN_HEADER.len(), frozen_body(), driver));
        for reply in [resp_a(), resp_b()] {
            let mut write = connection.try_write().expect("queued write slot");
            write[..reply.len()].copy_from_slice(&reply);
            write.submit(reply.len(), driver);
        }
        Outcome::CloseAfter
    }
}

#[test]
fn split_owners_survive_direct_and_deferred_pipelined_submission() {
    let mut want = Vec::new();
    want.extend_from_slice(STATIC_HEADER);
    want.extend_from_slice(STATIC_BODY);
    want.extend_from_slice(SHARED_HEADER);
    want.extend_from_slice(SHARED_BODY);
    want.extend_from_slice(FROZEN_HEADER);
    want.extend_from_slice(FROZEN_BODY);
    want.extend_from_slice(&resp_a());
    want.extend_from_slice(&resp_b());
    let gate = Gate::new();
    Listener::new(64, Default::default())
        .direct_flights(0)
        .run::<0, _, Bundle<Tcp, Identity, Throughput>, _>(
            PipelineApp { gate: gate.clone() },
            |case| {
                let peer = case.request_reply(b"GO\n".to_vec());
                case.until(&gate, 1);
                let got = peer.join().expect("peer join");

                assert_eq!(
                    got, want,
                    "responses corrupted, reordered, or truncated on the pipelined path"
                );
                assert_eq!(gate.hits(), 1, "connection must close exactly once");
            },
        );
}

const DIRECT_BORROWED: &[u8] = b"borrowed-direct;";
const DIRECT_SHARED: &[u8] = b"shared-direct;";
const DIRECT_FROZEN: &[u8] = b"frozen-direct;";

#[derive(Default)]
struct DirectBodyState(u8);

struct DirectBodyApp {
    gate: Gate,
}

impl DirectBodyApp {
    fn submit_next<'d, const ID: u8>(
        &mut self,
        mut connection: connection::Ctx<'_, 'd, ID, Identity, DirectBodyState>,
        driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) {
        let mut write = connection
            .try_write()
            .expect("the single direct flight must recycle before the next callback");
        match write.state().0 {
            0 => {
                write.state_mut().0 = 1;
                assert!(write.submit_borrowed(0, DIRECT_BORROWED, driver));
            }
            1 => {
                write.state_mut().0 = 2;
                assert!(write.submit_shared(0, Shared::copy_from_slice(DIRECT_SHARED), driver,));
            }
            2 => {
                write.state_mut().0 = 3;
                let body = {
                    let pool = buffer::Pool::try_new(1, DIRECT_FROZEN.len())
                        .expect("direct frozen body pool");
                    let mut body = pool.try_acquire().expect("direct frozen body lease");
                    body.try_extend(DIRECT_FROZEN)
                        .expect("fill direct frozen body");
                    body.freeze()
                };
                assert!(write.submit_frozen(0, body, driver));
            }
            _ => panic!("direct body sequence submitted too many frames"),
        }
    }
}

impl<'d, const ID: u8> Application<'d, ID> for DirectBodyApp {
    type Conn = DirectBodyState;
    type Wire = Identity;
    type Input = dope_manifold::receive::Borrowed;

    fn deadline(self: Pin<&Self>) -> Option<std::time::Instant> {
        None
    }

    fn send(
        self: Pin<&mut Self>,
        mut connection: connection::Ctx<'_, 'd, ID, Identity, DirectBodyState>,
        _sent: usize,
        driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) {
        if connection.state().0 == 3 {
            connection.set_close_after();
        } else {
            self.get_mut().submit_next(connection, driver);
        }
    }

    fn close(
        self: Pin<&mut Self>,
        _connection: connection::Ctx<'_, 'd, ID, Identity, DirectBodyState>,
    ) {
        self.get_mut().gate.hit();
    }
}

impl<'d, const ID: u8> dope_manifold::listener::handler::BorrowedApplication<'d, ID>
    for DirectBodyApp
{
    fn chunk<R: Retainable>(
        self: Pin<&mut Self>,
        connection: connection::Ctx<'_, 'd, ID, Identity, DirectBodyState>,
        _chunk: R,
        driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        self.get_mut().submit_next(connection, driver);
        Outcome::Ok
    }
}

#[test]
fn one_direct_flight_recycles_across_every_owned_body_kind() {
    let gate = Gate::new();
    Listener::new(8, Default::default())
        .direct_flights(1)
        .egress(egress::Config::shared(0, 0))
        .run::<0, _, Bundle<Tcp, Identity, Throughput>, _>(
            DirectBodyApp { gate: gate.clone() },
            |case| {
                let peer = case.request_reply(b"GO\n".to_vec());
                case.until(&gate, 1);

                let got = peer.join().expect("peer join");
                let want: Vec<u8> = [DIRECT_BORROWED, DIRECT_SHARED, DIRECT_FROZEN]
                    .into_iter()
                    .flatten()
                    .copied()
                    .collect();
                assert_eq!(got, want);
            },
        );
}

struct InvalidScratchApp {
    rejected: Rc<Cell<bool>>,
    gate: Gate,
}

impl<'d, const ID: u8> Application<'d, ID> for InvalidScratchApp {
    type Conn = ();
    type Wire = Identity;
    type Input = dope_manifold::receive::Borrowed;

    fn deadline(self: Pin<&Self>) -> Option<std::time::Instant> {
        None
    }

    fn accept(
        self: Pin<&mut Self>,
        mut connection: connection::Ctx<'_, 'd, ID, Identity, ()>,
        driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let mut first = connection.try_write().expect("direct write slot");
        first[0] = b'A';
        assert!(first.submit(1, driver));

        let scratch = connection.try_write().expect("queued write slot");
        let invalid = scratch.len() + 1;
        self.get_mut()
            .rejected
            .set(!scratch.submit(invalid, driver));
        Outcome::Ok
    }

    fn close(self: Pin<&mut Self>, _connection: connection::Ctx<'_, 'd, ID, Identity, ()>) {
        self.get_mut().gate.hit();
    }
}

impl<'d, const ID: u8> dope_manifold::listener::handler::BorrowedApplication<'d, ID>
    for InvalidScratchApp
{
    fn chunk<R: Retainable>(
        self: Pin<&mut Self>,
        _connection: connection::Ctx<'_, 'd, ID, Identity, ()>,
        _chunk: R,
        _driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let _ = self;
        Outcome::Ok
    }
}

#[test]
fn oversized_scratch_write_is_rejected_instead_of_truncated() {
    let rejected = Rc::new(Cell::new(false));
    let gate = Gate::new();
    Listener::new(64, Default::default()).run::<0, _, Bundle<Tcp, Identity, Throughput>, _>(
        InvalidScratchApp {
            rejected: rejected.clone(),
            gate: gate.clone(),
        },
        |case| {
            let peer = case.peer(dope_test::peer::Peer::read_all);
            case.until(&gate, 1);

            assert!(rejected.get(), "invalid scratch length must fail");
            assert_eq!(peer.join().expect("peer join"), b"A");
        },
    );
}

const DISCARD_PREFIX: &[u8] = b"discard-this-prefix";
const DISCARD_PAYLOAD: &[u8] = b"keep-this-payload";

struct DiscardApp {
    gate: Gate,
}

impl<'d, const ID: u8> Application<'d, ID> for DiscardApp {
    type Conn = Vec<u8>;
    type Wire = Identity;
    type Input = dope_manifold::receive::Borrowed;

    fn deadline(self: Pin<&Self>) -> Option<std::time::Instant> {
        None
    }

    fn accept(
        self: Pin<&mut Self>,
        mut connection: connection::Ctx<'_, 'd, ID, Identity, Vec<u8>>,
        _driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let _ = self;
        assert!(connection.begin_discard(DISCARD_PREFIX.len()));
        Outcome::Ok
    }

    fn close(self: Pin<&mut Self>, _connection: connection::Ctx<'_, 'd, ID, Identity, Vec<u8>>) {
        self.get_mut().gate.hit();
    }
}

impl<'d, const ID: u8> dope_manifold::listener::handler::BorrowedApplication<'d, ID>
    for DiscardApp
{
    fn chunk<R: Retainable>(
        self: Pin<&mut Self>,
        mut connection: connection::Ctx<'_, 'd, ID, Identity, Vec<u8>>,
        chunk: R,
        driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let _ = self;
        let state = connection.state_mut();
        state.extend_from_slice(chunk.as_ref());
        assert!(
            DISCARD_PAYLOAD.starts_with(state),
            "discarded prefix leaked into the application"
        );
        if state.len() != DISCARD_PAYLOAD.len() {
            return Outcome::Ok;
        }
        let mut write = connection.try_write().expect("discard reply write slot");
        write[..DISCARD_PAYLOAD.len()].copy_from_slice(DISCARD_PAYLOAD);
        assert!(write.submit(DISCARD_PAYLOAD.len(), driver));
        Outcome::CloseAfter
    }
}

#[test]
fn discard_uses_retained_receive_buffer_and_preserves_following_payload() {
    let gate = Gate::new();
    Listener::new(64, Default::default()).run::<0, _, Bundle<Tcp, Identity, Throughput>, _>(
        DiscardApp { gate: gate.clone() },
        |case| {
            let mut request = DISCARD_PREFIX.to_vec();
            request.extend_from_slice(DISCARD_PAYLOAD);
            let peer = case.request_reply(request);
            case.until(&gate, 1);

            assert_eq!(peer.join().expect("peer join"), DISCARD_PAYLOAD);
            assert_eq!(gate.hits(), 1, "connection must close exactly once");
        },
    );
}
