use std::pin::Pin;

use dope_manifold::{
    Bundle, Outcome,
    listener::{connection, handler::Application},
    timing::Throughput,
};
use dope_net::{tcp::Tcp, wire::Identity};
use dope_test::{fibers::Gate, scenario::scenarios::Listener};
use o3::buffer::bytes::Retainable;

struct ReplyApp {
    payload: Vec<u8>,
    gate: Gate,
}

impl<'d, const ID: u8> Application<'d, ID> for ReplyApp {
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

impl<'d, const ID: u8> dope_manifold::listener::handler::BorrowedApplication<'d, ID> for ReplyApp {
    fn chunk<R: Retainable>(
        self: Pin<&mut Self>,
        mut connection: connection::Ctx<'_, 'd, ID, Identity, ()>,
        _chunk: R,
        driver: &mut dope_core::driver::retained::Context<'_, '_, 'd>,
    ) -> Outcome {
        let payload = &self.get_mut().payload;
        let n = payload.len();
        let mut write = connection.try_write().expect("listener write slot");
        write[..n].copy_from_slice(payload);
        write.submit(n, driver);
        Outcome::CloseAfter
    }
}

#[test]
fn drain_reply_is_delivered_in_full_before_close() {
    let want = dope_test::peer::Pattern::with_len(12_000).into_bytes();
    let gate = Gate::new();
    Listener::new(64, Default::default()).run::<0, _, Bundle<Tcp, Identity, Throughput>, _>(
        ReplyApp {
            payload: want.clone(),
            gate: gate.clone(),
        },
        |case| {
            let peer = case.request_reply(b"GET\n".to_vec());
            case.until(&gate, 1);
            let got = peer.join().expect("peer join");

            assert_eq!(got, want, "reply truncated or corrupted on the drain path");
            assert_eq!(gate.hits(), 1, "connection must close exactly once");
        },
    );
}
