use o3::buffer::write::Sink as _;
use std::{cell::Cell, convert, pin::Pin, rc::Rc};

use dope_core::{
    driver::{self, route, schedule::clock},
    random::Seed,
};
use dope_manifold::{
    app::{receive, receive::Outcome},
    network::listener::{
        conn::{self, write, write::error},
        handler::{self, Application, failures, retire, send},
    },
};
use dope_net::{
    link::egress::{self, load::Value},
    wire::Identity,
};
use dope_test::harness::observe::VecCellExt as _;
use dope_test::harness::signals::Gate;
use o3::buffer::{bytes::TryRetain, resident};

type Payload<'d> = dope_net::link::egress::data::Bound<
    resident::Shared<'d>,
    o3::buffer::resident::bound::AtMost<6>,
>;

pub const FIRST_HEADER: &[u8] = b"first:";
pub const FIRST_BODY: &[u8; 5] = b"body;";
pub const SECOND_HEADER: &[u8] = b"second:";
pub const SECOND_BODY: &[u8; 6] = b"owner;";
pub const THIRD_HEADER: &[u8] = b"third:";
pub const THIRD_BODY: &[u8; 6] = b"lease;";

pub const FIRST_REPLY: [u8; 8000] = [0xA1; 8000];
pub const SECOND_REPLY: [u8; 9000] = [0xB2; 9000];

pub enum Event {
    Chunk(usize),
    Write(Result<(), write::error::Admission>, egress::Pending),
}

pub struct Admit {
    pub(crate) gate: Gate,
    pub(crate) events: Rc<Cell<Vec<Event>>>,
}

impl<'d, ID: route::Identity> Application<'d, ID> for Admit {
    type Conn = ();
    type Build = convert::Infallible;
    type Wire = Identity;
    type WireFailure = failures::Close;
    type Retirement = retire::Close;
    type Egress = Payload<'d>;
    type Input = receive::Borrowed;
    type Send = send::Silent;

    fn build_failed(self: Pin<&mut Self>, cause: Self::Build) {
        match cause {}
    }

    fn connection(self: Pin<&Self>, _: &Seed) -> Result<Self::Conn, Self::Build> {
        Ok(())
    }

    fn deadline(self: Pin<&Self>, _: clock::Clock<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }

    fn close<'e>(
        self: Pin<&mut Self>,
        _: conn::Ctx<'e, '_, '_, '_, '_, 'd, ID, Identity, (), Payload<'d>>,
    ) {
        self.get_mut().gate.hit();
    }
}

impl<'d, ID: route::Identity> handler::Borrowed<'d, ID> for Admit {
    fn chunk<R: TryRetain>(
        self: Pin<&mut Self>,
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, Identity, (), Payload<'d>>,
        chunk: R,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Outcome {
        let event = Event::Chunk(chunk.as_ref().len());
        self.events.push(event);
        for (header, body) in [
            (FIRST_HEADER, Some(egress::NonEmpty::from_array(FIRST_BODY))),
            (
                SECOND_HEADER,
                Some(egress::NonEmpty::from_array(SECOND_BODY)),
            ),
            (THIRD_HEADER, Some(egress::NonEmpty::from_array(THIRD_BODY))),
            (FIRST_REPLY.as_slice(), None),
            (SECOND_REPLY.as_slice(), None),
        ] {
            let mut output_slot = std::mem::MaybeUninit::uninit();
            let result = match conn.try_output(driver, &mut output_slot) {
                Ok(mut output) => {
                    let (_, mut prefix) = output.get_mut().parts();
                    prefix.write_slice(header).unwrap();
                    let body = body.map(|body| {
                        Value::Checked(
                            body.try_bound::<o3::buffer::resident::bound::AtMost<6>>()
                                .map_err(|rejected| rejected.into_parts().1)
                                .expect("fixture body bound"),
                        )
                    });
                    match body {
                        Some(body) => write::Output::try_publish(output, body, write::End::Open),
                        None => write::Output::finish(output, write::End::Open),
                    }
                    .map_err(error::Mismatch::into_cause)
                    .expect("publication");
                    Ok(())
                }
                Err(cause) => Err(cause),
            };
            let event = Event::Write(result, conn.pending_output());
            self.events.push(event);
        }
        Outcome::CloseAfter
    }
}
