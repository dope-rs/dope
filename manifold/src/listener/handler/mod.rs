use std::{pin, time};

use dope_core::driver::retained;
use dope_net::wire;
use o3::buffer::bytes;

use crate::{listener::connection, receive};

mod policy;

pub(crate) use policy::Policy;

pub trait Mode<'d, const ID: u8, A>: receive::Mode<A::Wire> + Policy<'d, ID, A>
where
    A: Application<'d, ID>,
{
}

pub trait BorrowedApplication<'d, const ID: u8>:
    Application<'d, ID, Input = receive::Borrowed>
{
    fn chunk<R: bytes::Retainable>(
        self: pin::Pin<&mut Self>,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn>,
        chunk: R,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome;
}

pub trait RetainedApplication<'d, const ID: u8>:
    Application<'d, ID, Input = receive::Retained>
{
    const RETENTION: receive::Retention;

    fn retained_chunk(
        self: pin::Pin<&mut Self>,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn>,
        chunk: <Self::Wire as wire::Wire>::RetainedRecv<'d>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome;
}

impl<'d, const ID: u8, A> Mode<'d, ID, A> for receive::Borrowed where A: BorrowedApplication<'d, ID> {}

impl<'d, const ID: u8, A> Mode<'d, ID, A> for receive::Retained where A: RetainedApplication<'d, ID> {}

pub trait Application<'d, const ID: u8>: Sized {
    type Conn: Default;
    type Wire: wire::Wire;
    type Input: Mode<'d, ID, Self>;

    fn connection(self: pin::Pin<&Self>) -> Self::Conn {
        Self::Conn::default()
    }

    fn deadline(self: pin::Pin<&Self>) -> Option<time::Instant>;

    fn send(
        self: pin::Pin<&mut Self>,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn>,
        sent: usize,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let _ = (self, connection, sent, driver);
    }

    /// Activates application work that became ready without socket progress.
    fn activate(
        self: pin::Pin<&mut Self>,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        let _ = (self, connection, driver);
    }

    fn close(
        self: pin::Pin<&mut Self>,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn>,
    ) {
        let _ = (self, connection);
    }

    fn defer_close(
        self: pin::Pin<&Self>,
        connection: connection::Ref<'_, 'd, ID, Self::Wire, Self::Conn>,
    ) -> bool {
        let _ = (self, connection);
        false
    }

    fn accept(
        self: pin::Pin<&mut Self>,
        connection: connection::Ctx<'_, 'd, ID, Self::Wire, Self::Conn>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> crate::Outcome {
        let _ = (self, connection, driver);
        <crate::Outcome>::Ok
    }
}
