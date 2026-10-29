use std::{convert::Infallible, io, io::Write as _};

use dope_manifold::client::{
    close::Reason,
    session::codec::{Codec, Finish, Input, Parse, Stateless, eof, staging::Buffered},
};
use dope_test::harness::peer;
use o3::{buffer, buffer::resident::staging};

pub struct Exhausted;

impl Codec for Exhausted {
    type Head<'input, 'd, R: buffer::Cursor>
        = ()
    where
        R: 'input,
        'd: 'input;
    type Error = Infallible;
    type Staging = Buffered;

    fn staging(&self) -> Result<staging::Plan, staging::Error> {
        staging::Plan::new(1, 1)
    }
}

impl Stateless for Exhausted {
    fn parse<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        input: Input<'id, 'input, 'd, R>,
    ) -> Result<Parse<'id, ()>, Infallible>
    where
        'd: 'input,
    {
        Ok(match input.need_more(None) {
            Some(need) => Parse::More(need),
            None => Parse::CapacityExhausted,
        })
    }
}

impl eof::Finish for Exhausted {
    fn finish<'id, 'input, 'd, R: buffer::Cursor>(
        &self,
        input: Input<'id, 'input, 'd, R>,
    ) -> Result<Finish<()>, Infallible>
    where
        'd: 'input,
    {
        if input.is_empty() {
            Ok(Finish::Empty)
        } else {
            Ok(Finish::CapacityExhausted)
        }
    }
}

pub struct Run<C: Codec> {
    pub codec: C,
    pub error: Option<C::Error>,
    pub reason: Reason,
}

pub fn serve(acceptor: &mut peer::TcpAcceptor<'_>) -> io::Result<()> {
    let mut first = acceptor.accept()?;
    first.write_all(&[0xff])?;
    let second = acceptor.accept()?;
    let release = acceptor.accept()?;
    drop((second, release));
    Ok(())
}
