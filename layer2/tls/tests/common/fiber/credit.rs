use dope_test::harness::peer;

use crate::common::fiber::{Connect, FIRST, Peer};

pub const AFTER_RELEASE: &[u8] = b"after-release";
pub const INDEPENDENT: &[u8] = b"independent";
pub const READY: &[u8] = b"ready";

pub fn exchange(
    [held, other]: &mut [&peer::Stream<'_>; 2],
    pubkey: [u8; 32],
    body: &[u8],
    joined: bool,
    fail: bool,
) -> Result<(), Connect> {
    let mut held = Peer::establish(held, pubkey)?;
    let mut other = Peer::establish(other, pubkey)?;
    if fail {
        other.try_send(READY)?;
        assert_eq!(held.try_recv()?, READY);
    }
    if joined {
        let mut wire = held.seal(FIRST)?;
        wire.extend_from_slice(&held.seal(body)?);
        held.socket.write_all(&wire).map_err(Connect::Peer)?;
    } else {
        held.try_send(body)?;
    }
    if fail {
        other.try_send(INDEPENDENT)?;
        assert_eq!(held.try_recv()?, READY);
        held.try_send(AFTER_RELEASE)?;
    } else {
        assert_eq!(other.try_recv()?, READY);
        held.try_send(AFTER_RELEASE)?;
        other.try_send(INDEPENDENT)?;
    }
    Ok(())
}
