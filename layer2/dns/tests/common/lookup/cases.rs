use dope_dns::lookup::error::Kind;

use crate::common::lookup::server::{Mode, Reply};

static DUP: [[u8; 4]; 2] = [[192, 0, 2, 1], [192, 0, 2, 1]];
static MANY: [[u8; 4]; 2] = [[192, 0, 2, 1], [192, 0, 2, 2]];

pub const FILTER: Mode<'_> = Mode::Reply(Reply::Addrs(&DUP, true, true));
pub const ALIAS: Mode<'_> =
    Mode::Reply(Reply::Alias(Some([192, 0, 2, 2]), b"\x04edge\x07example\0"));
pub const FULL: Mode<'_> = Mode::Reply(Reply::Addrs(&MANY, false, false));
pub const CAPACITY: Result<u8, Kind> = Err(Kind::Capacity {
    limit: 1,
    actual: 2,
});
