use dope_core::io::transfer;
use dope_net::wire::receive::discard;

const PREFIX: discard::Prefix = discard::Prefix::fixed::<{ transfer::MAX_BYTES }>();
const EXPECTED: discard::Prefix =
    discard::Prefix::new(transfer::NonZero::fixed_size::<{ transfer::MAX_BYTES }>());

#[test]
fn reuses_transfer_domain() {
    assert!(PREFIX == EXPECTED);
    assert!(size_of::<discard::Prefix>() == size_of::<transfer::NonZero>());
}
