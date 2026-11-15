pub mod common;
use common::suite::cid::contract::Wire;

#[test]
fn replays_lifecycle() {
    let mut wire = Wire::new().expect("lifecycle peers");
    wire.probe(0).expect("initial delivery");
    wire.rotate().expect("initial CID retirement");
    wire.probe(1).expect("delivery after rotation");
    wire.rebind(2).expect("delivery on rebound path");
    wire.rotate().expect("rebound CID retirement");
    wire.cycle().expect("connection replacement and retirement");
    wire.probe(3).expect("replacement delivery");
    assert_eq!(wire.counts(), (1, 1, 4));
}
