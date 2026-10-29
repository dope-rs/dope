use dope_test::quic::wire::ShortHeader;

#[test]
fn observes_cid_width() {
    let mut plain = vec![0x40];
    plain.extend_from_slice(&[0x31; 8]);
    plain.extend_from_slice(b"protected");
    assert_eq!(ShortHeader::new(&plain).cid(8), Some([0x31; 8].as_slice()));

    let mut routed = vec![0x5f];
    routed.extend_from_slice(&[0x07]);
    routed.extend_from_slice(&[0x42; 8]);
    assert_eq!(
        ShortHeader::new(&routed).cid(9),
        Some([0x07, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42].as_slice())
    );

    assert_eq!(ShortHeader::new(&plain).cid(0), None);
    assert_eq!(ShortHeader::new(&plain[..8]).cid(8), None);
    plain[0] = 0xc0;
    assert_eq!(ShortHeader::new(&plain).cid(8), None);
}
