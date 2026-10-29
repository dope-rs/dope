use common::random::Purpose;
use dope_runtime::random::{Entropy, Process};

pub mod common;

#[test]
fn permutation_is_stable() {
    let seed = Process::<Purpose>::random().expect("seed entropy");
    let expected = seed.permute::<0x7061_7468>(17).into_bytes();
    assert_eq!(seed.permute::<0x7061_7468>(17).into_bytes(), expected);

    let mut images = (0..=u16::MAX)
        .map(|value| seed.permute::<0x7061_7468>(u64::from(value)).into_bytes())
        .collect::<Vec<_>>();
    images.sort_unstable();
    assert!(images.windows(2).all(|pair| pair[0] != pair[1]));
}

#[test]
fn separates_operations() {
    let root = Process::<Purpose>::random().expect("seed entropy");
    let value = 17_u64;
    let mut left = (value >> 32) as u32;
    let mut right = value as u32;
    for round in 0..4_u64 {
        let input = (round << 32) | u64::from(right);
        let word = u64::from_be_bytes(root.word::<0x7061_7468>(input).into_bytes());
        let next = left ^ word as u32;
        left = right;
        right = next;
    }
    assert_ne!(
        ((u64::from(left) << 32) | u64::from(right)).to_be_bytes(),
        root.permute::<0x7061_7468>(value).into_bytes()
    );
}
