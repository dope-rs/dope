use dope_runtime::{
    random,
    random::{Domain, Namespace},
};

pub struct Purpose;

pub const HASH_INPUT: (&[u8], &[u8]) = (b"name", b"value");
pub const HASH_DOMAIN: Namespace = Namespace::new(*b"testhash");

impl random::Purpose for Purpose {
    const DOMAIN: Domain = Namespace::new(*b"test\0rng").fixed();
}
