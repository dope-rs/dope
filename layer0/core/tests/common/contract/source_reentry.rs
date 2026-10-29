use std::os::fd::OwnedFd;

use dope_core::driver::life::domain::{Domain, Generation};

fn generation(owner: Generation<OwnedFd>, source: OwnedFd) {
    let _ = owner.fd(source);
}

fn domain(owner: Domain<OwnedFd>, source: OwnedFd) {
    let _ = owner.fd(source);
}

fn main() {}
