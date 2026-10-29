use std::os::fd::OwnedFd;

use dope_core::driver::life::domain::{Domain, Generation};

fn admitted(epoch: Generation, domain: Domain, source: OwnedFd) {
    drop(domain.fd(&source));
    drop(epoch.fd(source));
}

fn main() {}
