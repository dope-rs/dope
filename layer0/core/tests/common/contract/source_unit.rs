use dope_core::driver::life::domain::{Domain, Generation};

fn generation(owner: Generation) {
    let _ = owner.fd(());
}

fn domain(owner: Domain) {
    let _ = owner.fd(());
}

fn main() {}
