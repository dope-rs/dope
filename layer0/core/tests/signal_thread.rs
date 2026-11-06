use std::mem::MaybeUninit;

use dope_core::platform::signal::{Acquire, Error, Source};
use o3::mem::slot;

#[test]
fn rejects_worker() {
    let mut retained = MaybeUninit::uninit();
    assert!(matches!(
        Source::scope(slot::Vacant::new(&mut retained), |_| Ok::<_, ()>(())),
        Err(Error::Acquire(Acquire::Thread))
    ));
}
