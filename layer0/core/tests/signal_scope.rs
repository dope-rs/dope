use std::{mem::MaybeUninit, thread};

use dope_core::platform::signal::{Acquire, Error, Source};
use o3::mem::slot;

fn main() {
    let rejected = thread::spawn(|| {
        let mut retained = MaybeUninit::uninit();
        matches!(
            Source::scope(slot::Vacant::new(&mut retained), |_| Ok::<_, ()>(())),
            Err(Error::Acquire(Acquire::Thread))
        )
    })
    .join()
    .expect("signal worker");
    assert!(rejected);

    let mut retained = MaybeUninit::uninit();
    assert!(matches!(
        Source::scope(slot::Vacant::new(&mut retained), |_| {
            let mut nested = MaybeUninit::uninit();
            assert!(matches!(
                Source::scope(slot::Vacant::new(&mut nested), |_| Ok::<_, ()>(())),
                Err(Error::Acquire(Acquire::Busy))
            ));
            Err::<(), _>(7)
        }),
        Err(Error::Run(7))
    ));
    Source::scope(slot::Vacant::new(&mut retained), |_| Ok::<_, ()>(()))
        .expect("released signal scope");
}
