pub trait Policy {
    type Receipt<R>;

    fn observe<R>(capture: impl FnOnce() -> R) -> Self::Receipt<R>;

    fn publish<R>(saved: Self::Receipt<R>, capture: impl FnOnce() -> R) -> R;
}

pub struct Raw;
pub struct Decoded;

impl Policy for Raw {
    type Receipt<R> = R;

    fn observe<R>(capture: impl FnOnce() -> R) -> R {
        capture()
    }

    fn publish<R>(saved: R, _: impl FnOnce() -> R) -> R {
        saved
    }
}

impl Policy for Decoded {
    type Receipt<R> = ();

    fn observe<R>(_: impl FnOnce() -> R) {}

    fn publish<R>((): (), capture: impl FnOnce() -> R) -> R {
        capture()
    }
}
