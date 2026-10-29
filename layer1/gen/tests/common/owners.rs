use std::cell::Cell;

use dope_test::fibers::Yield;

pub struct Guard<'a>(pub &'a Cell<usize>);

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

impl Guard<'_> {
    #[fibers::fiber_fn('d, crate = ::fibers)]
    pub async fn receiver<'d>(self) {}
}

#[fibers::fiber_fn('d, crate = ::fibers)]
pub(crate) async fn named<'a, 'd>(_guard: Guard<'a>) {}

#[fibers::fiber_fn('d, crate = ::fibers)]
pub(crate) async fn destructured<'a, 'd>(_guards @ (_, _): (Guard<'a>, Guard<'a>)) {}

pub struct Order<'a>(pub &'a Cell<usize>, pub usize);

impl Drop for Order<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get() * 10 + self.1);
    }
}

pub async fn original<'a>(
    mut value: usize,
    (ref mut first, _): (Order<'a>, Order<'a>),
    _: Order<'a>,
) {
    let value = &mut value;
    *value += first.1;
}

#[fibers::fiber_fn('d, crate = ::fibers)]
pub(crate) async fn patterns<'a, 'd>(
    ref mut value: usize,
    (ref mut first, _): (Order<'a>, Order<'a>),
    _last: Order<'a>,
) -> (usize, usize) {
    *value += 1;
    Yield::new().await;
    first.1 += 3;
    (*value, first.0.get())
}
