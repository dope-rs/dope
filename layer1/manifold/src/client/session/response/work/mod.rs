use std::mem;

use dope_core::driver::schedule::app;
pub(crate) use seal::Seal;

mod seal;

pub trait Admission: Seal {
    fn admit<T>(&mut self, run: impl FnOnce() -> T) -> Capacity<T> {
        self.admit_with((), |()| run())
    }

    fn admit_with<T, U>(&mut self, value: T, run: impl FnOnce(T) -> U) -> Capacity<U, T>;
}

#[must_use = "capacity reports whether response work was admitted"]
pub enum Capacity<T, C = ()> {
    Available(T),
    Exhausted(C),
}

impl Admission for app::Work<'_, '_> {
    fn admit_with<T, U>(&mut self, value: T, run: impl FnOnce(T) -> U) -> Capacity<U, T> {
        match self.permit() {
            Some(permit) => Capacity::Available(permit.run(|| run(value))),
            None => Capacity::Exhausted(value),
        }
    }
}

pub(crate) struct Reserve<'turn, 'd> {
    work: app::Work<'turn, 'd>,
    credit: Option<app::Permit<'turn, 'd>>,
}

impl<'turn, 'd> Reserve<'turn, 'd> {
    pub(crate) const fn new(work: app::Work<'turn, 'd>, permit: app::Permit<'turn, 'd>) -> Self {
        Self {
            work,
            credit: Some(permit),
        }
    }
}

impl Admission for Reserve<'_, '_> {
    fn admit_with<T, U>(&mut self, value: T, run: impl FnOnce(T) -> U) -> Capacity<U, T> {
        match self.credit.take().or_else(|| self.work.permit()) {
            Some(permit) => Capacity::Available(permit.run(|| run(value))),
            None => Capacity::Exhausted(value),
        }
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Reserve<'static, 'static>>() == 2 * word);
    assert!(mem::size_of::<Capacity<&'static u8>>() == word);
    assert!(mem::size_of::<Capacity<usize, usize>>() == 2 * word);
};
