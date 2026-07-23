use std::cell::Cell;
use std::convert::Infallible;
use std::marker::PhantomData;

use dope_fiber::{Fiber, SplitTask, SplitView};

struct Escape<'outer>(PhantomData<&'outer ()>);

impl<'d, 'outer> SplitTask<'d> for Escape<'outer> {
    type Input = &'outer Cell<Option<SplitView<'outer>>>;
    type State = ();
    type Context = ();
    type Output = ();
    type Error = Infallible;

    fn build<'req>(
        view: SplitView<'req>,
        escaped: Self::Input,
        _state: &'req Self::State,
        _context: &'req Self::Context,
    ) -> Result<impl Fiber<'d, Output = Self::Output> + 'req, Self::Error>
    where
        'd: 'req,
    {
        escaped.set(Some(view));
        Ok(dope_fiber::ready(()))
    }
}

fn main() {}
