use std::{cell::Cell, num::NonZeroUsize, ops::ControlFlow};

use dope_sys::io::socket::option::{self, Value};

pub struct Options<'a> {
    pub calls: &'a Cell<libc::c_int>,
    pub count: NonZeroUsize,
}

pub struct Step {
    value: Value,
    last: bool,
}

impl option::Cursor for Options<'_> {
    type Step = Step;

    fn pop(&mut self) -> Self::Step {
        let value = self.calls.get();
        self.calls.set(value + 1);
        Step {
            value: Value::new(value),
            last: self.calls.get() as usize >= self.count.get(),
        }
    }
}

impl option::Step for Step {
    fn kind(&self) -> ControlFlow<option::Kind, option::Kind> {
        if self.last {
            ControlFlow::Break(option::Kind::RecvBuffer)
        } else {
            ControlFlow::Continue(option::Kind::RecvBuffer)
        }
    }

    fn value(&self) -> &Value {
        &self.value
    }
}
