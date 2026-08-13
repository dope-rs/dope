use std::{any, cell, os::unix::process::ExitStatusExt as _, panic, process, rc};

use crate::checks::Outcome as _;

fn payload_text(payload: Box<dyn any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_owned(),
            Err(_) => String::new(),
        },
    }
}

pub struct Expectation<'a> {
    needle: Option<&'a str>,
}

impl Expectation<'_> {
    pub const fn any() -> Self {
        Self { needle: None }
    }

    pub const fn containing(needle: &str) -> Expectation<'_> {
        Expectation {
            needle: Some(needle),
        }
    }

    pub fn assert<R>(self, f: impl FnOnce() -> R) {
        let payload = panic::catch_unwind(panic::AssertUnwindSafe(f));
        assert!(payload.is_err(), "expected panic");
        if let (Some(needle), Err(payload)) = (self.needle, payload) {
            let text = payload_text(payload);
            assert!(text.contains(needle), "panic {text:?} lacks {needle:?}");
        }
    }

    pub fn panics_with(f: impl FnOnce(), needle: &str) {
        let payload = panic::catch_unwind(panic::AssertUnwindSafe(f));
        assert!(payload.is_err(), "expected panic");
        let Err(payload) = payload else {
            return;
        };
        let text = payload_text(payload);
        assert!(text.contains(needle), "panic {text:?} lacks {needle:?}");
    }
}

pub struct CountDrop(pub rc::Rc<cell::Cell<usize>>);

impl CountDrop {
    pub fn counter() -> rc::Rc<cell::Cell<usize>> {
        rc::Rc::new(cell::Cell::new(0))
    }
}

impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

pub struct OrderedDrop {
    pub order: rc::Rc<cell::RefCell<Vec<usize>>>,
    pub value: usize,
}

impl Drop for OrderedDrop {
    fn drop(&mut self) {
        self.order.borrow_mut().push(self.value);
    }
}

pub struct Process {
    status: process::ExitStatus,
}

impl Process {
    pub fn respawn(test_name: &str, envs: &[(&str, &str)]) -> Self {
        use std::{env::current_exe, process::Command};
        let executable = current_exe().or_abort("locate current test executable");
        let mut cmd = Command::new(executable);
        cmd.arg("--exact").arg(test_name);
        for (key, value) in envs {
            cmd.env(key, value);
        }
        let status = cmd.status().or_abort("run abort-test subprocess");
        Self { status }
    }

    pub fn expect_abort(self) {
        assert!(!self.status.success());
        assert_eq!(self.status.signal(), Some(libc::SIGABRT));
    }
}
