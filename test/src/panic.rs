use std::cell::{Cell, RefCell};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::ExitStatus;
use std::rc::Rc;

pub fn assert_unwinds<R>(f: impl FnOnce() -> R) {
    assert!(catch_unwind(AssertUnwindSafe(f)).is_err());
}

fn panic_text(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => payload
            .downcast::<&'static str>()
            .map(|message| (*message).to_owned())
            .unwrap_or_default(),
    }
}

pub fn assert_panics_with(f: impl FnOnce(), needle: &str) {
    let payload = catch_unwind(AssertUnwindSafe(f)).expect_err("expected panic");
    let text = panic_text(payload);
    assert!(text.contains(needle), "panic {text:?} lacks {needle:?}");
}

pub fn counter() -> Rc<Cell<usize>> {
    Rc::new(Cell::new(0))
}

pub struct CountDrop(pub Rc<Cell<usize>>);

impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

pub struct OrderedDrop {
    pub order: Rc<RefCell<Vec<usize>>>,
    pub value: usize,
}

impl Drop for OrderedDrop {
    fn drop(&mut self) {
        self.order.borrow_mut().push(self.value);
    }
}

pub fn respawn_self(test_name: &str, envs: &[(&str, &str)]) -> ExitStatus {
    let mut cmd = std::process::Command::new(std::env::current_exe().expect("current test binary"));
    cmd.arg("--exact").arg(test_name);
    for (key, value) in envs {
        cmd.env(key, value);
    }
    cmd.status().expect("respawn self")
}

pub fn expect_abort(status: ExitStatus) {
    assert!(!status.success());
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(libc::SIGABRT));
    }
}
