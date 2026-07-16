use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::abort;

#[cold]
pub fn abort_on_drop_panic<T>(payload: T) {
    let outcome = ManuallyDrop::new(catch_unwind(AssertUnwindSafe(|| drop(payload))));
    if outcome.is_err() {
        abort();
    }
}
