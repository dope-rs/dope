extern crate dope;
use dope_fiber::{ErasedTaskId, TaskId};

fn require_send<T: Send>() {}
fn require_sync<T: Sync>() {}

fn main() {
    require_send::<TaskId>();
    require_sync::<TaskId>();
    require_send::<ErasedTaskId>();
    require_sync::<ErasedTaskId>();
}
