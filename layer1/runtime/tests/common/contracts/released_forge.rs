use std::pin::Pin;

use dope_runtime::executor;

fn forge<T>(app: Pin<&mut T>) -> executor::raw::Released<'_, T> {
    executor::raw::Released::new(app)
}

fn main() {}
