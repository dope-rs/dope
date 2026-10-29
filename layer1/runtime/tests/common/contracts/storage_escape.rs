use dope_core::driver::storage;
use dope_runtime::topology::recipe::{Borrow, Build};
fn widen<'a, 'b: 'a>(storage: &'a String) -> &'b String {
    match <Borrow as Build<storage::Value<String>>>::build(Borrow, storage) {
        Ok(value) => value,
        Err(error) => match error {},
    }
}

fn main() {}
