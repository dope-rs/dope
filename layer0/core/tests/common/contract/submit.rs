use dope_core::driver::result;
use o3::alloc;

#[derive(Debug)]
pub enum Error {
    Allocation(alloc::Error),
    Submit(result::SubmitError),
}
