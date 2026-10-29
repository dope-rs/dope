use o3::{alloc, buffer::pool};

use crate::io::reactor;

#[derive(Debug)]
pub enum Create {
    Buffer(pool::CreateError),
    Allocation(alloc::Error),
    Queue(alloc::Error),
    Reactor(reactor::Create),
}
