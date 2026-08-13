use crate::{backend, backend::uring::engine::submit, platform::reactor};

impl reactor::Source for backend::Uring {
    type Queue<'a> = submit::Queue<'a>;

    fn queue(&mut self) -> Self::Queue<'_> {
        submit::Queue::new(self)
    }
}
