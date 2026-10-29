use crate::io::socket::option::Step;

/// Advances an option sequence whose exact nonzero count was reserved.
pub trait Cursor {
    type Step: Step;

    fn pop(&mut self) -> Self::Step;
}
