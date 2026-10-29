use crate::io::reactor::{complete, submit::Ready};

/// One exact reserved queue slot, retaining every prepared entry's borrow.
pub trait Vacancy<'q, T: complete::raw::Token>: Sized {
    type Entry;
    type Owned;
    type Borrowed<'r>;
    type Submit<'r>;
    type Prepared<'e, E: 'e>: Ready;

    fn push_owned(self, entry: Self::Entry, token: T);

    /// # Safety
    /// Every encoded address must remain live through terminal completion or quiescence.
    unsafe fn push<'r>(self, entry: Self::Borrowed<'r>, token: T);

    /// # Safety
    /// Every encoded address must remain live through terminal completion or quiescence.
    unsafe fn borrow<'e, 'r>(
        self,
        entry: &'e Self::Submit<'r>,
    ) -> Self::Prepared<'e, Self::Submit<'r>>;

    fn prepare<'e>(self, entry: &'e Self::Owned) -> Self::Prepared<'e, Self::Owned>;
}
