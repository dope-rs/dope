use std::{mem, pin};

/// # Safety
/// Both projections retain the same pinned application through its final traversal.
/// The enclosing lifecycle ends all projection access before releasing its storage.
pub unsafe trait Project {
    type App;

    fn shared(self: pin::Pin<&Self>) -> pin::Pin<&Self::App>;

    fn exclusive(self: pin::Pin<&mut Self>) -> pin::Pin<&mut Self::App>;
}

// SAFETY: the original mutable Pin retains its owner at the same address.
unsafe impl<T> Project for pin::Pin<&mut T> {
    type App = T;

    fn shared(self: pin::Pin<&Self>) -> pin::Pin<&T> {
        self.get_ref().as_ref()
    }

    fn exclusive(self: pin::Pin<&mut Self>) -> pin::Pin<&mut T> {
        self.get_mut().as_mut()
    }
}

// SAFETY: ManuallyDrop is transparent and the enclosing owner pins its value
// until final traversal releases it exactly once.
unsafe impl<T> Project for mem::ManuallyDrop<T> {
    type App = T;

    fn shared(self: pin::Pin<&Self>) -> pin::Pin<&T> {
        // SAFETY: the projection retains the enclosing pin and initialized value.
        unsafe { self.map_unchecked(|value| &**value) }
    }

    fn exclusive(self: pin::Pin<&mut Self>) -> pin::Pin<&mut T> {
        // SAFETY: the projection retains the enclosing pin without moving T.
        unsafe { self.map_unchecked_mut(|value| &mut **value) }
    }
}
