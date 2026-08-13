use std::pin;

// SAFETY: Storage drops its pinned entries before its pinned queue, so the
// binding's retained ready link cannot outlive the queue.
unsafe impl<'a, 'd> crate::raw::StableRootBindingSource<'a, 'd> for super::Source<'a, 'd> {
    fn context(self) -> pin::Pin<&'a crate::raw::RootBinding<'d>> {
        self.context
    }
}
