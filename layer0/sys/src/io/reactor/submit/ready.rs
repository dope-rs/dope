/// Initialized entries borrowing their exact reserved queue capacity.
pub trait Ready {
    fn push(self);
}
