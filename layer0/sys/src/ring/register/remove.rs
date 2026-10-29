/// Failed removal returns the original occupied slot and exact syscall error.
#[derive(Debug)]
pub struct Remove<F, E> {
    pub file: F,
    pub failure: E,
}
