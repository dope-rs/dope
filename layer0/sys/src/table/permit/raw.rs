/// Exact scalar identity for a permit generation.
/// # Safety
/// `identity` is deterministic, non-panicking, non-reentrant, and injective across every key presented to one permit FIFO.
pub unsafe trait Key: Copy {
    fn identity(self) -> u64;
}
