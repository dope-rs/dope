/// The durability boundary requested for a file sync operation.
#[repr(u32)]
pub enum Sync {
    /// Persist file data and the metadata required to retrieve it.
    Data = 1,
    /// Persist all file data and metadata.
    All = 0,
}
