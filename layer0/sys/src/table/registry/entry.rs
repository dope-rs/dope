use crate::table::change;

/// A registered read retaining its operation identity and physical file slot.
/// Identity, file, index, family, and numeric key conversions remain unchanged while registered.
pub trait Entry: Sized {
    type Id: Copy + Eq;
    /// Numeric conversion equals `key(id())` for this entry's echo.
    type Echo: Copy + Eq + Into<usize>;
    type File: change::Id;
    type Family: Copy + Eq;
    type Call: Copy;

    fn id(&self) -> Self::Id;
    fn echo(&self) -> Self::Echo;
    fn file(&self) -> Self::File;
    fn index(&self) -> u32;
    fn family(&self) -> Self::Family;
    fn operation(&self) -> Self::Call;
    fn into_call(self) -> Self::Call;
    fn key(id: Self::Id) -> usize;
}
