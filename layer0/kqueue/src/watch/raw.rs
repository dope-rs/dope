use super::{Event, Watch};

pub type Events<'a> = std::iter::Empty<Event<'a>>;

/// # Safety
/// Bytes retain the matching native completion's initialized records.
pub unsafe fn events<'a>(watch: &Watch, _: &'a [u8]) -> Events<'a> {
    match *watch {}
}
