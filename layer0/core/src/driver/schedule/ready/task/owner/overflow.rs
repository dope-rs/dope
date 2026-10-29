use o3::depot::capacity;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Overflow {
    actual: usize,
}

impl From<capacity::Error> for Overflow {
    fn from(error: capacity::Error) -> Self {
        Self {
            actual: error.requested(),
        }
    }
}
