use o3::mem::quota;

#[derive(Debug)]
pub enum Error {
    Empty,
    Exhausted,
}

pub trait Admission<T> {
    fn into_item(self) -> Result<T, Error>;
}

impl<T> Admission<T> for quota::Admission<T> {
    fn into_item(self) -> Result<T, Error> {
        match self {
            Self::Item(item) => Ok(item),
            Self::Empty => Err(Error::Empty),
            Self::Exhausted => Err(Error::Exhausted),
        }
    }
}
