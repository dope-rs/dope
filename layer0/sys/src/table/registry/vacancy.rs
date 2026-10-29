/// A read operation with reserved storage for its exact kernel echo.
pub trait Vacancy {
    type Echo;

    fn insert(self) -> Self::Echo;
}
