/// Both retry indices have reserved space for the retained operation.
pub trait Vacancy {
    fn commit(self);
}
