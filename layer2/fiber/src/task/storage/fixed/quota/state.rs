use crate::task::storage::fixed::{
    Capacity,
    quota::{Retired, Vacancy},
};

/// Closed quota behavior used by fixed task planes.
pub trait State: Copy {
    fn capacity(self) -> Capacity;

    fn vacancy(self) -> Option<Vacancy<Self>>;

    fn consume(self, vacancy: Vacancy<Self>) -> Self;

    fn retire(self, retired: Retired) -> Self;
}
