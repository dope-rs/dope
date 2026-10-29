mod queue;
mod vacancy;

pub(crate) use queue::Queue;
pub(crate) use vacancy::Vacancy;

struct Entry<E> {
    event: E,
    suppressed: bool,
}
