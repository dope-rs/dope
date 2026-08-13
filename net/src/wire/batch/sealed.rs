use std::{array, iter, num, option};

use crate::wire::batch;

unsafe impl<T> batch::raw::Source for iter::Empty<T> {}

unsafe impl<T> batch::raw::Source for iter::Once<T> {}

unsafe impl<T> batch::raw::Source for option::IntoIter<T> {}

unsafe impl<T, const N: usize> batch::raw::Source for array::IntoIter<T, N> {
    const MAX_ITEMS: num::NonZeroUsize = match num::NonZeroUsize::new(N) {
        Some(limit) => limit,
        None => num::NonZeroUsize::MIN,
    };
}
