use dope_sys::{engine::ring::Context, io::Close};

use crate::reactor::{Held, ring::Ready};

pub(crate) type Closing<C> = Held<
    <Ready<<C as Context>::Token, <C as Context>::Layout> as Close>::State,
    <C as Context>::Tuning,
>;
