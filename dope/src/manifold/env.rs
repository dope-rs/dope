use std::marker::PhantomData;

use crate::backend;
use crate::transport::Transport;
use crate::transport::wire::Wire;

pub trait Env {
    type Transport: Transport;
    type Wire: Wire;
    type Profile: backend::profile::Profile;
}

type Variance<T, W, F> = fn() -> (T, W, F);

pub struct Bundle<T, W, F>(PhantomData<Variance<T, W, F>>);

impl<T, W, F> Env for Bundle<T, W, F>
where
    T: Transport,
    W: Wire,
    F: backend::profile::Profile,
{
    type Transport = T;
    type Wire = W;
    type Profile = F;
}
