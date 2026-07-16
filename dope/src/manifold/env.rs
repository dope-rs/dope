use std::marker::PhantomData;

use crate::runtime::profile::RuntimeProfile;
use dope_net::Transport;
use dope_net::wire::Wire;

pub trait Env {
    type Transport: Transport;
    type Wire: Wire;
    type Profile: RuntimeProfile;
}

type Variance<T, W, F> = fn() -> (T, W, F);

pub struct Bundle<T, W, F>(PhantomData<Variance<T, W, F>>);

impl<T, W, F> Env for Bundle<T, W, F>
where
    T: Transport,
    W: Wire,
    F: RuntimeProfile,
{
    type Transport = T;
    type Wire = W;
    type Profile = F;
}
