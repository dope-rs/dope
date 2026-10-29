use dope_core::driver::settings;
use dope_net::link::pool::recv::input;

use crate::{
    app::env::{self, quota},
    timing,
};

pub struct Bundle<T, W, D, F = D, A = quota::Standard>(crate::Family<(T, W, D, F, A)>);

impl<T, W, D, F, A> env::Spec for Bundle<T, W, D, F, A>
where
    T: dope_net::Transport,
    W: input::Execution,
    D: settings::Profile,
    F: timing::Policy,
    A: quota::Policy,
{
    type Transport = T;
    type Wire = W;
    type Driver = D;
    type Timing = F;
    type Admission = A;
}
