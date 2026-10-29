use dope_core::driver::settings;
use dope_net::link::pool::recv::input;

use crate::{app::env::quota, timing};

pub trait Spec {
    type Transport: dope_net::Transport;
    type Wire: input::Execution;
    type Driver: settings::Profile;
    type Timing: timing::Policy;
    type Admission: quota::Policy;
}
