pub mod connector;
pub mod datagram;
pub mod dispatch;
pub mod file;
pub mod listener;
pub mod receive;
pub mod service;
pub mod timing;

use std::marker;

use dope_core::{driver::settings, io};
use dope_net::wire;
use listener::config;
pub trait Env {
    type Transport: dope_net::Transport;
    type Wire: wire::Wire;
    type Driver: settings::Profile;
    type Timing: timing::Policy;
    type Admission: config::Admission;
}

type Variance<T, W, D, F, A> = fn() -> (T, W, D, F, A);

pub struct Bundle<T, W, D, F = D, A = config::StandardAdmission>(
    marker::PhantomData<Variance<T, W, D, F, A>>,
);

impl<T, W, D, F, A> Env for Bundle<T, W, D, F, A>
where
    T: dope_net::Transport,
    W: wire::Wire,
    D: settings::Profile,
    F: timing::Policy,
    A: config::Admission,
{
    type Transport = T;
    type Wire = W;
    type Driver = D;
    type Timing = F;
    type Admission = A;
}

type DriverEvent<'d> = io::Event<'d>;
type ReadEvent = io::ReadEvent;
type RecvEvent<'d> = io::RecvEvent<'d>;
type StatEvent = io::StatEvent;
pub enum Outcome {
    Ok,
    Capacity,
    Overrun,
    CloseAfter,
}
