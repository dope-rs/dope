mod finish;
use dope_core::{
    driver::{retained, route::Identity},
    io::event::net::send::Completion,
};
pub use finish::Finish;
use o3::base::Call as _;
use send::reclaim;

use crate::{
    link::{
        Consumed,
        egress::{
            self, data,
            reserve::{demand, error},
        },
        event::{DirectStatus, SendFailure, SendOutcome},
        pool::{self, recv::input},
        slot::{
            transmit::{self, outcome::Data},
            types,
        },
    },
    wire::{self, send},
};

#[must_use]
pub enum Rejected<B> {
    ConnectionUnavailable(egress::NonEmpty<B>),
    PendingUnavailable(egress::NonEmpty<B>),
    Egress(error::Capacity<B>),
}

/// Exclusive state access after queued and acknowledgement ownership is clear.
pub struct Direct<'a, S> {
    state: &'a mut S,
    status: DirectStatus,
}

impl<'a, S> Direct<'a, S> {
    const fn new(state: &'a mut S, status: DirectStatus) -> Self {
        Self { state, status }
    }

    pub fn into_parts(self) -> (&'a mut S, DirectStatus) {
        (self.state, self.status)
    }
}

const _: () = {
    assert!(
        std::mem::size_of::<Direct<'static, ()>>()
            == std::mem::size_of::<usize>() + std::mem::size_of::<DirectStatus>()
    );
};

pub struct Settlement<
    'a,
    'p,
    'w,
    'd,
    ID: Identity + 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    S,
    M: input::Mode<W>,
    B,
    const IOV: usize,
    D: demand::Mode = demand::Fallible,
> where
    'd: 'w,
{
    pool: &'a mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
    done: Completion<'d>,
}

impl<'a, 'p, 'w, 'd, ID: Identity + 'w, T, W, S, M, B, const IOV: usize, D>
    Settlement<'a, 'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>
where
    'd: 'w,
    T: crate::Transport,
    W: wire::Spec + 'w + 'd,
    M: input::Mode<W>,
    D: demand::Mode,
{
    pub(in crate::link::pool) fn new(
        pool: &'a mut pool::Connections<'p, 'w, 'd, ID, T, W, S, M, B, IOV, D>,
        done: Completion<'d>,
    ) -> Self {
        Self { pool, done }
    }

    pub fn settle<O>(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
        direct: impl for<'scope> FnOnce(Direct<'scope, S>) -> O,
        free: impl FnMut(pool::Key<'d, ID>, B),
        complete: impl FnOnce(&mut types::Connection<'w, 'd, ID, W, S>, &SendOutcome<'d, ID, O>),
    ) -> SendOutcome<'d, ID, O>
    where
        B: data::raw::Payload<'d>,
        M::Ingress: Finish<W>,
    {
        self.run(
            driver,
            move |egress, result| {
                if egress.queue.is_active() || egress.connection.sending().has_ack() {
                    Self::close(egress, result)
                } else {
                    Self::direct(egress, result, direct)
                }
            },
            free,
            complete,
        )
    }

    pub fn settle_owned(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
        complete: impl FnOnce(&mut types::Connection<'w, 'd, ID, W, S>, &SendOutcome<'d, ID, ()>),
    ) -> SendOutcome<'d, ID, ()>
    where
        B: data::raw::Payload<'d>,
        M::Ingress: Finish<W>,
    {
        self.run(driver, Self::close, |_, body| drop(body), complete)
    }

    fn close<O>(
        egress: &mut transmit::Sender<'_, 'p, 'w, 'd, ID, W, S, B, IOV, D>,
        result: Data,
    ) -> SendOutcome<'d, ID, O>
    where
        B: data::raw::Payload<'d>,
    {
        let key = egress.connection.key();
        egress
            .connection
            .sending()
            .abort_queue(&mut egress.queue, key);
        match result {
            Data::Sent(sent) => {
                egress.connection.discard();
                SendOutcome::CloseSent {
                    key,
                    acknowledged: sent.into_len(),
                }
            }
            Data::Rejected(error) => SendOutcome::Rejected {
                key,
                failure: SendFailure::Direct(error),
            },
            Data::Failed(error) => SendOutcome::Failed {
                key,
                failure: SendFailure::Direct(error),
            },
            Data::Closed => {
                egress.connection.discard();
                SendOutcome::Close(key)
            }
        }
    }

    fn direct<O>(
        egress: &mut transmit::Sender<'_, 'p, 'w, 'd, ID, W, S, B, IOV, D>,
        result: Data,
        settle: impl for<'scope> FnOnce(Direct<'scope, S>) -> O,
    ) -> SendOutcome<'d, ID, O>
    where
        B: data::raw::Payload<'d>,
    {
        let status = match result {
            Data::Sent(sent) => {
                let len = sent.into_len();
                let ack = match <<W::Send as send::Policy<W>>::Reclaim as reclaim::Policy>::direct()
                {
                    reclaim::Direct::Independent => None,
                    reclaim::Direct::Retained => Some(Consumed::proven(len)),
                };
                DirectStatus::Sent(ack)
            }
            Data::Rejected(error) => DirectStatus::Rejected(error),
            Data::Failed(error) => DirectStatus::Failed(error),
            Data::Closed => DirectStatus::Closed,
        };
        SendOutcome::Direct {
            key: egress.connection.key(),
            settled: settle(Direct::new(&mut egress.connection.state, status)),
        }
    }

    fn run<O>(
        self,
        driver: &mut retained::Context<'_, '_, 'd>,
        mode: impl FnOnce(
            &mut transmit::Sender<'_, 'p, 'w, 'd, ID, W, S, B, IOV, D>,
            Data,
        ) -> SendOutcome<'d, ID, O>,
        release: impl FnMut(pool::Key<'d, ID>, B),
        complete: impl FnOnce(&mut types::Connection<'w, 'd, ID, W, S>, &SendOutcome<'d, ID, O>),
    ) -> SendOutcome<'d, ID, O>
    where
        B: data::raw::Payload<'d>,
    {
        let Self { pool, done } = self;
        let (token, result) = done.into_parts();
        let key = match pool.keys.parse(token) {
            Ok(key) => key,
            Err(_) => return SendOutcome::Drop,
        };
        let (result, space) = {
            let Some(mut egress) = pool.sender(key.connection()) else {
                return SendOutcome::Drop;
            };
            let (end, space) = {
                use crate::wire::send::Availability;

                match result {
                    Ok(bytes) => {
                        egress
                            .connection
                            .sending()
                            .sent(egress.flights, driver, bytes, key)
                    }
                    Err(error) => (
                        egress.connection.sending().failed(error),
                        Availability::Unchanged,
                    ),
                }
            };
            let key = key.connection();
            let result = {
                use crate::link::slot::transmit::outcome::{Data, End, Terminal};

                match end {
                    Terminal::Pending | Terminal::Drop => SendOutcome::Drop,
                    Terminal::Queued(Data::Sent(sent)) => {
                        match egress.connection.sending().settle(
                            &mut egress.queue,
                            driver.region_token(),
                            key,
                            sent,
                            release,
                        ) {
                            Ok(used) => SendOutcome::Queued {
                                key,
                                acknowledged: used,
                            },
                            Err(error) => {
                                egress
                                    .connection
                                    .sending()
                                    .abort_queue(&mut egress.queue, key);
                                egress.connection.discard();
                                SendOutcome::Invalid { key, error }
                            }
                        }
                    }
                    Terminal::Queued(Data::Rejected(error)) => {
                        egress
                            .connection
                            .sending()
                            .abort_queue(&mut egress.queue, key);
                        SendOutcome::Rejected {
                            key,
                            failure: SendFailure::Queued(error),
                        }
                    }
                    Terminal::Queued(Data::Failed(error)) => {
                        egress
                            .connection
                            .sending()
                            .abort_queue(&mut egress.queue, key);
                        SendOutcome::Failed {
                            key,
                            failure: SendFailure::Queued(error),
                        }
                    }
                    Terminal::Queued(Data::Closed) => {
                        egress
                            .connection
                            .sending()
                            .abort_queue(&mut egress.queue, key);
                        SendOutcome::Close(key)
                    }
                    Terminal::Direct(result) => mode(&mut egress, result),
                    Terminal::Control(End::Sent | End::Closed) => SendOutcome::Terminal { key },
                    Terminal::Control(End::Rejected(error)) => SendOutcome::Rejected {
                        key,
                        failure: SendFailure::Terminal(error),
                    },
                    Terminal::Write(End::Rejected(error)) => SendOutcome::Rejected {
                        key,
                        failure: SendFailure::WriteShutdown(error),
                    },
                    Terminal::Control(End::Failed(error)) => SendOutcome::Failed {
                        key,
                        failure: SendFailure::Terminal(error),
                    },
                    Terminal::Write(End::Failed(error)) => SendOutcome::Failed {
                        key,
                        failure: SendFailure::WriteShutdown(error),
                    },
                    Terminal::Write(End::Sent) => SendOutcome::WriteShutdown { key },
                    Terminal::Write(End::Closed) => SendOutcome::Close(key),
                    Terminal::Invalid(error) => {
                        egress
                            .connection
                            .sending()
                            .abort_queue(&mut egress.queue, key);
                        SendOutcome::Invalid {
                            key,
                            error: error.into(),
                        }
                    }
                }
            };
            egress
                .pending
                .wait_send(egress.connection.send_status().is_waiting());
            complete(egress.connection, &result);
            (result, space)
        };
        if let Some(release) = <M::Ingress as Finish<W>>::release::<ID>(space) {
            release.call(&mut pool.prepared.runtime);
        }
        if space == send::Availability::Released {
            pool.prepared.pending.release_send();
        }
        result
    }
}
