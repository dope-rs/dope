use dope_sys::{
    engine,
    io::{
        file,
        file::Opened as _,
        reactor::complete::{self, Decode, Outcome as _, Value as _, resolve::Reclaimed},
        socket,
    },
    kernel::{self, Returned as _},
    memory::buffer::provided,
    probe::Stopped,
    ring,
    table::token::{self, host::Flight as _},
};
use o3::base::num::bounded;

use crate::{
    driver::{
        life::native,
        receive::tokens,
        result::poll::fault::{self, Violation},
        retained::flight,
        route::{self, indices::SlotIndex, kind, kind::Shape},
    },
    io::{
        self, Event,
        event::{
            Kind,
            batch::Flow,
            net::{accept, recv::Completion},
        },
        fd::{domain, handles},
        outputs, recv,
        socket::Create,
        transfer,
    },
    ring::cqe::{self, Envelope, Opened, Parts, Rejected, Returned, Selected, Value},
};

/// One native packet and its original validated flight, consumed together.
pub(crate) struct Binding<'q, 'd, P> {
    packet: Envelope<P>,
    flight: flight::Complete<'q, 'd>,
}

pub(super) type Retired<'d, N, P> =
    Reclaimed<tokens::Token<'d, Selected<N>>, native::Report, Rejected<P>>;

impl<'q, 'd, N, P> Binding<'q, 'd, P>
where
    P: ring::Packet<Native = N, Parts = Parts<N>, Buffer = tokens::Token<'d, Selected<N>>>,
    N: ring::Source
        + token::Source
        + provided::Source
        + file::Completion
        + complete::Observe<cqe::Token<N>>,
    cqe::Observation<N>: complete::Record<cqe::Token<N>, Outcome = cqe::Scalar<N>, Flags = cqe::Flags<N>>
        + Into<<crate::Engine as complete::Observe>::Observation>,
    Returned<N>: kernel::Returned<Value = Value<N>, Invalid = <cqe::Scalar<N> as complete::Outcome>::Invalid>
        + Into<<crate::Backend as kernel::Source>::Returned>,
    Opened<N>: file::raw::Completion<
            Outcome = cqe::Scalar<N>,
            Invalid = <cqe::Scalar<N> as complete::Outcome>::Invalid,
        >,
    cqe::Completed<N>: Into<<crate::Engine as engine::Buffer>::Token>,
{
    pub(super) const fn new(packet: Envelope<P>, flight: flight::Complete<'q, 'd>) -> Self {
        Self { packet, flight }
    }

    pub(super) fn resolve(self) -> Result<Event<'d>, Rejected<P>> {
        match self.flight.kind().into() {
            Shape::Socket(proof) => {
                let (this, creation) = self.plain()?.creation(proof)?;
                let (this, outcome) = this.check(|packet| packet.0.parts().outcome.unit())?;
                let driver = this.flight.driver();
                let target = this.flight.complete().bind(proof);
                Ok(match outcome {
                    Ok(()) => match handles::Created::complete(creation, driver) {
                        handles::Admission::Ready(created) => {
                            outputs::socket(target, created, driver)
                        }
                        handles::Admission::Retired(()) => outputs::socket_gone(target),
                    },
                    Err(errno) => {
                        driver.outbound().fail_create(creation);
                        outputs::socket_error(
                            target,
                            Create::from_native(socket::Create::socket(errno)),
                        )
                    }
                })
            }
            Shape::Open(proof) => {
                let (this, opened) = self.opened()?;
                let target = this.flight.complete().bind(proof);
                Ok(match opened.deliver() {
                    Ok(fd) => outputs::opened(target, fd),
                    Err(errno) => outputs::open_failed(target, errno),
                })
            }
            Shape::Accept(proof) => {
                let (this, outcome) = self.accepting()?;
                let flow = this.packet.flow();
                let driver = this.flight.driver();
                let target = this.flight.settle(flow).bind(proof);
                Ok(match outcome {
                    Ok(accepted) => outputs::accepted(target, accepted, flow, driver),
                    Err(errno) => outputs::accept_error(target, errno, flow),
                })
            }
            Shape::Recv(proof) => self.recv(proof),
            Shape::Send(proof) => {
                let (this, value) = self.plain()?.check(Envelope::bytes)?;
                Ok(outputs::sent(this.flight.complete().bind(proof), value))
            }
            Shape::Write(proof) => {
                let (this, value) = self.plain()?.check(Envelope::bytes)?;
                Ok(outputs::written(this.flight.complete().bind(proof), value))
            }
            Shape::Connect(proof) => {
                let (this, value) = self.plain()?.check(Envelope::unit)?;
                Ok(outputs::connected(
                    this.flight.complete().bind(proof),
                    value,
                ))
            }
            Shape::Sync(proof) => {
                let (this, value) = self.plain()?.check(Envelope::unit)?;
                Ok(outputs::synced(this.flight.complete().bind(proof), value))
            }
            Shape::Tuning(proof) => {
                let (this, value) = self.plain()?.check(Envelope::unit)?;
                Ok(Kind::Tuning((this.flight.bind(proof), value).into()).into())
            }
        }
    }

    pub(super) fn reclaim(self) -> Result<Retired<'d, N, P>, Rejected<P>> {
        let (packet, report) = match self.flight.kind().into() {
            Shape::Socket(proof) => return self.drop_socket(proof),
            Shape::Open(proof) => return self.drop_open(proof),
            Shape::Accept(proof) => return self.drop_accept(proof),
            Shape::Recv(proof) => return self.drop_recv(proof),
            Shape::Send(proof) => {
                let (this, result) = self.plain()?.check(Envelope::bytes)?;
                let report = io::Reclaimed::Send {
                    echo: fault::Echo::new(this.flight.bind(proof).end(Flow::Last)),
                    result,
                };
                (this.packet, report)
            }
            Shape::Write(proof) => {
                let (this, result) = self.plain()?.check(Envelope::bytes)?;
                let report = io::Reclaimed::Write {
                    echo: fault::Echo::new(this.flight.bind(proof).end(Flow::Last)),
                    result,
                };
                (this.packet, report)
            }
            Shape::Connect(proof) => {
                let (this, result) = self.plain()?.check(Envelope::unit)?;
                let report = io::Reclaimed::Connect {
                    echo: fault::Echo::new(this.flight.bind(proof).end(Flow::Last)),
                    result,
                };
                (this.packet, report)
            }
            Shape::Sync(proof) => {
                let (this, result) = self.plain()?.check(Envelope::unit)?;
                let report = io::Reclaimed::Sync {
                    echo: fault::Echo::new(this.flight.bind(proof).end(Flow::Last)),
                    result,
                };
                (this.packet, report)
            }
            Shape::Tuning(proof) => {
                let (this, result) = self.plain()?.check(Envelope::unit)?;
                let report = io::Reclaimed::Tuning {
                    echo: fault::Echo::new(this.flight.bind(proof).end(Flow::Last)),
                    result,
                };
                (this.packet, report)
            }
        };
        Ok(Reclaimed {
            buffer: packet.0.into_parts().1,
            report: Some(native::Report(report)),
            result: Ok(()),
        })
    }

    fn reject(self, cause: Violation) -> Rejected<P> {
        let flow = self.packet.flow();
        let target = self.flight.settle(flow).into_token();
        self.packet.reject(fault::Subject::Target(target), cause)
    }

    fn invalid(self, cause: Returned<N>) -> Rejected<P> {
        Stopped {
            state: (self.packet, self.flight),
            cause,
        }
        .into()
    }

    fn check<R>(
        self,
        decode: impl FnOnce(&Envelope<P>) -> Result<R, Returned<N>>,
    ) -> Result<(Self, R), Rejected<P>> {
        let result = decode(&self.packet);
        let ((packet, flight), value) = Stopped::attach((self.packet, self.flight), result)?;
        Ok((Self { packet, flight }, value))
    }

    fn plain(self) -> Result<Self, Rejected<P>> {
        if !self.packet.0.is_plain() {
            return Err(self.reject(Violation::Result));
        }
        Ok(self)
    }

    fn creation(
        self,
        proof: kind::Witness<kind::Socket>,
    ) -> Result<(Self, domain::Creation), Rejected<P>> {
        let creation = self
            .flight
            .driver()
            .outbound()
            .creation(route::Erased::from(&self.flight).bind(proof));
        match creation {
            Some(creation) => Ok((self, creation)),
            None => Err(self.reject(Violation::Slot)),
        }
    }

    fn opened(self) -> Result<(Self, Opened<N>), Rejected<P>> {
        use file::raw::Completion as _;
        let this = self.plain()?;
        this.check(|packet| {
            // SAFETY: this original flight uniquely owns the packet's open result;
            // consuming the binding excludes another descriptor decode.
            unsafe { Opened::<N>::open(packet.0.parts().outcome) }.map_err(Returned::<N>::invalid)
        })
    }

    fn accepted(&self, value: Value<N>) -> Result<handles::Accepted, Violation> {
        let value: bounded::U32<0, { i32::MAX as u32 }> = value.into();
        let slot = SlotIndex::try_from(value.get()).map_err(Violation::SlotIndex)?;
        let end = self.flight.driver().descriptors().domain().accept_cap();
        handles::Accepted::try_from((slot, end)).map_err(Violation::Descriptor)
    }

    fn accepting(self) -> Result<(Self, Result<handles::Accepted, dope_sys::Errno>), Rejected<P>> {
        if self.packet.0.buffer().is_some() {
            return Err(self.reject(Violation::Buffer));
        }
        let outcome = match self.packet.0.parts().outcome.decode() {
            Decode::Value(value) => match self.accepted(value) {
                Ok(accepted) => Ok(accepted),
                Err(cause) => return Err(self.reject(cause)),
            },
            Decode::Error(errno) => Err(errno),
            Decode::Invalid(invalid) => return Err(self.invalid(Returned::<N>::invalid(invalid))),
        };
        Ok((self, outcome))
    }

    fn recv(mut self, proof: kind::Witness<kind::Recv>) -> Result<Event<'d>, Rejected<P>> {
        use dope_sys::memory::buffer::raw::Pending as _;

        let flow = self.packet.flow();
        let outcome = match self.packet.0.parts().outcome.decode() {
            Decode::Value(value) if !value.is_zero() => {
                let vacant = match tokens::Vacant::try_new(self.flight.driver()) {
                    Ok(vacant) => vacant,
                    Err(cause) => return Err(self.reject(Violation::Provided(cause))),
                };
                let Some(buffer) = self.packet.0.buffer_mut().take() else {
                    return Err(self.reject(Violation::Buffer));
                };
                let len = transfer::Len::from_bound(value.into());
                let lease = match buffer.try_complete(len) {
                    Ok(token) => recv::Lease::new(token.bind(vacant)),
                    Err((buffer, actual, capacity)) => {
                        drop(vacant);
                        *self.packet.0.buffer_mut() = Some(buffer);
                        return Err(self.reject(Violation::BufferLength { actual, capacity }));
                    }
                };
                return Ok(outputs::selected(
                    self.flight.settle(flow).bind(proof),
                    flow,
                    lease,
                ));
            }
            Decode::Value(_) if self.packet.0.buffer().is_none() => Ok(()),
            Decode::Value(value) => return Err(self.invalid(Returned::<N>::value(value))),
            Decode::Error(errno) => {
                if self.packet.0.buffer().is_some() {
                    return Err(self.reject(Violation::Buffer));
                }
                Err(errno)
            }
            Decode::Invalid(invalid) => return Err(self.invalid(Returned::<N>::invalid(invalid))),
        };
        let driver = self.flight.driver();
        Ok(Completion::control(self.flight.settle(flow).bind(proof), outcome, flow, driver).into())
    }

    fn drop_socket(
        self,
        proof: kind::Witness<kind::Socket>,
    ) -> Result<Retired<'d, N, P>, Rejected<P>> {
        let this = self.plain()?;
        let (this, outcome) = this.check(|packet| packet.0.parts().outcome.unit())?;
        let (this, creation) = this.creation(proof)?;
        let driver = this.flight.driver();
        let echo = fault::Echo::new(this.flight.bind(proof).end(Flow::Last));
        let slot = *creation.as_ref();
        let result = match outcome {
            Ok(()) => {
                match driver.outbound().complete(creation) {
                    domain::Create::Deliver => driver.descriptors().discard(slot),
                    domain::Create::Retired => {}
                }
                Ok(())
            }
            Err(errno) => {
                driver.outbound().fail_create(creation);
                Err(socket::Create::socket(errno))
            }
        };
        Ok(Reclaimed {
            buffer: this.packet.0.into_parts().1,
            report: Some(native::Report(io::Reclaimed::Socket {
                echo,
                slot: slot.physical(),
                result: result.map_err(Create::from_native),
                close: None,
            })),
            result: Ok(()),
        })
    }

    fn drop_open(self, proof: kind::Witness<kind::Open>) -> Result<Retired<'d, N, P>, Rejected<P>> {
        use file::raw::Completion as _;
        let (this, opened) = self.opened()?;
        let subject = fault::Subject::Target(route::Erased::from(&this.flight).into_token());
        let echo = fault::Echo::new(this.flight.bind(proof).end(Flow::Last));
        let (outcome, close) = match opened.deliver() {
            Ok(file) => (Ok(()), Some(Opened::<N>::close(file))),
            Err(errno) => (Err(errno), None),
        };
        let (buffer, result) = match close {
            Some(Ok(())) | None => (this.packet.0.into_parts().1, Ok(())),
            Some(Err(errno)) => (
                None,
                Err(this.packet.reject(subject, Violation::Cleanup(errno))),
            ),
        };
        Ok(Reclaimed {
            buffer,
            report: Some(native::Report(io::Reclaimed::Open {
                echo,
                result: outcome,
                close,
            })),
            result,
        })
    }

    fn drop_accept(
        self,
        proof: kind::Witness<kind::Accept>,
    ) -> Result<Retired<'d, N, P>, Rejected<P>> {
        let (this, outcome) = self.accepting()?;
        let flow = this.packet.flow();
        let driver = this.flight.driver();
        let echo = fault::Echo::new(this.flight.bind(proof).end(flow));
        let result = outcome
            .map(|accepted| driver.descriptors().abandon(accepted))
            .map_err(accept::Failure::from);
        Ok(Reclaimed {
            buffer: this.packet.0.into_parts().1,
            report: Some(native::Report(io::Reclaimed::Accept { echo, result, flow })),
            result: Ok(()),
        })
    }

    fn drop_recv(self, proof: kind::Witness<kind::Recv>) -> Result<Retired<'d, N, P>, Rejected<P>> {
        let empty = self.packet.0.buffer().is_none();
        let outcome = match self.packet.0.parts().outcome.decode() {
            Decode::Value(value) => {
                if value.is_zero() != empty {
                    return Err(self.reject(Violation::Buffer));
                }
                Ok(value)
            }
            Decode::Error(errno) => {
                if !empty {
                    return Err(self.reject(Violation::Buffer));
                }
                Err(errno)
            }
            Decode::Invalid(invalid) => return Err(self.invalid(Returned::<N>::invalid(invalid))),
        };
        let flow = self.packet.flow();
        let echo = fault::Echo::new(self.flight.bind(proof).end(flow));
        let result = outcome.map(|value| {
            let value: bounded::U32<0, { i32::MAX as u32 }> = value.into();
            value.get() as usize
        });
        Ok(Reclaimed {
            buffer: self.packet.0.into_parts().1,
            report: Some(native::Report(io::Reclaimed::Recv { echo, result, flow })),
            result: Ok(()),
        })
    }
}
