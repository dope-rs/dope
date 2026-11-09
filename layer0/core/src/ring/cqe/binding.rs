use dope_sys::{
    Errno,
    io::{
        fd::Fd,
        file,
        reactor::complete::{Decode, Outcome as _, Packet as _, resolve::Reclaimed},
        socket,
    },
    kernel::Returned as _,
    probe::Stopped,
    table::token::host::Flight as _,
};

use crate::{
    Backend,
    driver::{
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
            net::{accept, connect, recv::Completion},
        },
        fd::{domain, handles},
        outputs, recv,
        socket::Create,
        transfer,
    },
    ring::cqe::{Envelope, Rejected, Returned, Selected, Value},
};

/// One native packet and its original validated flight, consumed together.
pub(crate) struct Binding<'q, 'd> {
    packet: Envelope<'d>,
    flight: flight::Complete<'q, 'd>,
}

pub(super) type Retired<'d> = Reclaimed<tokens::Token<'d, Selected>, io::Reclaimed, Rejected<'d>>;

impl<'q, 'd> Binding<'q, 'd> {
    pub(super) const fn new(packet: Envelope<'d>, flight: flight::Complete<'q, 'd>) -> Self {
        Self { packet, flight }
    }

    pub(super) fn resolve(self) -> Result<Event<'d>, Rejected<'d>> {
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
                        driver.descriptors().fail_create(creation);
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
                Ok(match opened {
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
            Shape::Read(proof) => {
                let (this, value) = self.plain()?.check(Envelope::bytes)?;
                Ok(outputs::read(this.flight.complete().bind(proof), value))
            }
            Shape::Connect(proof) => {
                let (this, value) = self.plain()?.check(Envelope::unit)?;
                Ok(outputs::connected(
                    this.flight.complete().bind(proof),
                    value.map_err(connect::Error::Connect),
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

    pub(super) fn reclaim(self) -> Result<Retired<'d>, Rejected<'d>> {
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
            Shape::Read(proof) => {
                let (this, result) = self.plain()?.check(Envelope::bytes)?;
                let report = io::Reclaimed::Read {
                    echo: fault::Echo::new(this.flight.bind(proof).end(Flow::Last)),
                    result,
                };
                (this.packet, report)
            }
            Shape::Connect(proof) => {
                let (this, result) = self.plain()?.check(Envelope::unit)?;
                let report = io::Reclaimed::Connect {
                    echo: fault::Echo::new(this.flight.bind(proof).end(Flow::Last)),
                    result: result.map_err(connect::Error::Connect),
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
            report: Some(report),
            result: Ok(()),
        })
    }

    fn reject(self, cause: Violation) -> Rejected<'d> {
        let flow = self.packet.flow();
        let target = self.flight.settle(flow).into_token();
        self.packet.reject(fault::Subject::Target(target), cause)
    }

    fn invalid(self, cause: Returned) -> Rejected<'d> {
        Stopped {
            state: (self.packet, self.flight),
            cause,
        }
        .into()
    }

    fn check<R>(
        self,
        decode: impl FnOnce(&Envelope<'d>) -> Result<R, Returned>,
    ) -> Result<(Self, R), Rejected<'d>> {
        let result = decode(&self.packet);
        let ((packet, flight), value) = Stopped::attach((self.packet, self.flight), result)?;
        Ok((Self { packet, flight }, value))
    }

    fn plain(self) -> Result<Self, Rejected<'d>> {
        if !self.packet.0.is_plain() {
            return Err(self.reject(Violation::Result));
        }
        Ok(self)
    }

    fn creation(
        self,
        proof: kind::Witness<kind::Socket>,
    ) -> Result<(Self, domain::Creation), Rejected<'d>> {
        let creation = self
            .flight
            .driver()
            .descriptors()
            .creation(route::Erased::from(&self.flight).bind(proof));
        match creation {
            Some(creation) => Ok((self, creation)),
            None => Err(self.reject(Violation::Slot)),
        }
    }

    fn opened(self) -> Result<(Self, file::Opened), Rejected<'d>> {
        let this = self.plain()?;
        this.check(|packet| {
            // SAFETY: this original flight uniquely owns the packet's open result;
            // consuming the binding excludes another descriptor decode.
            unsafe { <Backend as file::raw::Completion>::open(packet.0.parts().outcome) }
                .map_err(Returned::invalid)
        })
    }

    fn accepted(&self, value: Value) -> Result<handles::Accepted, Violation> {
        let slot = SlotIndex::try_from(value.get()).map_err(Violation::SlotIndex)?;
        let end = self.flight.driver().descriptors().domain().accept_cap();
        handles::Accepted::try_from((slot, end)).map_err(Violation::Descriptor)
    }

    fn accepting(self) -> Result<(Self, Result<handles::Accepted, dope_sys::Errno>), Rejected<'d>> {
        if self.packet.0.buffer().is_some() {
            return Err(self.reject(Violation::Buffer));
        }
        let outcome = match self.packet.0.parts().outcome.decode() {
            Decode::Value(value) => match self.accepted(value) {
                Ok(accepted) => Ok(accepted),
                Err(cause) => return Err(self.reject(cause)),
            },
            Decode::Error(errno) => Err(errno),
            Decode::Invalid(invalid) => return Err(self.invalid(Returned::invalid(invalid))),
        };
        Ok((self, outcome))
    }

    fn recv(mut self, proof: kind::Witness<kind::Recv>) -> Result<Event<'d>, Rejected<'d>> {
        use dope_sys::memory::buffer::raw::Pending as _;

        let flow = self.packet.flow();
        let outcome = match self.packet.0.parts().outcome.decode() {
            Decode::Value(value) if value.get() != 0 => {
                let vacant = match self.flight.driver().owners().try_vacant() {
                    Ok(vacant) => vacant,
                    Err(cause) => return Err(self.reject(Violation::Provided(cause))),
                };
                let Some(buffer) = self.packet.0.buffer_mut().take() else {
                    return Err(self.reject(Violation::Buffer));
                };
                let len = transfer::Len::from_bound(value);
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
            Decode::Value(value) => return Err(self.invalid(Returned::value(value))),
            Decode::Error(errno) => {
                if self.packet.0.buffer().is_some() {
                    return Err(self.reject(Violation::Buffer));
                }
                Err(errno)
            }
            Decode::Invalid(invalid) => return Err(self.invalid(Returned::invalid(invalid))),
        };
        let driver = self.flight.driver();
        Ok(Completion::control(self.flight.settle(flow).bind(proof), outcome, flow, driver).into())
    }

    fn drop_socket(self, proof: kind::Witness<kind::Socket>) -> Result<Retired<'d>, Rejected<'d>> {
        let this = self.plain()?;
        let (this, outcome) = this.check(|packet| packet.0.parts().outcome.unit())?;
        let (this, creation) = this.creation(proof)?;
        let driver = this.flight.driver();
        let echo = fault::Echo::new(this.flight.bind(proof).end(Flow::Last));
        let slot = *creation.as_ref();
        let result = match outcome {
            Ok(()) => {
                match driver.descriptors().complete(creation) {
                    domain::Create::Deliver => driver.descriptors().discard(slot),
                    domain::Create::Retired => {}
                }
                Ok(())
            }
            Err(errno) => {
                driver.descriptors().fail_create(creation);
                Err(socket::Create::socket(errno))
            }
        };
        Ok(Reclaimed {
            buffer: this.packet.0.into_parts().1,
            report: Some(io::Reclaimed::Socket {
                echo,
                slot: slot.physical(),
                result: result.map_err(Create::from_native),
                close: None,
            }),
            result: Ok(()),
        })
    }

    fn drop_open(self, proof: kind::Witness<kind::Open>) -> Result<Retired<'d>, Rejected<'d>> {
        let (this, opened) = self.opened()?;
        let subject = fault::Subject::Target(route::Erased::from(&this.flight).into_token());
        let echo = fault::Echo::new(this.flight.bind(proof).end(Flow::Last));
        let (outcome, close) = match opened {
            Ok(file) => (Ok(()), Some(<Backend as Fd>::close::<Errno>(file))),
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
            report: Some(io::Reclaimed::Open {
                echo,
                result: outcome,
                close,
            }),
            result,
        })
    }

    fn drop_accept(self, proof: kind::Witness<kind::Accept>) -> Result<Retired<'d>, Rejected<'d>> {
        let (this, outcome) = self.accepting()?;
        let flow = this.packet.flow();
        let driver = this.flight.driver();
        let echo = fault::Echo::new(this.flight.bind(proof).end(flow));
        let result = outcome
            .map(|accepted| driver.descriptors().abandon(accepted))
            .map_err(accept::Failure::from);
        Ok(Reclaimed {
            buffer: this.packet.0.into_parts().1,
            report: Some(io::Reclaimed::Accept { echo, result, flow }),
            result: Ok(()),
        })
    }

    fn drop_recv(self, proof: kind::Witness<kind::Recv>) -> Result<Retired<'d>, Rejected<'d>> {
        let empty = self.packet.0.buffer().is_none();
        let outcome = match self.packet.0.parts().outcome.decode() {
            Decode::Value(value) => {
                if (value.get() == 0) != empty {
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
            Decode::Invalid(invalid) => return Err(self.invalid(Returned::invalid(invalid))),
        };
        let flow = self.packet.flow();
        let echo = fault::Echo::new(self.flight.bind(proof).end(flow));
        let result = outcome.map(|value| value.get() as usize);
        Ok(Reclaimed {
            buffer: self.packet.0.into_parts().1,
            report: Some(io::Reclaimed::Recv { echo, result, flow }),
            result: Ok(()),
        })
    }
}
