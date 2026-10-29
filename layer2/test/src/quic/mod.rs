//! QUIC end-to-end contracts independent of the protocol implementation.
//! Environments adapt them to memory, kernel, or external transports.

#![forbid(unsafe_code)]

pub mod net;
pub mod proof;
pub mod wire;

use std::num::NonZero;

use o3::alloc::{self, BoxSliceExt as _};
use proof::{Completion, Incomplete};

/// Bytes occupied by the flow and sequence identity in every payload.
pub const HEADER_BYTES: usize = 16;

/// An independently executed transport lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    /// One UDP datagram per output buffer.
    Plain,
    /// UDP segmentation enabled when the environment supports it.
    Gso,
}

impl Lane {
    /// Whether this lane requests UDP segmentation.
    pub const fn is_gso(self) -> bool {
        matches!(self, Self::Gso)
    }
}

/// Deterministic datagrams keyed by `(flow, sequence)` for exact observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Datagram {
    flows: NonZero<usize>,
    per_flow: NonZero<usize>,
    payload_bytes: NonZero<usize>,
    total: NonZero<usize>,
    payload_total: NonZero<usize>,
}

impl Datagram {
    /// Validates dimensions supplied by a runtime configuration boundary.
    pub const fn try_new(flows: usize, per_flow: usize, bytes: usize) -> Result<Self, ShapeError> {
        if flows == 0 || flows > u64::MAX as usize {
            return Err(ShapeError::Flows { actual: flows });
        }
        let flows = NonZero::<usize>::MIN.saturating_add(flows - 1);
        if per_flow == 0 || per_flow > u64::MAX as usize {
            return Err(ShapeError::PerFlow { actual: per_flow });
        }
        let per_flow = NonZero::<usize>::MIN.saturating_add(per_flow - 1);
        if bytes < HEADER_BYTES {
            return Err(ShapeError::Payload {
                actual: bytes,
                required: HEADER_BYTES,
            });
        }
        let bytes = NonZero::<usize>::MIN.saturating_add(bytes - 1);
        let Some(total) = flows.checked_mul(per_flow) else {
            return Err(ShapeError::Count {
                flows: flows.get(),
                per_flow: per_flow.get(),
            });
        };
        let Some(volume) = total.checked_mul(bytes) else {
            return Err(ShapeError::Bytes {
                count: total.get(),
                payload: bytes.get(),
            });
        };
        Ok(Self {
            flows,
            per_flow,
            payload_bytes: bytes,
            total,
            payload_total: volume,
        })
    }

    /// Number of independent flows.
    pub const fn flows(self) -> usize {
        self.flows.get()
    }

    /// Number of payloads generated for each flow.
    pub const fn per_flow(self) -> usize {
        self.per_flow.get()
    }

    /// Exact encoded payload size.
    pub const fn bytes(self) -> usize {
        self.payload_bytes.get()
    }

    /// Exact number of distinct payloads required for completion.
    pub const fn total(self) -> usize {
        self.total.get()
    }

    /// Allocates one payload while preserving invalid requested identities.
    pub fn try_payload(self, flow: usize, sequence: usize) -> Result<Vec<u8>, EncodeError> {
        self.validate(flow, sequence)?;
        let mut payload = Box::<[u8]>::try_repeat(self.bytes(), 0)
            .map_err(EncodeError::Allocation)?
            .into_vec();
        Self::write(flow, sequence, &mut payload);
        Ok(payload)
    }

    /// Encodes one payload into caller-owned storage without allocation.
    pub fn encode(
        self,
        flow: usize,
        sequence: usize,
        payload: &mut [u8],
    ) -> Result<(), EncodeError> {
        if payload.len() != self.bytes() {
            return Err(EncodeError::Length {
                actual: payload.len(),
                expected: self.bytes(),
            });
        }
        self.validate(flow, sequence)?;
        Self::write(flow, sequence, payload);
        Ok(())
    }

    fn validate(self, flow: usize, sequence: usize) -> Result<(), EncodeError> {
        if flow >= self.flows() {
            return Err(EncodeError::Flow {
                actual: flow,
                count: self.flows(),
            });
        }
        if sequence >= self.per_flow() {
            return Err(EncodeError::Sequence {
                actual: sequence,
                count: self.per_flow(),
            });
        }
        Ok(())
    }

    fn write(flow: usize, sequence: usize, payload: &mut [u8]) {
        payload[..8].copy_from_slice(&(flow as u64).to_be_bytes());
        payload[8..HEADER_BYTES].copy_from_slice(&(sequence as u64).to_be_bytes());
        for (offset, byte) in payload[HEADER_BYTES..].iter_mut().enumerate() {
            *byte = payload_byte(flow, sequence, offset);
        }
    }

    /// Decodes and validates one payload without allocating flow state.
    pub fn decode(self, payload: &[u8]) -> Result<Observation, Violation> {
        if payload.len() != self.bytes() {
            return Err(Violation::Length {
                actual: payload.len(),
                expected: self.bytes(),
            });
        }
        let mut encoded = [0; 8];
        encoded.copy_from_slice(&payload[..8]);
        let raw_flow = u64::from_be_bytes(encoded);
        if raw_flow > usize::MAX as u64 {
            return Err(Violation::Flow {
                actual: Index::Encoded(raw_flow),
                count: self.flows(),
            });
        }
        encoded.copy_from_slice(&payload[8..HEADER_BYTES]);
        let raw_seq = u64::from_be_bytes(encoded);
        if raw_seq > usize::MAX as u64 {
            return Err(Violation::Sequence {
                actual: Index::Encoded(raw_seq),
                count: self.per_flow(),
            });
        }
        let flow = raw_flow as usize;
        let sequence = raw_seq as usize;
        if flow >= self.flows() {
            return Err(Violation::Flow {
                actual: Index::Encoded(raw_flow),
                count: self.flows(),
            });
        }
        if sequence >= self.per_flow() {
            return Err(Violation::Sequence {
                actual: Index::Encoded(raw_seq),
                count: self.per_flow(),
            });
        }
        for (offset, actual) in payload[HEADER_BYTES..].iter().copied().enumerate() {
            let expected = payload_byte(flow, sequence, offset);
            if actual != expected {
                return Err(Violation::Payload {
                    offset,
                    expected,
                    actual,
                });
            }
        }
        Ok(Observation { flow, sequence })
    }
}

/// Invalid runtime dimensions for a deterministic datagram scenario.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShapeError {
    Flows { actual: usize },
    PerFlow { actual: usize },
    Payload { actual: usize, required: usize },
    Count { flows: usize, per_flow: usize },
    Bytes { count: usize, payload: usize },
}

/// Failure to encode a requested scenario payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodeError {
    /// Payload storage could not be represented or allocated.
    Allocation(alloc::Error),
    /// Destination storage does not have the scenario's exact size.
    Length { actual: usize, expected: usize },
    /// Flow is outside the declared scenario.
    Flow { actual: usize, count: usize },
    /// Sequence is outside the declared flow.
    Sequence { actual: usize, count: usize },
}

/// Flow and sequence identity within a scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Observation {
    /// Flow ordinal.
    pub flow: usize,
    /// Sequence within that flow.
    pub sequence: usize,
}

/// A violation of the deterministic E2E payload contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Index {
    Native(usize),
    Encoded(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Violation {
    /// Payload length is not exact.
    Length { actual: usize, expected: usize },
    /// Encoded flow is outside the scenario.
    Flow { actual: Index, count: usize },
    /// Encoded sequence is outside the scenario.
    Sequence { actual: Index, count: usize },
    /// Payload contents were corrupted.
    Payload {
        offset: usize,
        expected: u8,
        actual: u8,
    },
    /// This payload identity was already delivered.
    Duplicate { flow: usize, sequence: usize },
}

fn record(seen: &mut [u64], observed: Observation, index: usize) -> Result<(), Violation> {
    let word = &mut seen[index / u64::BITS as usize];
    let mask = 1_u64 << (index % u64::BITS as usize);
    if *word & mask != 0 {
        return Err(Violation::Duplicate {
            flow: observed.flow,
            sequence: observed.sequence,
        });
    }
    *word |= mask;
    Ok(())
}

/// Exactly-once delivery oracle for one receiving endpoint.
pub struct Oracle {
    scenario: Datagram,
    seen: Box<[u64]>,
    delivered: usize,
}

impl Oracle {
    /// Creates an empty oracle for `scenario`.
    pub fn new(scenario: Datagram) -> Self {
        Self {
            scenario,
            seen: vec![0; scenario.total().div_ceil(u64::BITS as usize)].into_boxed_slice(),
            delivered: 0,
        }
    }

    /// Validates and records one payload atomically.
    pub fn observe(&mut self, payload: &[u8]) -> Result<Observation, Violation> {
        let seen = self.scenario.decode(payload)?;
        let index = seen.flow * self.scenario.per_flow() + seen.sequence;
        record(&mut self.seen, seen, index)?;
        self.delivered += 1;
        Ok(seen)
    }

    /// Number of distinct valid payloads observed so far.
    pub const fn delivered(&self) -> usize {
        self.delivered
    }

    /// Whether every required payload has been observed exactly once.
    pub fn is_complete(&self) -> bool {
        self.delivered == self.scenario.total()
    }

    /// Consumes the oracle and returns the proof of complete delivery.
    pub fn finish(self) -> Result<Completion, Incomplete> {
        if self.is_complete() {
            Ok(Completion::new(
                self.delivered,
                self.scenario.payload_total.get(),
            ))
        } else {
            Err(Incomplete {
                expected: self.scenario.total(),
                delivered: self.delivered,
            })
        }
    }
}

/// Connection-owned exactly-once oracle for one flow, without synchronization.
pub struct FlowOracle {
    scenario: Datagram,
    flow: usize,
    seen: Box<[u64]>,
    delivered: usize,
}

impl FlowOracle {
    pub fn try_new(scenario: Datagram, flow: usize) -> Result<Self, Violation> {
        if flow >= scenario.flows() {
            return Err(Violation::Flow {
                actual: Index::Native(flow),
                count: scenario.flows(),
            });
        }
        Ok(Self::from_valid(scenario, flow))
    }

    fn from_valid(scenario: Datagram, flow: usize) -> Self {
        Self {
            scenario,
            flow,
            seen: vec![0; scenario.per_flow().div_ceil(u64::BITS as usize)].into_boxed_slice(),
            delivered: 0,
        }
    }

    pub fn observe(&mut self, payload: &[u8]) -> Result<Observation, Violation> {
        let seen = self.scenario.decode(payload)?;
        self.record(seen)
    }

    /// Binds a previously unassigned connection oracle to the flow encoded by
    /// its first payload and records that payload in one validation pass.
    pub fn bind(scenario: Datagram, payload: &[u8]) -> Result<(Self, Observation), Violation> {
        let seen = scenario.decode(payload)?;
        let mut oracle = Self::from_valid(scenario, seen.flow);
        let seen = oracle.record(seen)?;
        Ok((oracle, seen))
    }

    fn record(&mut self, seen: Observation) -> Result<Observation, Violation> {
        if seen.flow != self.flow {
            return Err(Violation::Flow {
                actual: Index::Native(seen.flow),
                count: self.scenario.flows(),
            });
        }
        record(&mut self.seen, seen, seen.sequence)?;
        self.delivered += 1;
        Ok(seen)
    }

    pub const fn delivered(&self) -> usize {
        self.delivered
    }

    /// Returns the first missing identity without changing delivery state.
    pub fn missing(&self) -> Option<Observation> {
        (0..self.scenario.per_flow())
            .find(|&sequence| {
                self.seen[sequence / u64::BITS as usize]
                    & (1_u64 << (sequence % u64::BITS as usize))
                    == 0
            })
            .map(|sequence| Observation {
                flow: self.flow,
                sequence,
            })
    }

    pub fn finish(self) -> Result<Completion, Incomplete> {
        if self.delivered == self.scenario.per_flow() {
            Ok(Completion::new(
                self.delivered,
                self.scenario.payload_total.get() / self.scenario.flows.get(),
            ))
        } else {
            Err(Incomplete {
                expected: self.scenario.per_flow(),
                delivered: self.delivered,
            })
        }
    }
}

fn payload_byte(flow: usize, sequence: usize, offset: usize) -> u8 {
    let flow = u16::from(flow.to_le_bytes()[0]) * 31;
    let sequence = u16::from(sequence.to_le_bytes()[0].rotate_left(3));
    let offset = u16::from(offset.to_le_bytes()[0]) * 17;
    (flow + sequence + offset).to_le_bytes()[0]
}
