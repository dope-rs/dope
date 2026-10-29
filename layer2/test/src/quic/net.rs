use std::num::NonZero;

/// Terminal result of one transport-owned datagram allocation.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Sent(usize),
    Failed,
    Shutdown,
}

/// O(1) transport accounting: accepted = outstanding + settled.
#[derive(Default)]
pub struct Oracle {
    accepted: Accepted,
    outcomes: Outcomes,
    outstanding: Outstanding,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Accepted {
    operations: usize,
    datagrams: usize,
    bytes: usize,
    segmented_operations: usize,
    maximum_datagrams_per_operation: usize,
}

#[derive(Default)]
struct Outstanding {
    operations: usize,
    bytes: usize,
    peak_bytes: usize,
}

impl Oracle {
    /// Admits totals before updating their bounded outstanding and GSO subsets.
    pub fn accept(
        &mut self,
        count: NonZero<usize>,
        bytes: usize,
        is_gso: bool,
    ) -> Result<(), Violation> {
        if bytes == 0 {
            return Err(Violation::EmptyAcceptance {
                datagrams: count,
                bytes,
            });
        }
        if is_gso != (count.get() > 1) {
            return Err(Violation::InvalidSegmentation {
                datagrams: count,
                is_gso,
            });
        }
        let ops = self.accepted.operations.checked_add(1).ok_or(
            Violation::AcceptedOperationsOverflow {
                current: self.accepted.operations,
                added: 1,
            },
        )?;
        let dgrams = self.accepted.datagrams.checked_add(count.get()).ok_or(
            Violation::AcceptedDatagramsOverflow {
                current: self.accepted.datagrams,
                added: count,
            },
        )?;
        let total =
            self.accepted
                .bytes
                .checked_add(bytes)
                .ok_or(Violation::AcceptedBytesOverflow {
                    current: self.accepted.bytes,
                    added: bytes,
                })?;
        let open_ops = self.outstanding.operations + 1;
        let open_len = self.outstanding.bytes + bytes;
        let gso_ops = self.accepted.segmented_operations + usize::from(is_gso);

        self.accepted.operations = ops;
        self.accepted.datagrams = dgrams;
        self.accepted.bytes = total;
        self.accepted.segmented_operations = gso_ops;
        self.accepted.maximum_datagrams_per_operation = self
            .accepted
            .maximum_datagrams_per_operation
            .max(count.get());
        self.outstanding.operations = open_ops;
        self.outstanding.bytes = open_len;
        self.outstanding.peak_bytes = self.outstanding.peak_bytes.max(open_len);
        Ok(())
    }

    /// Consumes one outstanding operation, bounding all outcome counts.
    pub fn settle(&mut self, owned: usize, outcome: Outcome) -> Result<(), Violation> {
        if owned == 0 {
            return Err(Violation::EmptySettlement { bytes: owned });
        }
        if let Outcome::Sent(sent) = outcome
            && sent != owned
        {
            return Err(Violation::PartialDatagram {
                owner_bytes: owned,
                sent_bytes: sent,
            });
        }
        let open_ops = self.outstanding.operations.checked_sub(1).ok_or(
            Violation::OutstandingOperationsUnderflow {
                current: self.outstanding.operations,
                removed: 1,
            },
        )?;
        let open_len = self.outstanding.bytes.checked_sub(owned).ok_or(
            Violation::OutstandingBytesUnderflow {
                current: self.outstanding.bytes,
                removed: owned,
            },
        )?;
        match outcome {
            Outcome::Sent(_) => {}
            Outcome::Failed => self.outcomes.failed_operations += 1,
            Outcome::Shutdown => self.outcomes.shutdown_operations += 1,
        }
        self.outstanding.operations = open_ops;
        self.outstanding.bytes = open_len;
        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        self.outstanding.operations == 0 && self.outstanding.bytes == 0
    }

    /// Proves that settled operation and byte totals equal accepted totals.
    /// The transport retains each allocation's unique settlement authority.
    pub fn finish(&self) -> Result<Completion, Violation> {
        if !self.is_complete() {
            return Err(Violation::Incomplete {
                outstanding_operations: self.outstanding.operations,
                outstanding_bytes: self.outstanding.bytes,
            });
        }
        Ok(Completion {
            accepted: self.accepted,
            outcomes: self.outcomes,
            peak_bytes: self.outstanding.peak_bytes,
        })
    }
}

/// Failure of the socket-owner conservation contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Violation {
    EmptyAcceptance {
        datagrams: NonZero<usize>,
        bytes: usize,
    },
    EmptySettlement {
        bytes: usize,
    },
    InvalidSegmentation {
        datagrams: NonZero<usize>,
        is_gso: bool,
    },
    OutstandingOperationsUnderflow {
        current: usize,
        removed: usize,
    },
    OutstandingBytesUnderflow {
        current: usize,
        removed: usize,
    },
    PartialDatagram {
        owner_bytes: usize,
        sent_bytes: usize,
    },
    AcceptedOperationsOverflow {
        current: usize,
        added: usize,
    },
    AcceptedDatagramsOverflow {
        current: usize,
        added: NonZero<usize>,
    },
    AcceptedBytesOverflow {
        current: usize,
        added: usize,
    },
    Incomplete {
        outstanding_operations: usize,
        outstanding_bytes: usize,
    },
}

const _: () = assert!(std::mem::size_of::<Violation>() == 3 * std::mem::size_of::<usize>());

/// Complete, terminally settled transport accounting for one E2E run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Completion {
    accepted: Accepted,
    outcomes: Outcomes,
    peak_bytes: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Outcomes {
    failed_operations: usize,
    shutdown_operations: usize,
}

impl Completion {
    pub const fn operations(self) -> usize {
        self.accepted.operations
    }

    pub const fn datagrams(self) -> usize {
        self.accepted.datagrams
    }

    pub const fn bytes(self) -> usize {
        self.accepted.bytes
    }

    pub const fn gso_ops(self) -> usize {
        self.accepted.segmented_operations
    }

    pub const fn max_batch(self) -> usize {
        self.accepted.maximum_datagrams_per_operation
    }

    pub const fn failed_ops(self) -> usize {
        self.outcomes.failed_operations
    }

    pub const fn shutdowns(self) -> usize {
        self.outcomes.shutdown_operations
    }

    pub const fn peak_bytes(self) -> usize {
        self.peak_bytes
    }

    /// Saved operations; every accepted operation carries at least one datagram.
    pub const fn saved_ops(self) -> usize {
        self.accepted.datagrams - self.accepted.operations
    }
}
