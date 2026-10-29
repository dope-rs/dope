use dope_core::driver::route::table::conn;
use dope_net::wire::domain;
use o3::buffer::hold::limits;

/// Declarative receive-retention bound for one connection pool.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    each: usize,
    min: usize,
}

/// Resident-byte authority for ranges retained from a callback-scoped cursor.
/// Unlike [`Retention`], this can never reserve a connection receive credit.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    each: limits::Bound,
    min: usize,
}

impl Retention {
    pub const NONE: Self = Self::new(0, 0);

    pub const fn new(each: usize, min: usize) -> Self {
        Self { each, min }
    }

    pub fn capacity(self, conns: conn::Capacity) -> Result<domain::Retained, crate::ConfigError> {
        let count = conns.entries().get();
        count
            .checked_mul(self.each)
            .map(|cap| cap.max(self.min))
            .map(domain::Retained::from)
            .ok_or(crate::ConfigError::RetainedReceiveCapacity {
                connections: count,
                each: self.each,
                minimum: self.min,
            })
    }
}

impl Budget {
    pub const NONE: Self = Self::new(limits::Bound::new(0), 0);

    pub const fn new(each: limits::Bound, min: usize) -> Self {
        Self { each, min }
    }

    pub fn capacity(self, conns: conn::Capacity) -> Result<domain::Resident, crate::ConfigError> {
        let count = conns.entries().get();
        let cap = self
            .each
            .try_mul(count)
            .map_err(crate::ConfigError::ScopedReceiveCapacity)?;
        Ok(domain::Resident::from_limit(cap.max(self.min)))
    }
}
