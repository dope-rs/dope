use crate::Budget;

/// A scheduler reservation retaining its exact turn, driver and lane brands.
pub trait Quota {
    type Budget<'a, Lane>: Budget
    where
        Self: 'a;

    fn remaining(&self) -> usize;
    fn share<const N: usize, const I: usize>(&self, cursor: usize) -> usize;
    fn budget<Lane>(&self, limit: usize) -> Self::Budget<'_, Lane>;

    /// Returns the reservation before work resumes outside this reactor scope.
    fn refund(self);
}
