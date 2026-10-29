use std::convert::Infallible;

/// Retained storage cleanup preserves its primary error across every retry.
pub trait Cleanup<P>: Sized {
    type Error;

    fn primary(&self) -> P;
    fn failure(&self) -> Self::Error;
    fn try_close(self, report: impl FnMut(Self::Error)) -> Result<P, Self>;
}

impl<P> Cleanup<P> for Infallible {
    type Error = P;

    fn primary(&self) -> P {
        match *self {}
    }

    fn failure(&self) -> P {
        match *self {}
    }

    fn try_close(self, _: impl FnMut(Self::Error)) -> Result<P, Self> {
        match self {}
    }
}
