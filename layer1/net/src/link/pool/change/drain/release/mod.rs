use dope_core::driver::retained;

mod borrowed;

pub use borrowed::Borrowed;

/// Selects the exact authority required to return a released payload.
pub trait Policy<'d, X, B> {
    fn release(&mut self, context: &mut X, payload: B, driver: &mut retained::Context<'_, '_, 'd>);
}

impl<'d, X, B, F: FnMut(&mut X, B)> Policy<'d, X, B> for F {
    fn release(
        &mut self,
        context: &mut X,
        payload: B,
        _driver: &mut retained::Context<'_, '_, 'd>,
    ) {
        self(context, payload);
    }
}
