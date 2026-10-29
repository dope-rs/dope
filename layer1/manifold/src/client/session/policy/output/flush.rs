use core::convert::Infallible;

use o3::base::Call;

/// A flush callback retaining its concrete closure and exact input contract.
pub enum Flush<F = Infallible, C = Infallible> {
    None,
    Ready(F),
    Context(C),
}

impl<O, X, F: Call<O>, C: Call<(O, X)>> Call<(O, X)> for Flush<F, C> {
    fn call(self, (owner, context): (O, X)) {
        match self {
            Self::None => {}
            Self::Ready(call) => call.call(owner),
            Self::Context(call) => call.call((owner, context)),
        }
    }
}
