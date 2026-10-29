use core::convert::Infallible;

use o3::base::Call;

/// Send notifications retain their concrete callback and exact input contract.
pub enum Sent<F = Infallible, C = Infallible, P = Infallible, Q = Infallible> {
    None,
    Ready(F),
    Connection(C),
    Pending(P),
    Queue(Q),
}

impl<O, I, P, T, F: Call<O>, C: Call<(O, I, T)>, B: Call<(O, I, P, T)>, Q: Call<(O, I, P)>>
    Call<(O, I, P, T)> for Sent<F, C, B, Q>
{
    fn call(self, (owner, conn, pending, token): (O, I, P, T)) {
        match self {
            Self::None => {}
            Self::Ready(call) => call.call(owner),
            Self::Connection(call) => call.call((owner, conn, token)),
            Self::Pending(call) => call.call((owner, conn, pending, token)),
            Self::Queue(call) => call.call((owner, conn, pending)),
        }
    }
}
