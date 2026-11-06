mod cancel;
mod sent;

pub use cancel::Cancel;
pub use sent::Sent;

pub type Args<'a, 'c, 't, 'e, 'd, N, ID> = (
    &'a mut N,
    &'c mut super::session::turns::Context<
        't,
        'e,
        'd,
        <N as super::session::Session<'d, ID>>::ConnState,
        <N as super::session::Session<'d, ID>>::Send,
        ID,
    >,
    super::session::Input,
);

pub type BoundArgs<'a, 'c, 't, 'e, 'life, 'd, P, L, ID, const MAX: usize> = (
    &'a mut super::State<'life, 'd, P, L, ID, MAX>,
    &'c mut super::session::Context<'t, 'e, 'life, 'd, P, L, ID, MAX>,
    super::session::Input,
);
