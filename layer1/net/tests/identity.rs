use dope_core::driver::route::KeyTag;
use dope_net::wire::{Identity, open};

#[test]
fn identity_exact_parts() {
    assert_eq!(std::mem::size_of::<Identity>(), 0);
    let mut rt = Identity;
    let opened = <Identity as open::Runtime<'_, '_, Identity, KeyTag<0>>>::open(
        &mut rt,
        (),
        |(), wire, send| open::Disposition::Commit((wire, send)),
    );
    let (Identity, ()) = {
        match opened {
            open::Outcome::Opened(parts) => Some(parts),
            open::Outcome::Unavailable(()) => None,
            open::Outcome::Failed { context: (), cause } => cause.eliminate(),
        }
        .expect("the identity wire must open immediately")
    };
}
