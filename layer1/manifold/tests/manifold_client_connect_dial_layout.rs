use std::mem;

use dope_manifold::client::attempt::{self, queue};
use dope_net::tcp::Tcp;
use queue::Source;

#[test]
fn control_layout() {
    assert_eq!(
        mem::size_of::<attempt::Action<'static, Tcp>>(),
        mem::size_of::<(attempt::Id<'static>, attempt::Plan<Tcp>)>()
    );
    assert_eq!(
        mem::size_of::<queue::Control<'static, 'static, Tcp>>(),
        mem::size_of::<&'static Source<'static, Tcp>>()
    );
    assert_eq!(
        mem::size_of::<queue::Lease<'static, 'static, Tcp>>(),
        2 * mem::size_of::<usize>()
    );
    assert_eq!(
        mem::size_of::<queue::Reserve<'static, 'static, Tcp>>(),
        3 * mem::size_of::<usize>()
    );
    assert_eq!(
        mem::size_of::<Option<queue::Reserve<'static, 'static, Tcp>>>(),
        mem::size_of::<queue::Reserve<'static, 'static, Tcp>>()
    );
    assert_eq!(
        mem::size_of::<Option<queue::Lease<'static, 'static, Tcp>>>(),
        mem::size_of::<queue::Lease<'static, 'static, Tcp>>()
    );
}
