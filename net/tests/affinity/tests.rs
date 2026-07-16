use dope_test::{not_send, not_sync};

const _: fn() = || {
    not_send::<dope_net::multishot::Multishot, _>();
    not_sync::<dope_net::multishot::Multishot, _>();
    not_send::<dope_net::link::slot::PendingFlags, _>();
    not_sync::<dope_net::link::slot::PendingFlags, _>();
};
