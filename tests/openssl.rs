use std::{
    cell::Cell,
    env, fs,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use dope_test::harness::support::file;

pub mod common;
use common::suite::openssl::{COMMANDS, Identity, Server};

#[test]
#[ignore = "external: requires OpenSSL with QUIC support"]
fn verified_handshake() {
    let base = env::temp_dir();
    let cleanup = Cell::new(None);
    let root = file::Directory::try_with(&base, "quic-openssl", |result| cleanup.set(Some(result)))
        .expect("temporary path");
    for args in COMMANDS {
        let output = Command::new("openssl")
            .args(args)
            .current_dir(root.path())
            .output()
            .expect("OpenSSL certificate command");
        assert!(output.status.success(), "{args:?}: {output:?}");
    }
    let identity = Identity {
        key: fs::read(root.path().join("key.der")).expect("DER private key"),
        chain: [fs::read(root.path().join("cert.der")).expect("DER certificate")],
    };
    let mut server = Server::new(&identity).expect("QUIC server");
    let mut peer = Command::new("openssl")
        .args([
            "s_client",
            "-quic",
            "-connect",
            &server.addr().expect("bound address").to_string(),
            "-alpn",
            "h3",
            "-CAfile",
            "cert.pem",
            "-verify_ip",
            "127.0.0.1",
            "-verify_return_error",
            "-brief",
            "-ign_eof",
        ])
        .current_dir(root.path())
        .stdin(Stdio::null())
        .stdout(fs::File::create(root.path().join("stdout")).expect("peer stdout"))
        .stderr(fs::File::create(root.path().join("stderr")).expect("peer stderr"))
        .spawn()
        .expect("OpenSSL peer");
    let deadline = Instant::now() + Duration::from_secs(10);
    let handle = loop {
        server.poll().expect("QUIC progress");
        let diagnostic = fs::read_to_string(root.path().join("stderr")).expect("peer diagnostic");
        if let Some(handle) = server.router.handler().established.first().copied()
            && diagnostic.contains("Verification: OK")
        {
            break handle;
        }
        assert!(
            Instant::now() < deadline,
            "OpenSSL handshake timeout: received={}, sent={}, peer={}",
            server.received,
            server.sent,
            diagnostic,
        );
        assert!(
            peer.try_wait().expect("peer status").is_none(),
            "OpenSSL exited before verification: {}",
            diagnostic
        );
    };
    let conn = server.router.conn(handle).expect("established connection");
    assert!(conn.view().path().mtu() >= 1200);
    assert!(server.received > 0 && server.sent > 0);
    assert_eq!(server.router.handler().established.len(), 1);
    assert_eq!(server.router.handler().certificates, 0);
    if peer.try_wait().expect("peer status").is_none() {
        peer.kill().expect("stop OpenSSL");
    }
    peer.wait().expect("reap OpenSSL");
    drop(root);
    cleanup
        .take()
        .expect("directory teardown observed")
        .expect("temporary directory removed");
}
