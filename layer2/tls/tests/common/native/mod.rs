pub mod reentry;

use std::{
    io::{self, Read as _},
    num::NonZero,
    sync::Arc,
};

use rustls::{
    ClientConfig, ClientConnection, Connection, RootCertStore, ServerConfig, ServerConnection,
    crypto::aws_lc_rs, pki_types::CertificateDer, version,
};

use dope_test::harness::peer;

use crate::common::{Signer, state::name};

#[derive(Debug)]
pub enum Error {
    Peer(peer::Error),
    Io(io::Error),
    Config(dope_tls::Error),
    Tls(rustls::Error),
    Certificate(rcgen::Error),
    Handshake,
    Closed,
    ReadZero,
    WriteZero,
    FlightLimit,
    DataAfterClose,
    Close(NonZero<usize>),
}

pub fn output(tls: &mut Connection) -> Result<Vec<u8>, Error> {
    let mut wire = Vec::new();
    while tls.wants_write() {
        if tls.write_tls(&mut wire).map_err(Error::Io)? == 0 {
            return Err(Error::WriteZero);
        }
    }
    Ok(wire)
}

fn input(tls: &mut Connection, mut input: &[u8]) -> Result<(), Error> {
    while !input.is_empty() {
        if tls.read_tls(&mut input).map_err(Error::Io)? == 0 {
            return Err(Error::ReadZero);
        }
        tls.process_new_packets().map_err(Error::Tls)?;
    }
    Ok(())
}

pub fn write(tls: &mut Connection, socket: &peer::Stream<'_>) -> Result<(), Error> {
    socket.write_all(&output(tls)?).map_err(Error::Peer)
}

pub fn read(tls: &mut Connection, socket: &peer::Stream<'_>) -> Result<(), Error> {
    let mut bytes = [0; 16 * 1024];
    let count = socket.read(&mut bytes).map_err(Error::Peer)?;
    if count == 0 {
        tls.read_tls(&mut &[][..]).map_err(Error::Io)?;
        return Err(Error::Closed);
    }
    input(tls, &bytes[..count])
}

pub fn transfer(from: &mut Connection, to: &mut Connection) -> Result<(), Error> {
    input(to, &output(from)?)
}

pub fn pump(client: &mut Connection, server: &mut Connection) -> Result<(), Error> {
    for _ in 0..32 {
        if !client.wants_write() && !server.wants_write() {
            return Ok(());
        }
        transfer(client, server)?;
        transfer(server, client)?;
    }
    Err(Error::FlightLimit)
}

pub fn try_half_closed() -> Result<(ClientConnection, ServerConnection), Error> {
    let signer = Signer::new([42; 32]);
    let cert = CertificateDer::from(
        signer
            .certificate("example.com", rcgen::CertificateParams::default())
            .map_err(Error::Certificate)?,
    );
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone()).map_err(Error::Tls)?;
    let provider = Arc::new(aws_lc_rs::default_provider());
    let client = ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&version::TLS13])
        .map_err(Error::Tls)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let mut server = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&version::TLS13])
        .map_err(Error::Tls)?
        .with_no_client_auth()
        .with_single_cert(vec![cert], signer.key())
        .map_err(Error::Tls)?;
    server.send_tls13_tickets = 0;
    let mut client =
        Connection::Client(ClientConnection::new(Arc::new(client), name()).map_err(Error::Tls)?);
    let mut server =
        Connection::Server(ServerConnection::new(Arc::new(server)).map_err(Error::Tls)?);
    pump(&mut client, &mut server)?;
    if client.is_handshaking() || server.is_handshaking() {
        return Err(Error::Handshake);
    }
    client.send_close_notify();
    transfer(&mut client, &mut server)?;
    if let Some(count) = NonZero::new(server.reader().read(&mut [0; 1]).map_err(Error::Io)?) {
        return Err(Error::Close(count));
    }
    match (client, server) {
        (Connection::Client(client), Connection::Server(server)) => Ok((client, server)),
        _ => unreachable!("roles"),
    }
}
