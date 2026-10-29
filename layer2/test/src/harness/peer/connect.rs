use std::{io, net};

#[derive(Debug)]
pub enum Connect {
    Open(io::Error),
    ReadTimeout {
        stream: net::TcpStream,
        error: io::Error,
    },
}
