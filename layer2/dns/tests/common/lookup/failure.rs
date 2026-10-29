use std::{io, net::SocketAddr, num::TryFromIntError};

use dope::{manifold::app::creation, runtime::executor::session};
use dope_dns::{
    TargetError,
    lookup::{create, error::Error},
};
use dope_test::fibers;
use o3::alloc;

#[derive(Debug)]
pub enum Run {
    Repeated,
    Allocated(usize),
    TargetClone,
    LaneAdmission,
    Exhausted(alloc::Exhausted),
    Target(TargetError),
    Create(create::Error),
    Fiber(fibers::Run),
}

#[derive(Debug)]
pub enum App<'report> {
    Install(creation::Error),
    Session(session::Error<'report, Result<Result<Option<SocketAddr>, Error<1>>, Run>>),
    Run(Run),
}

#[derive(Debug)]
pub enum Server {
    Cookie,
    Header,
    Label { offset: usize, value: u8 },
    Name,
    Stream,
    Size { actual: usize, expected: usize },
    Kind { actual: u16, expected: u16 },
    Pointer,
    Offset(usize),
    Length(TryFromIntError),
    Io(io::Error),
}
