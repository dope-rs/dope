use std::{
    io::{self, Read, Write},
    net, thread, time,
};

use dope::{
    core::driver::settings,
    manifold::{service::discover, timing},
    net::link::egress,
    runtime,
};
use dope_dns::{config, discovery, transport};
use dope_fiber::{abi, context, extensions, task::sleep};

use crate::fixture;

const A: u16 = 1;
const AAAA: u16 = 28;
const CNAME: u16 = 5;
const OPT: u16 = 41;
const IN: u16 = 1;
const QUESTION_OFFSET: usize = 12;
const POLL_INTERVAL: time::Duration = time::Duration::from_millis(1);
const LOOKUP_LIMIT: time::Duration = time::Duration::from_secs(3);

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
struct Host<'d, const N: usize> {
    #[pin]
    #[manifold]
    datagram: transport::datagram::Datagram<'d, 0, 1, N>,
    #[pin]
    #[manifold]
    stream: transport::stream::Stream<'d, 1, 1, N>,
}

enum LookupResult {
    Published(Vec<net::SocketAddr>),
    Failed(Box<discovery::Error>),
    Deadline,
}

#[pin_project::pin_project(!Unpin)]
struct Lookup<'d, const N: usize> {
    discovery: discovery::Discovery<'d, 1, N>,
    started: bool,
    limit: time::Instant,
    #[pin]
    delay: Option<sleep::Sleep<'d, 'd>>,
}

impl<'d, const N: usize> Lookup<'d, N> {
    fn new(discovery: discovery::Discovery<'d, 1, N>) -> Self {
        Self {
            discovery,
            started: false,
            limit: time::Instant::now() + LOOKUP_LIMIT,
            delay: None,
        }
    }
}

impl<'d, const N: usize> abi::Fiber<'d> for Lookup<'d, N> {
    type Output = LookupResult;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> std::task::Poll<Self::Output> {
        use std::task::Poll;

        let (this, mut context) = call.into_parts();
        let mut this = this.project();
        if let Some(delay) = this.delay.as_mut().as_pin_mut() {
            let Some(polled) = context.as_mut().try_poll(delay) else {
                return Poll::Pending;
            };
            if polled.is_pending() {
                return Poll::Pending;
            }
            this.delay.set(None);
        }

        let now = time::Instant::now();
        if now >= *this.limit {
            return Poll::Ready(LookupResult::Deadline);
        }
        if !*this.started {
            discover::Discover::refresh(&mut *this.discovery, now, discover::Refresh::Startup);
            *this.started = true;
        }

        let poll_at = match discover::Discover::poll(&mut *this.discovery, now) {
            discover::Action::Pending { poll_at } | discover::Action::Expired { poll_at, .. } => {
                poll_at
            }
            discover::Action::Published { snapshot, .. } => {
                let addresses = snapshot
                    .endpoints()
                    .iter()
                    .map(|endpoint| *endpoint.addr())
                    .collect();
                return Poll::Ready(LookupResult::Published(addresses));
            }
            discover::Action::Failed { error, .. } => {
                return Poll::Ready(LookupResult::Failed(Box::new(error)));
            }
        };

        let delay = poll_at
            .and_then(|at| at.checked_duration_since(now))
            .unwrap_or(POLL_INTERVAL)
            .min(POLL_INTERVAL);
        let timer = context.as_mut().driver_access().timer();
        this.delay.set(Some(
            sleep::Sleep::new(timer, delay).expect("lookup delay must fit the monotonic clock"),
        ));
        context.wake();
        Poll::Pending
    }
}

enum Reply {
    Addresses {
        values: &'static [[u8; 4]],
        unrelated_root: bool,
        edns: bool,
    },
    Alias([u8; 4]),
    NoData,
    Truncated,
    Refused,
    Name,
}

struct Question {
    end: usize,
    kind: u16,
}

fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn question(packet: &[u8]) -> io::Result<Question> {
    let mut cursor = QUESTION_OFFSET;
    loop {
        let length = usize::from(*packet.get(cursor).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "DNS question name is truncated")
        })?);
        cursor += 1;
        if length == 0 {
            break;
        }
        cursor = cursor.checked_add(length).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "DNS question name overflows")
        })?;
        if cursor > packet.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DNS question label is truncated",
            ));
        }
    }
    let trailer = packet.get(cursor..cursor + 4).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS question trailer is truncated",
        )
    })?;
    Ok(Question {
        end: cursor + 4,
        kind: u16::from_be_bytes([trailer[0], trailer[1]]),
    })
}

fn pointer(offset: usize) -> io::Result<[u8; 2]> {
    let offset = u16::try_from(offset)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "DNS pointer exceeds u16"))?;
    Ok((0xc000 | offset).to_be_bytes())
}

fn name(output: &mut Vec<u8>, labels: &[&[u8]]) {
    for label in labels {
        output.push(label.len() as u8);
        output.extend_from_slice(label);
    }
    output.push(0);
}

fn record(output: &mut Vec<u8>, owner: &[u8], kind: u16, class: u16, ttl: u32, data: &[u8]) {
    output.extend_from_slice(owner);
    put_u16(output, kind);
    put_u16(output, class);
    put_u32(output, ttl);
    put_u16(output, data.len() as u16);
    output.extend_from_slice(data);
}

fn response(query: &[u8], question: &Question, reply: Reply) -> io::Result<Vec<u8>> {
    let (rcode, answers, additional) = match &reply {
        Reply::Addresses {
            values,
            unrelated_root,
            edns,
        } => (
            0,
            values.len() + usize::from(*unrelated_root),
            usize::from(*edns),
        ),
        Reply::Alias(_) => (0, 2, 0),
        Reply::NoData => (0, 0, 0),
        Reply::Truncated => (0, 0, 0),
        Reply::Refused => (5, 0, 0),
        Reply::Name => (3, 0, 0),
    };
    let answers = u16::try_from(answers)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "too many DNS answers"))?;
    let mut output = Vec::new();
    output.extend_from_slice(
        query
            .get(..2)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "DNS ID is truncated"))?,
    );
    let additional = u16::try_from(additional)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "too many DNS additions"))?;
    let truncated = if matches!(&reply, Reply::Truncated) {
        0x0200
    } else {
        0
    };
    put_u16(&mut output, 0x8180 | truncated | rcode);
    put_u16(&mut output, 1);
    put_u16(&mut output, answers);
    put_u16(&mut output, 0);
    put_u16(&mut output, additional);
    output.extend_from_slice(
        query.get(QUESTION_OFFSET..question.end).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "DNS question is truncated")
        })?,
    );

    let expected = pointer(QUESTION_OFFSET)?;
    match reply {
        Reply::Addresses {
            values,
            unrelated_root,
            edns,
        } => {
            if unrelated_root {
                record(&mut output, &[0], A, IN, 90, &[198, 51, 100, 9]);
            }
            for value in values {
                record(&mut output, &expected, A, IN, 30, value);
            }
            if edns {
                record(&mut output, &[0], OPT, 1232, 0, &[]);
            }
        }
        Reply::Alias(address) => {
            let mut alias = Vec::new();
            name(&mut alias, &[b"edge", b"example"]);
            let alias_offset = output.len() + 12;
            record(&mut output, &expected, CNAME, IN, 20, &alias);
            record(&mut output, &pointer(alias_offset)?, A, IN, 60, &address);
        }
        Reply::NoData | Reply::Truncated | Reply::Refused | Reply::Name => {}
    }
    Ok(output)
}

fn read_frame(stream: &mut net::TcpStream) -> io::Result<Vec<u8>> {
    let mut prefix = [0; 2];
    stream.read_exact(&mut prefix)?;
    let mut frame = vec![0; usize::from(u16::from_be_bytes(prefix))];
    stream.read_exact(&mut frame)?;
    Ok(frame)
}

fn write_frame(stream: &mut net::TcpStream, frame: &[u8]) -> io::Result<()> {
    let length = u16::try_from(frame.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "DNS frame exceeds u16"))?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(frame)
}

fn serve_stream_retry() -> io::Result<(net::SocketAddr, thread::JoinHandle<io::Result<()>>)> {
    static ADDRESS: [[u8; 4]; 1] = [[192, 0, 2, 3]];

    let listener = net::TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(false)?;
    let address = listener.local_addr()?;
    let socket = net::UdpSocket::bind(address)?;
    socket.set_read_timeout(Some(LOOKUP_LIMIT))?;
    let handle = thread::spawn(move || {
        let mut packet = [0; 512];
        for _ in 0..2 {
            let (length, peer) = socket.recv_from(&mut packet)?;
            let query = &packet[..length];
            let question = question(query)?;
            let reply = if question.kind == A {
                Reply::Truncated
            } else {
                Reply::NoData
            };
            socket.send_to(&response(query, &question, reply)?, peer)?;
        }

        let (mut stream, _) = listener.accept()?;
        stream.set_read_timeout(Some(LOOKUP_LIMIT))?;
        stream.set_write_timeout(Some(LOOKUP_LIMIT))?;
        let malformed_query = read_frame(&mut stream)?;
        let malformed = malformed_query
            .get(..2)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "DNS ID is truncated"))?;
        write_frame(&mut stream, malformed)?;

        let retry = read_frame(&mut stream)?;
        let question = question(&retry)?;
        let answer = response(
            &retry,
            &question,
            Reply::Addresses {
                values: &ADDRESS,
                unrelated_root: false,
                edns: false,
            },
        )?;
        write_frame(&mut stream, &answer)
    });
    Ok((address, handle))
}

fn serve(
    exchanges: usize,
    mut script: impl FnMut(u16) -> Reply + Send + 'static,
) -> io::Result<(net::SocketAddr, thread::JoinHandle<io::Result<()>>)> {
    let socket = net::UdpSocket::bind("127.0.0.1:0")?;
    socket.set_read_timeout(Some(LOOKUP_LIMIT))?;
    let address = socket.local_addr()?;
    let handle = thread::spawn(move || {
        let mut packet = [0; 512];
        for _ in 0..exchanges {
            let (length, peer) = socket.recv_from(&mut packet)?;
            let query = &packet[..length];
            let question = question(query)?;
            let response = response(query, &question, script(question.kind))?;
            socket.send_to(&response, peer)?;
        }
        Ok(())
    });
    Ok((address, handle))
}

fn lookup<const N: usize>(server: net::SocketAddr, attempts: u8) -> io::Result<LookupResult> {
    let servers = config::Servers::try_from_iter([server])?;
    let storage = dope_dns::Storage::<1, N>::new(fixture::config(servers, attempts))?;
    let executor = runtime::executor::Executor::new(settings::Config::for_tcp_profile::<
        timing::Balanced,
    >(1)?)?
    .with_storage(storage);
    executor.enter(|mut session| {
        let seed = session.hash_state(dope_dns::HASH_DOMAIN);
        let storage = session.storage();
        let resolver = storage.bind::<0, 1, 4>(
            seed,
            egress::Config::default(),
            &mut session.driver_access(),
        )?;
        let discovery = resolver.discovery(dope_dns::Target::parse("api.example:443")?)?;
        let (datagram, stream) = resolver.into_parts();
        session.with_app(Host { datagram, stream }, |mut app| {
            extensions::AppSessionExt::block_on(&mut app, Lookup::new(discovery))
        })?
    })
}

fn finish(server: thread::JoinHandle<io::Result<()>>) {
    server
        .join()
        .expect("DNS responder thread must not panic")
        .expect("DNS responder must complete its script");
}

#[test]
fn public_lookup_filters_unrelated_and_duplicate_addresses() -> io::Result<()> {
    static DUPLICATES: [[u8; 4]; 2] = [[192, 0, 2, 1], [192, 0, 2, 1]];
    let (server, handle) = serve(2, |kind| {
        if kind == A {
            Reply::Addresses {
                values: &DUPLICATES,
                unrelated_root: true,
                edns: true,
            }
        } else {
            Reply::NoData
        }
    })?;

    let result = lookup::<1>(server, 1)?;
    finish(handle);
    let LookupResult::Published(addresses) = result else {
        panic!("lookup must publish its unique relevant address");
    };
    assert_eq!(addresses, [net::SocketAddr::from(([192, 0, 2, 1], 443))]);
    Ok(())
}

#[test]
fn public_lookup_resolves_a_compressed_alias() -> io::Result<()> {
    let (server, handle) = serve(2, |kind| {
        if kind == A {
            Reply::Alias([192, 0, 2, 2])
        } else {
            Reply::NoData
        }
    })?;

    let result = lookup::<1>(server, 1)?;
    finish(handle);
    let LookupResult::Published(addresses) = result else {
        panic!("lookup must publish the compressed alias target");
    };
    assert_eq!(addresses, [net::SocketAddr::from(([192, 0, 2, 2], 443))]);
    Ok(())
}

#[test]
fn public_lookup_reports_the_address_capacity() -> io::Result<()> {
    static UNIQUE: [[u8; 4]; 2] = [[192, 0, 2, 1], [192, 0, 2, 2]];
    let (server, handle) = serve(2, |kind| {
        if kind == A {
            Reply::Addresses {
                values: &UNIQUE,
                unrelated_root: false,
                edns: false,
            }
        } else {
            Reply::NoData
        }
    })?;

    let result = lookup::<1>(server, 1)?;
    finish(handle);
    let LookupResult::Failed(error) = result else {
        panic!("lookup must surface its endpoint capacity failure");
    };
    assert_eq!(
        error.kind(),
        discovery::ErrorKind::Capacity {
            limit: 1,
            actual: 2
        }
    );
    Ok(())
}

#[test]
fn public_lookup_retries_a_malformed_stream_response() -> io::Result<()> {
    let (server, handle) = serve_stream_retry()?;

    let result = lookup::<1>(server, 2)?;
    finish(handle);
    let LookupResult::Published(addresses) = result else {
        panic!("lookup must retry its malformed stream response");
    };
    assert_eq!(addresses, [net::SocketAddr::from(([192, 0, 2, 3], 443))]);
    Ok(())
}

#[test]
fn public_lookup_retries_a_refused_datagram_response() -> io::Result<()> {
    static ADDRESS: [[u8; 4]; 1] = [[192, 0, 2, 4]];
    let mut a_queries = 0;
    let (server, handle) = serve(3, move |kind| {
        if kind == AAAA {
            return Reply::NoData;
        }
        a_queries += 1;
        if a_queries == 1 {
            Reply::Refused
        } else {
            Reply::Addresses {
                values: &ADDRESS,
                unrelated_root: false,
                edns: false,
            }
        }
    })?;

    let result = lookup::<1>(server, 2)?;
    finish(handle);
    let LookupResult::Published(addresses) = result else {
        panic!("lookup must retry its refused datagram response");
    };
    assert_eq!(addresses, [net::SocketAddr::from(([192, 0, 2, 4], 443))]);
    Ok(())
}

#[test]
fn public_lookup_preserves_nxdomain_as_a_name_failure() -> io::Result<()> {
    let (server, handle) = serve(2, |_| Reply::Name)?;

    let result = lookup::<1>(server, 1)?;
    finish(handle);
    let LookupResult::Failed(error) = result else {
        panic!("lookup must surface NXDOMAIN");
    };
    assert_eq!(error.kind(), discovery::ErrorKind::Name);
    Ok(())
}
