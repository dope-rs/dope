use std::io;
use std::mem;
use std::pin::Pin;
use std::task::Poll;

use o3::buffer::Block;

use super::already_done;
use super::{Source, SourceRef};
use crate::{Context, Fiber};
use dope::DriverContext;
use dope::driver::ready::CompletionWaker;
use dope::driver::token::Token;
use dope::manifold::file::{FileOutcome, Files, ReadDone};

enum Phase<'d, B> {
    Ready(B, usize),
    Init { source: SourceRef<'d>, buffer: B },
    Pending(Token),
    Done,
}

struct ReadState<'h, 'd, const ID: u8, const N: usize, B> {
    host: &'h Files<'d, ID, N>,
    offset: u64,
    phase: Phase<'d, B>,
}

impl<'h, 'd, const ID: u8, const N: usize, B> ReadState<'h, 'd, ID, N, B> {
    fn new<K>(
        host: &'h Files<'d, ID, N>,
        src: &Source<'d, K>,
        buffer: B,
        offset: u64,
        ready: Option<usize>,
    ) -> Self {
        let phase = match ready {
            Some(output) => Phase::Ready(buffer, output),
            None => Phase::Init {
                source: src.lease(),
                buffer,
            },
        };
        Self {
            host,
            offset,
            phase,
        }
    }

    fn enter_poll(&mut self) -> Phase<'d, B> {
        if let Phase::Pending(token) = self.phase {
            return Phase::Pending(token);
        }
        mem::replace(&mut self.phase, Phase::Done)
    }

    fn poll(
        &mut self,
        mut cx: Pin<&mut Context<'_, 'd>>,
        begin: impl FnOnce(
            &Files<'d, ID, N>,
            SourceRef<'d>,
            B,
            u64,
            &mut DriverContext<'_, 'd>,
        ) -> Result<Token, (B, io::Error)>,
        complete: impl FnOnce(
            &Files<'d, ID, N>,
            Token,
            CompletionWaker<'d>,
        ) -> FileOutcome<(B, ReadDone)>,
    ) -> Poll<(B, io::Result<usize>)>
    where
        B: Default,
    {
        let token = match self.enter_poll() {
            Phase::Done => return Poll::Ready((B::default(), Err(already_done()))),
            Phase::Ready(buffer, output) => return Poll::Ready((buffer, Ok(output))),
            Phase::Pending(token) => token,
            Phase::Init { source, buffer } => {
                let begun = {
                    let mut driver = cx.as_mut().driver_access();
                    begin(self.host, source, buffer, self.offset, &mut driver)
                };
                match begun {
                    Ok(token) => {
                        self.phase = Phase::Pending(token);
                        token
                    }
                    Err((buffer, error)) => {
                        return Poll::Ready((buffer, Err(error)));
                    }
                }
            }
        };
        match complete(self.host, token, cx.completion_waker()) {
            FileOutcome::Done((buffer, done)) => {
                self.phase = Phase::Done;
                Poll::Ready((
                    buffer,
                    match done {
                        ReadDone::Complete(output) => Ok(output),
                        ReadDone::Failed(error) => Err(error),
                    },
                ))
            }
            FileOutcome::Pending => Poll::Pending,
        }
    }

    fn cancel(&self, cancel: impl FnOnce(&Files<'d, ID, N>, Token)) {
        if let Phase::Pending(token) = self.phase {
            cancel(self.host, token);
        }
    }
}

pub struct Read<'h, 'd, const ID: u8, const N: usize> {
    state: ReadState<'h, 'd, ID, N, Vec<u8>>,
}

impl<'h, 'd, const ID: u8, const N: usize> Read<'h, 'd, ID, N> {
    pub fn new<K>(
        host: &'h Files<'d, ID, N>,
        src: &Source<'d, K>,
        buffer: Vec<u8>,
        offset: u64,
    ) -> Self {
        let ready = buffer.is_empty().then_some(0);
        Self {
            state: ReadState::new(host, src, buffer, offset, ready),
        }
    }
}

impl<const ID: u8, const N: usize> Drop for Read<'_, '_, ID, N> {
    fn drop(&mut self) {
        self.state.cancel(|host, token| host.cancel_read(token));
    }
}

impl<'h, 'd, const ID: u8, const N: usize> Fiber<'d> for Read<'h, 'd, ID, N> {
    type Output = (Vec<u8>, io::Result<usize>);

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        self.get_mut().state.poll(
            cx,
            |host, source, buffer, offset, driver| host.begin_read(source, buffer, offset, driver),
            |host, token, wake| host.poll_read(token, wake),
        )
    }
}

pub struct BlockRead<'h, 'd, const ID: u8, const N: usize> {
    state: ReadState<'h, 'd, ID, N, Block>,
}

impl<'h, 'd, const ID: u8, const N: usize> BlockRead<'h, 'd, ID, N> {
    pub fn new<K>(
        host: &'h Files<'d, ID, N>,
        src: &Source<'d, K>,
        buffer: Block,
        offset: u64,
    ) -> Self {
        let ready = (buffer.len() == Block::CAPACITY).then(|| buffer.len());
        Self {
            state: ReadState::new(host, src, buffer, offset, ready),
        }
    }
}

impl<const ID: u8, const N: usize> Drop for BlockRead<'_, '_, ID, N> {
    fn drop(&mut self) {
        self.state
            .cancel(|host, token| host.cancel_block_read(token));
    }
}

impl<'h, 'd, const ID: u8, const N: usize> Fiber<'d> for BlockRead<'h, 'd, ID, N> {
    type Output = (Block, io::Result<usize>);

    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        self.get_mut().state.poll(
            cx,
            |host, source, buffer, offset, driver| {
                host.begin_block_read(source, buffer, offset, driver)
            },
            |host, token, wake| host.poll_block_read(token, wake),
        )
    }
}
