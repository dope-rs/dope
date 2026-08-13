use io_uring::opcode;

pub(super) struct Opcode {
    pub(super) code: u8,
    pub(super) name: &'static str,
}

impl Opcode {
    const fn new(code: u8, name: &'static str) -> Self {
        Self { code, name }
    }
}

/// Complete opcode contract of the Linux backend.
///
/// Flag-encoded variants are admitted separately by executable canaries.
pub(super) const OPCODES: &[Opcode] = &[
    Opcode::new(opcode::Nop::CODE, "NOP"),
    Opcode::new(opcode::PollAdd::CODE, "POLL_ADD"),
    Opcode::new(opcode::SendMsg::CODE, "SENDMSG"),
    Opcode::new(opcode::RecvMsg::CODE, "RECVMSG"),
    Opcode::new(opcode::Accept::CODE, "ACCEPT"),
    Opcode::new(opcode::AsyncCancel::CODE, "ASYNC_CANCEL"),
    Opcode::new(opcode::Connect::CODE, "CONNECT"),
    Opcode::new(opcode::Close::CODE, "CLOSE"),
    Opcode::new(opcode::Statx::CODE, "STATX"),
    Opcode::new(opcode::Read::CODE, "READ"),
    Opcode::new(opcode::Send::CODE, "SEND"),
    Opcode::new(opcode::Recv::CODE, "RECV"),
    Opcode::new(opcode::OpenAt2::CODE, "OPENAT2"),
    Opcode::new(opcode::Shutdown::CODE, "SHUTDOWN"),
    Opcode::new(opcode::Socket::CODE, "SOCKET"),
    Opcode::new(opcode::SetSockOpt::CODE, "URING_CMD"),
];
