pub enum Lowered<I, O, D, M> {
    AcceptMultishot { listener: I },
    RecvMulti { slot: I },
    RecvMsgMulti(M),
    ShutdownWrite { slot: I },
    SocketAt { socket: D, slot: O },
}
