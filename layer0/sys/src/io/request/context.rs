/// Typed inputs whose authority is preserved during native request lowering.
pub trait Context {
    type Target: Copy + Into<[u8; 3]>;
    type Outbound: Copy + Into<[u8; 3]>;
    type Datagram<'d>;

    fn datagram<'d>(fd: &Self::Datagram<'d>) -> Self::Target;
}
