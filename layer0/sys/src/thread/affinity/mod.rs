pub mod raw;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    DiscoveryUnsupported,
    BindingUnsupported { cpu: raw::Cpu },
    BindingRange { cpu: raw::Cpu, maximum: u16 },
    Current(crate::Errno),
    Bind { cpu: raw::Cpu, fault: crate::Errno },
}
