use dope_sys::{
    probe,
    probe::receive,
    table::{token, token::host},
};

use crate::{
    driver::{
        life::native::create,
        result,
        retained::flight,
        route,
        route::{indices, kind},
    },
    io::fd::domain::retire,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Context;

impl token::Context for Context {
    type Slot = indices::SlotIndex;
    type Control = route::Control;

    const ZERO: Self::Slot = indices::SlotIndex::ZERO;
    const SHUTDOWN: Self::Control = route::SHUTDOWN;

    fn control(code: u8, slot: Self::Slot) -> Self::Control {
        route::Control::new(kind::Code::new(code), slot)
    }
}

impl host::Context for Context {
    type Key = flight::raw::Echo;
    type Close<'d> = retire::Close<'d>;
    type Retire<'d> = retire::Retire<'d>;
    type Returned = retire::Returned;
    type Probe = result::Probe;

    fn read_key(raw: u64) -> Option<Self::Key> {
        // SAFETY: this leaf validates the kernel word through the arena's
        // original echo constructor before any host ownership can be bound.
        unsafe { flight::raw::Echo::from_kernel(raw) }
    }
}

impl probe::Context for Context {
    const RING: Self::Probe = result::Probe::Ring;
    const SOCKET: Self::Probe = result::Probe::DirectSocket;
    const OPTION: Self::Probe = result::Probe::SocketOption;
    const CLOSE: Self::Probe = result::Probe::CloseDirectSocket;
    const RECEIVE: Self::Probe = result::Probe::Receive;
    const MESSAGE: Self::Probe = result::Probe::ReceiveMessage;
    const LISTENER: Self::Probe = result::Probe::RegisterAcceptListener;
    const ACCEPT: Self::Probe = result::Probe::DirectAccept;
    const MULTISHOT: Self::Probe = result::Probe::DirectAcceptMultishot;
    const ACCEPT_CLOSE: Self::Probe = result::Probe::CloseDirectAccept;
    const MULTI_CLOSE: Self::Probe = result::Probe::CloseDirectAcceptMultishot;
    const RELEASE: Self::Probe = result::Probe::ReleaseAcceptListener;
}

impl<V: receive::Receive> receive::Context<V> for Context {
    type Check = result::ProbeError;

    fn inspect(mode: receive::Mode, bytes: &[u8], want: &[u8]) -> Result<(), Self::Check> {
        use dope_sys::io::reactor::complete::message;
        use o3::base::num::bounded;

        use crate::{
            driver::receive::{Capacity, Length},
            io::socket::Addr,
        };

        if mode == receive::Mode::Data {
            return V::bytes(bytes, want).map_err(|receive::Corrupt| {
                result::ProbeError::new(result::Probe::Receive, result::Violation::CorruptPayload)
            });
        }
        V::message::<{ Addr::STORAGE_CAPACITY }, _>(bytes, want, Addr::try_parse).map_err(|error| {
            let fault = match error {
                receive::Failure::Payload(receive::Corrupt) => result::Violation::CorruptPayload,
                receive::Failure::Truncated => result::Violation::TruncatedFrame,
                receive::Failure::Address(error) => result::Violation::InvalidAddress(error),
                receive::Failure::Frame(message::Error::Overflow {
                    name,
                    control: ctrl,
                }) => {
                    let name: bounded::U64<0, { u64::MAX }> = name.into();
                    let ctrl: bounded::U64<0, { u64::MAX }> = ctrl.into();
                    result::Violation::FrameOverflow {
                        name: Capacity::from(name),
                        control: Capacity::from(ctrl),
                    }
                }
                receive::Failure::Frame(message::Error::Short {
                    actual,
                    required: need,
                }) => {
                    let actual: bounded::U64<0, { u64::MAX }> = actual.into();
                    let need: bounded::U64<0, { u64::MAX }> = need.into();
                    result::Violation::FrameShort {
                        actual: Length::from(actual),
                        required: Length::from(need),
                    }
                }
                receive::Failure::Frame(message::Error::MissingTruncation {
                    region,
                    reported,
                    available: cap,
                }) => {
                    let reported: bounded::U64<0, { u64::MAX }> = reported.into();
                    let cap: bounded::U64<0, { u64::MAX }> = cap.into();
                    result::Violation::MissingFrameTruncation {
                        region: match region {
                            message::Region::Control => result::FrameRegion::Control,
                            message::Region::Payload => result::FrameRegion::Payload,
                        },
                        reported: Length::from(reported),
                        available: Length::from(cap),
                    }
                }
            };
            result::ProbeError::new(result::Probe::ReceiveMessage, fault)
        })
    }
}

impl<E> probe::Build<E> for Context
where
    create::Kernel: From<E>,
{
    type Primary = create::Failure;
    type Removed = (result::IoOperation, result::Kernel);

    fn remove(mode: receive::Mode, error: E) -> Self::Removed {
        let op = match mode {
            receive::Mode::Data => result::IoOperation::RemoveReceiveFile,
            receive::Mode::Message => result::IoOperation::RemoveReceiveMessageFile,
        };
        (op, create::Kernel::from(error).0)
    }

    fn release(probe: Self::Probe, error: E) -> Self::Removed {
        (
            result::IoOperation::Probe(probe),
            create::Kernel::from(error).0,
        )
    }
}

const _: () = {
    assert!(flight::raw::Echo::BITS == 56);
    assert!(route::SHIFT == 56);
    assert!(route::SLOT_BITS == 24);
    assert!(route::FRAMEWORK == u8::MAX);
};
