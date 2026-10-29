use dope_sys::io::reactor::complete::message::{Cut, Region};

use crate::reactor::cq::message::{Error, Length};

pub(in crate::reactor::cq::message) struct Span<'a> {
    pub(in crate::reactor::cq::message) bytes: &'a [u8],
    pub(in crate::reactor::cq::message) cut: Cut,
}

impl<'a> Span<'a> {
    pub(in crate::reactor::cq::message) fn decode(bytes: [u8; 4]) -> Length {
        Length::from_raw(u32::from_ne_bytes(bytes))
    }

    pub(in crate::reactor::cq::message) fn name(storage: &'a [u8], reported: Length) -> Self {
        let reported = reported.into_raw();
        Self {
            bytes: &storage[..reported.min(storage.len())],
            cut: if reported > storage.len() {
                Cut::Truncated
            } else {
                Cut::Complete
            },
        }
    }

    pub(in crate::reactor::cq::message) fn flagged(
        storage: &'a [u8],
        reported: Length,
        cut: bool,
        region: Region,
    ) -> Result<Self, Error> {
        Self::validate(storage, reported, cut, region)?;
        let len = reported.into_raw();
        Ok(Self {
            bytes: &storage[..len.min(storage.len())],
            cut: if cut { Cut::Truncated } else { Cut::Complete },
        })
    }

    pub(in crate::reactor::cq::message) fn validate(
        storage: &'a [u8],
        reported: Length,
        cut: bool,
        region: Region,
    ) -> Result<(), Error> {
        let len = reported.into_raw();
        if len > storage.len() && !cut {
            return Err(Error::MissingTruncation {
                region,
                reported,
                available: Length::from_usize(storage.len()),
            });
        }
        Ok(())
    }
}
