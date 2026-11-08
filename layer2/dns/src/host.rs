use std::{mem, str};

use o3::buffer::{bytes, storage::inline, write::consume};

use crate::wire::packet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Error {
    NoLabels,
    Capacity(consume::Error),
    NameLength {
        actual: usize,
        minimum: usize,
        maximum: usize,
    },
    LabelLength {
        index: usize,
        actual: usize,
        minimum: usize,
        maximum: usize,
    },
    LabelHyphen {
        index: usize,
        offset: usize,
    },
    LabelCharacter {
        index: usize,
        offset: usize,
        value: u8,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mismatch {
    LabelLength {
        index: usize,
        actual: usize,
        expected: usize,
    },
    LabelByte {
        index: usize,
        offset: usize,
        actual: u8,
        expected: u8,
    },
    ExtraLabel {
        index: usize,
        actual: usize,
    },
    MissingLabel {
        index: usize,
        expected: usize,
    },
}

const MAX_PRESENTATION_LEN: usize = 253;
const MIN_PRESENTATION_LEN: usize = 1;
const MAX_LABEL_LEN: usize = 63;
const MIN_LABEL_LEN: usize = 1;

/// Validated canonical ASCII hostname stored inline.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Name {
    bytes: inline::Bytes<MAX_PRESENTATION_LEN>,
}

impl Name {
    pub(crate) const MAX_WIRE_LEN: usize = MAX_PRESENTATION_LEN + 2;

    pub(crate) fn wire_len(&self) -> usize {
        self.bytes.len() + 2
    }

    pub fn parse(input: &str) -> Result<Self, Error> {
        let input = match input.strip_suffix('.') {
            Some(trimmed) => trimmed,
            None => input,
        };
        Self::assemble(input.len(), input.split('.').map(str::as_bytes))
    }

    pub(crate) fn try_decode(expanded: packet::Expanded<'_>) -> Result<Self, Error> {
        let (Some(total), labels) = expanded.into_parts() else {
            return Err(Error::NoLabels);
        };
        Self::assemble(total, labels)
    }

    fn assemble<'a>(total: usize, labels: impl Iterator<Item = &'a [u8]>) -> Result<Self, Error> {
        if total < MIN_PRESENTATION_LEN {
            return Err(Error::NameLength {
                actual: total,
                minimum: MIN_PRESENTATION_LEN,
                maximum: MAX_PRESENTATION_LEN,
            });
        }
        let mut bytes = inline::Bytes::new();
        let append = match bytes.try_reserve(total) {
            Ok(append) => append,
            Err(error) => return Err(Error::Capacity(error)),
        };
        append.try_write(|output| {
            let mut offset = 0;
            for (index, label) in labels.enumerate() {
                if !(MIN_LABEL_LEN..=MAX_LABEL_LEN).contains(&label.len()) {
                    return Err(Error::LabelLength {
                        index,
                        actual: label.len(),
                        minimum: MIN_LABEL_LEN,
                        maximum: MAX_LABEL_LEN,
                    });
                }
                if index != 0 {
                    let Some(dot) = output.get_mut(offset) else {
                        unreachable!("name");
                    };
                    *dot = b'.';
                    offset += 1;
                }
                if label.first() == Some(&b'-') {
                    return Err(Error::LabelHyphen { index, offset });
                }
                if label.last() == Some(&b'-') {
                    return Err(Error::LabelHyphen {
                        index,
                        offset: offset + label.len() - 1,
                    });
                }
                let end = offset + label.len();
                let Some(dst) = output.get_mut(offset..end) else {
                    unreachable!("name");
                };
                for (relative, (dst, &value)) in dst.iter_mut().zip(label).enumerate() {
                    if !(value.is_ascii_alphanumeric() || value == b'-' || value == b'_') {
                        return Err(Error::LabelCharacter {
                            index,
                            offset: offset + relative,
                            value,
                        });
                    }
                    *dst = value.to_ascii_lowercase();
                }
                offset = end;
            }
            Ok(())
        })?;
        Ok(Self { bytes })
    }

    pub fn as_str(&self) -> &str {
        // SAFETY: assemble stores only ASCII labels and dots; this borrow retains them.
        unsafe { str::from_utf8_unchecked(self.bytes.as_ref()) }
    }

    pub(crate) fn labels(&self) -> impl Iterator<Item = &[u8]> {
        self.bytes.as_ref().split(|byte| *byte == b'.')
    }
}

impl<'a> From<&'a Name> for bytes::Prefix<'a, MAX_PRESENTATION_LEN> {
    fn from(name: &'a Name) -> Self {
        Self::from(&name.bytes)
    }
}

const _: () = assert!(mem::size_of::<Name>() == MAX_PRESENTATION_LEN + 1);
