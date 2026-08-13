use std::{fmt, io};

use fmt::Write as _;

const MAX_PRESENTATION_LEN: usize = 253;

#[derive(Clone, Copy)]
enum AssemblyError {
    NoLabels,
    NameLength,
    LabelLength,
    LabelHyphen,
    LabelCharacter,
}

/// Validated canonical ASCII hostname stored inline.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Name {
    bytes: [u8; MAX_PRESENTATION_LEN],
    len: u8,
}

impl Name {
    pub(crate) const MAX_WIRE_LEN: usize = MAX_PRESENTATION_LEN + 2;

    pub(crate) const fn wire_len(&self) -> usize {
        self.len as usize + 2
    }

    pub fn parse(input: &str) -> io::Result<Self> {
        let input = match input.strip_suffix('.') {
            Some(without_root) => without_root,
            None => input,
        };
        Self::assemble(input.len(), input.split('.').map(str::as_bytes)).map_err(presentation_error)
    }

    pub(crate) fn labels(&self) -> impl Iterator<Item = &[u8]> {
        self.bytes[..usize::from(self.len)].split(|byte| *byte == b'.')
    }

    pub(crate) fn from_wire_labels(labels: &[&[u8]]) -> io::Result<Self> {
        let Some(separators) = labels.len().checked_sub(1) else {
            return Err(invalid("decoded DNS name has no labels"));
        };
        let Some(label_bytes) = labels
            .iter()
            .try_fold(0usize, |total, label| total.checked_add(label.len()))
        else {
            return Err(invalid("decoded DNS name length overflowed"));
        };
        let Some(total) = label_bytes.checked_add(separators) else {
            return Err(invalid("decoded DNS name length overflowed"));
        };
        Self::assemble(total, labels.iter().copied()).map_err(wire_error)
    }

    fn assemble<'a>(
        total: usize,
        labels: impl Iterator<Item = &'a [u8]>,
    ) -> Result<Self, AssemblyError> {
        if total == 0 || total > MAX_PRESENTATION_LEN {
            return Err(AssemblyError::NameLength);
        }
        let mut bytes = [0; MAX_PRESENTATION_LEN];
        let mut offset = 0;
        for label in labels {
            if label.is_empty() || label.len() > 63 {
                return Err(AssemblyError::LabelLength);
            }
            validate_label(label)?;
            if offset != 0 {
                bytes[offset] = b'.';
                offset += 1;
            }
            for &byte in label {
                bytes[offset] = byte.to_ascii_lowercase();
                offset += 1;
            }
        }
        if offset == 0 {
            return Err(AssemblyError::NoLabels);
        }
        if offset != total {
            return Err(AssemblyError::NameLength);
        }
        Ok(Self {
            bytes,
            len: total as u8,
        })
    }
}

fn validate_label(label: &[u8]) -> Result<(), AssemblyError> {
    if label.first() == Some(&b'-') || label.last() == Some(&b'-') {
        return Err(AssemblyError::LabelHyphen);
    }
    if label
        .iter()
        .any(|byte| !(byte.is_ascii_alphanumeric() || *byte == b'-' || *byte == b'_'))
    {
        return Err(AssemblyError::LabelCharacter);
    }
    Ok(())
}

fn presentation_error(error: AssemblyError) -> io::Error {
    invalid(match error {
        AssemblyError::NoLabels | AssemblyError::NameLength => "DNS name length must be in 1..=253",
        AssemblyError::LabelLength => "DNS label length must be in 1..=63",
        AssemblyError::LabelHyphen => "DNS host labels cannot begin or end with '-'",
        AssemblyError::LabelCharacter => {
            "DNS host labels must be ASCII letters, digits, '-' or '_'"
        }
    })
}

fn wire_error(error: AssemblyError) -> io::Error {
    invalid(match error {
        AssemblyError::NoLabels => "decoded DNS name has no labels",
        AssemblyError::NameLength => "decoded DNS name exceeds the host-name bound",
        AssemblyError::LabelLength => "decoded DNS label exceeds 63 bytes",
        AssemblyError::LabelHyphen => "DNS host labels cannot begin or end with '-'",
        AssemblyError::LabelCharacter => {
            "DNS host labels must be ASCII letters, digits, '-' or '_'"
        }
    })
}

impl fmt::Display for Name {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.bytes[..usize::from(self.len)] {
            formatter.write_char(char::from(*byte))?;
        }
        Ok(())
    }
}

impl fmt::Debug for Name {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Name({self})")
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
