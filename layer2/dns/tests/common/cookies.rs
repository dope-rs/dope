use std::mem;

const CODE: u16 = 10;
const CLIENT_BYTES: usize = 8;
const MAX_BYTES: usize = 40;

pub(crate) struct Client([u8; CLIENT_BYTES]);

impl Client {
    pub(crate) fn read(query: &[u8], end: usize) -> Option<Self> {
        let tail = query.get(end..)?;
        if !tail.starts_with(&[0, 0, 41]) {
            return None;
        }
        let length = usize::from(u16::from_be_bytes([*tail.get(9)?, *tail.get(10)?]));
        let mut options = tail.get(11..11usize.checked_add(length)?)?;
        while !options.is_empty() {
            let code = u16::from_be_bytes([*options.first()?, *options.get(1)?]);
            let length = usize::from(u16::from_be_bytes([*options.get(2)?, *options.get(3)?]));
            let end = 4usize.checked_add(length)?;
            let data = options.get(4..end)?;
            if code == CODE {
                if !(CLIENT_BYTES..=MAX_BYTES).contains(&length) {
                    return None;
                }
                let mut client = [0; CLIENT_BYTES];
                client.copy_from_slice(data.get(..CLIENT_BYTES)?);
                return Some(Self(client));
            }
            options = options.get(end..)?;
        }
        None
    }

    pub(crate) const fn into_bytes(self) -> [u8; CLIENT_BYTES] {
        self.0
    }
}

const _: () = assert!(mem::size_of::<Client>() == CLIENT_BYTES);
