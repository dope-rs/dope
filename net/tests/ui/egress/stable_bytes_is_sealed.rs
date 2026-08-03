use dope_net::link::egress::StableBytes;

struct ApplicationBytes(Vec<u8>);

impl AsRef<[u8]> for ApplicationBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl StableBytes for ApplicationBytes {}

fn main() {}
