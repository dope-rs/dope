use dope_net::wire::Cursor;
use dope_test::checks::TrackingAlloc;

struct Slice(&'static [u8]);

impl<'d> Cursor<'d> for Slice {
    fn chunk(&self) -> &[u8] {
        self.0
    }

    fn consume(&mut self, requested: usize) -> usize {
        let consumed = requested.min(self.0.len());
        self.0 = &self.0[consumed..];
        consumed
    }

    fn remaining(&self) -> usize {
        self.0.len()
    }

    fn retain(
        &self,
        range: std::ops::Range<usize>,
        _: &o3::buffer::resident::Budget<'d>,
    ) -> Result<dope_net::wire::RetainedBytes<'d>, dope_net::wire::RetainError> {
        self.0
            .get(range)
            .map(|bytes| {
                use o3::buffer::bytes::Bytes;
                dope_net::wire::RetainedBytes::from_buffered(Bytes::copy_from_slice(bytes))
            })
            .ok_or(dope_net::wire::RetainError::Range)
    }
}

#[test]
fn receive_cursor_exposes_and_consumes_storage_without_allocation() {
    let mut source = Slice(b"abcdef");
    let allocation = TrackingAlloc::<0>::during(|| {
        assert_eq!(source.chunk(), b"abcdef");
        assert_eq!(source.consume(3), 3);
        assert_eq!(source.chunk(), b"def");
        assert_eq!(source.consume(usize::MAX), 3);
    });

    assert_eq!(allocation, (0, 0));
    assert!(source.is_empty());
}
