use dope_net::wire::{self, Cursor};
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

#[test]
fn resident_cursor_owns_alignment_without_layout_or_allocation_cost() {
    use std::{mem, pin::pin};

    use dope_core::driver::{Driver, settings};
    use o3::buffer::{resident, storage};

    assert_eq!(
        mem::size_of::<wire::ResidentCursor<'static, 'static, 64>>(),
        mem::size_of::<(
            storage::Shared,
            &'static mut resident::Snapshot<'static, 64>,
        )>(),
    );

    let mut driver = pin!(
        Driver::new(settings::Config::for_quic_udp(2, 64).expect("driver config")).expect("driver")
    );
    crate::scope(driver.as_mut(), |mut scope| {
        let mut context = scope.context();
        let budget = resident::Budget::new(64, context.region_token());
        let mut owner = resident::Snapshot::<64>::new(&budget);
        owner.try_extend(b"abcdef").expect("resident bytes");

        let retained = {
            let mut cursor = wire::ResidentCursor::new(&mut owner);
            let ((remaining, retained), allocation) = TrackingAlloc::<0>::measure(|| {
                assert_eq!(cursor.chunk(), b"abcdef");
                assert_eq!(cursor.consume(2), 2);
                let retained = cursor.retain(1..3, &budget).expect("retained range");
                assert_eq!(retained.as_slice(), b"de");
                (cursor.remaining(), retained)
            });
            assert_eq!(allocation, (0, 0));
            assert_eq!(remaining, 4);
            retained
        };

        assert_eq!(owner.len(), 4);
        assert_eq!(
            owner.snapshot().expect("remaining bytes").as_slice(),
            b"cdef"
        );
        drop(retained);

        let mut cursor = wire::ResidentCursor::new(&mut owner);
        assert_eq!(cursor.consume(usize::MAX), 4);
        assert!(cursor.is_empty());
        drop(cursor);
        assert!(owner.is_empty());
    });
}
