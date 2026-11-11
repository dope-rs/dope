use crate::conn::wire::{path::probe, storage};
use o3::depot;

pub(in crate::conn) enum Challenge {}
pub(in crate::conn) enum Response {}

// SAFETY: encoding selected this same initialized challenge in its original
// ready prefix. The packet owner excludes any removal through publication.
unsafe impl depot::raw::Project<(probe::Position, usize), Challenge> for &mut probe::State {
    type Output = ();

    unsafe fn project(self, (index, bytes): (probe::Position, usize)) {
        let mut entry = unsafe {
            depot::raw::Project::<_, storage::Selected<'_, _, _, true>>::project(
                &mut self.challenges,
                usize::from(index.0),
            )
        };
        let sent = &mut entry.get_mut().sent;
        // A delayed response to a short transmission cannot prove a later MTU probe.
        *sent = if bytes < crate::proto::recovery::pmtud::BASE_PMTU as usize
            || matches!(sent, probe::Sent::Short)
        {
            probe::Sent::Short
        } else {
            probe::Sent::Expanded
        };
    }
}

// SAFETY: every earlier ready-prefix relocation has updated this retained
// selection. The exclusive packet owner consumes each selected reply once.
unsafe impl depot::raw::Project<probe::Position, Response> for &mut probe::State {
    type Output = Option<probe::Relocated>;

    unsafe fn project(self, index: probe::Position) -> Self::Output {
        let entry = unsafe {
            depot::raw::Project::<_, storage::Selected<'_, _, _, true>>::project(
                &mut self.response_controls,
                usize::from(index.0),
            )
        };
        let moved = entry.relocation().map(|(from, to)| probe::Relocated {
            from: probe::Position(from as u8),
            to: probe::Position(to as u8),
        });
        entry.take();
        moved
    }
}

const _: () = assert!(crate::conn::MAX_PATH_TOKENS <= u8::MAX as usize);
