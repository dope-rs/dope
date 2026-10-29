use dope_core::io;
use dope_test::reclaim;

pub struct Canceled;

impl reclaim::Matches<io::Reclaimed> for Canceled {
    fn matches(&self, event: &io::Reclaimed) -> bool {
        matches!(event, io::Reclaimed::Recv { result: Err(error), flow: io::event::batch::Flow::Last, .. } if *error == dope_core::Errno::CANCELED)
    }
}
