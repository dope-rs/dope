use dope_manifold::client;

use client::session;
use session::response::{Direct, Outcome};

enum Custom {
    Accepted,
    Retry,
}

impl Outcome for Custom {
    type Accepted = ();

    fn accept((): ()) -> Self {
        Self::Retry
    }

    fn retry_input() -> Self {
        Self::Retry
    }

    fn committed() -> Self {
        Self::Accepted
    }

    fn disposition(&self) -> Direct {
        match self {
            Self::Accepted => Direct::Committed,
            Self::Retry => Direct::RetryInput,
        }
    }
}

fn main() {}
