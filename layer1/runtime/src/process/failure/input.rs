use dope_core::platform::affinity;

use crate::process::failure::create;

#[derive(Debug)]
pub enum Error<E> {
    Layout(create::Error),
    Replica { cpu: affinity::Cpu, cause: E },
}
