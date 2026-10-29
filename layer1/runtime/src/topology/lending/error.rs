use crate::executor::session;

/// Construction and execution failures retain their exact original causes.
#[derive(Debug)]
pub enum Error<'report, E, T> {
    Build(E),
    Run(session::Error<'report, T>),
}
