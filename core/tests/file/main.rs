mod sealed;
mod tests;

pub(crate) use sealed::{dispatch_all, open_fds_for, retained_context, submit_retained};
