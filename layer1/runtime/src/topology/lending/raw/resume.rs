use crate::executor;

/// # Safety
/// After stopped quiescence, native retirement leaves no state awaiting dispatch before release or finish.
/// Dependent destruction may queue new cleanup that remains valid after the same driver resumes.
pub unsafe trait Resume<'d>: executor::raw::App<'d> {}
