fn resume_required<'scope, 'life: 'scope, 'p: 'life, 'd: 'p, S, Q, B>(
    stage: &mut lending::Stage<'_, 'scope, 'life, 'p, 'd, S, Q>,
    report: &mut session::Completion,
    build: B,
) where
    S: storage::Plan + 'd,
    Q: executor::Startup,
    B: lending::Execute<'life, 'd, Value, Q>,
{
    let _ = stage.with_lent(report, Value, build);
}
