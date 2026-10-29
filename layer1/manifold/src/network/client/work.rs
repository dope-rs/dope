use dope_core::driver::route::indices;

#[derive(dope_gen::WorkGraph)]
#[work(core = ::dope_core, runtime = ::dope_runtime)]
pub(super) enum Work {
    Discovery,
    Retirement,
}

pub(super) enum Task {
    Discovery,
    Retirement(indices::SlotIndex),
}
