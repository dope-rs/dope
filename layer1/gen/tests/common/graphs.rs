#[derive(engine::WorkGraph)]
#[work(core = ::engine::core, runtime = ::engine::runtime)]
pub enum r#Work {
    Application,
    Output,
    Transport,
}
