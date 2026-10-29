use engine::runtime::control;

#[pin_project::pin_project]
#[derive(engine::Application)]
#[dispatcher(crate = ::engine)]
#[coordinate]
pub struct Relative {
    #[dispatcher(state)]
    pub outer: super::SelfConst,
}

impl Relative {
    fn coordinate(_: relative_coord::Fields<'_, '_>) -> control::Flow {
        control::Flow::Idle
    }
}
