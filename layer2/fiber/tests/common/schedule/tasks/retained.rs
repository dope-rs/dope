use dope::core::{driver, driver::retained};

pub trait Retain<'a, 'd> {
    fn retained(self) -> retained::Context<'a, 'd, 'd>;
}

impl<'a, 'd> Retain<'a, 'd> for driver::Context<'a, 'd> {
    fn retained(self) -> retained::Context<'a, 'd, 'd> {
        let owner = retained::raw::Owner::from_driver(self.driver_ref());
        retained::Context::new(self, owner)
    }
}
