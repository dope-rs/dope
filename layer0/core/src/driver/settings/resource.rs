use crate::driver::settings::{claim, errors};

/// Startup-only typed composition of driver-resource claims.
pub trait Resource {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim>;
}

impl Resource for () {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        Ok(needs)
    }
}

impl<R: Resource> Resource for Option<R> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        match self {
            Some(tree) => tree.include_in(needs),
            None => Ok(needs),
        }
    }
}

impl<R: Resource + ?Sized> Resource for &R {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        (*self).include_in(needs)
    }
}

impl<A: Resource> Resource for (A,) {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        self.0.include_in(needs)
    }
}

impl<A: Resource, B: Resource> Resource for (A, B) {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        let needs = self.0.include_in(needs)?;
        self.1.include_in(needs)
    }
}

impl<A: Resource, B: Resource, C: Resource> Resource for (A, B, C) {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        let needs = self.0.include_in(needs)?;
        let needs = self.1.include_in(needs)?;
        self.2.include_in(needs)
    }
}

impl<A: Resource, B: Resource, C: Resource, D: Resource> Resource for (A, B, C, D) {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        let needs = self.0.include_in(needs)?;
        let needs = self.1.include_in(needs)?;
        let needs = self.2.include_in(needs)?;
        self.3.include_in(needs)
    }
}

impl<A: Resource, B: Resource, C: Resource, D: Resource, E: Resource> Resource for (A, B, C, D, E) {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        let needs = self.0.include_in(needs)?;
        let needs = self.1.include_in(needs)?;
        let needs = self.2.include_in(needs)?;
        let needs = self.3.include_in(needs)?;
        self.4.include_in(needs)
    }
}

impl<A: Resource, B: Resource, C: Resource, D: Resource, E: Resource, F: Resource> Resource
    for (A, B, C, D, E, F)
{
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        let needs = self.0.include_in(needs)?;
        let needs = self.1.include_in(needs)?;
        let needs = self.2.include_in(needs)?;
        let needs = self.3.include_in(needs)?;
        let needs = self.4.include_in(needs)?;
        self.5.include_in(needs)
    }
}

impl<A: Resource, B: Resource, C: Resource, D: Resource, E: Resource, F: Resource, G: Resource>
    Resource for (A, B, C, D, E, F, G)
{
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        let needs = self.0.include_in(needs)?;
        let needs = self.1.include_in(needs)?;
        let needs = self.2.include_in(needs)?;
        let needs = self.3.include_in(needs)?;
        let needs = self.4.include_in(needs)?;
        let needs = self.5.include_in(needs)?;
        self.6.include_in(needs)
    }
}

impl<
    A: Resource,
    B: Resource,
    C: Resource,
    D: Resource,
    E: Resource,
    F: Resource,
    G: Resource,
    H: Resource,
> Resource for (A, B, C, D, E, F, G, H)
{
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        let needs = self.0.include_in(needs)?;
        let needs = self.1.include_in(needs)?;
        let needs = self.2.include_in(needs)?;
        let needs = self.3.include_in(needs)?;
        let needs = self.4.include_in(needs)?;
        let needs = self.5.include_in(needs)?;
        let needs = self.6.include_in(needs)?;
        self.7.include_in(needs)
    }
}
