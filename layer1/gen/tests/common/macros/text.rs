use engine::core::{
    BranchError,
    driver::{
        route::{KeyTag, lease},
        settings,
        settings::{claim, errors},
        storage,
    },
    o3::alloc::{self, StrExt},
};

pub struct Text<'a>(pub &'a str);

impl storage::Plan for Text<'_> {
    type Output<'p, 'd>
        = (&'p str, lease::Claim<'d, KeyTag<11>>)
    where
        Self: 'd,
        'd: 'p;
}

impl storage::Build for Text<'_> {
    type Error = BranchError<alloc::Exhausted, lease::Error>;

    type Store<'d>
        = String
    where
        Self: 'd;

    fn try_store<'d>(&self) -> Result<Self::Store<'d>, Self::Error>
    where
        Self: 'd,
    {
        self.0.try_to_owned().map_err(BranchError::First)
    }

    fn try_build<'p, 'd>(
        self,
        store: &'p mut Self::Store<'d>,
        context: storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'p, 'd>, Self::Error>
    where
        Self: 'd,
        'd: 'p,
    {
        let claim = context
            .try_reserve::<KeyTag<11>>()
            .map_err(BranchError::Second)?;
        Ok((store.as_str(), claim))
    }
}

impl settings::Resource for Text<'_> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(claim::Claim::new(
            settings::Routes::singleton::<KeyTag<11>>(),
        ))
    }
}
