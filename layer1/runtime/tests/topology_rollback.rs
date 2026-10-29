use std::convert::Infallible;

use dope_core::{BranchError, driver::storage};
use dope_runtime::topology::recipe;

#[test]
fn branches_share_identity() {
    type RecipeError =
        <(recipe::Borrow, recipe::Borrow) as recipe::Build<storage::Value<()>>>::Error;

    let value: Option<RecipeError> = None;
    let branch: Option<BranchError<Infallible, Infallible>> = value;
    assert!(branch.is_none());
}
