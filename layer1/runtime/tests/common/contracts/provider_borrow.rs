use dope_runtime::client::{Context, Provider};
struct Borrowing;
impl<'d> Provider<'d> for Borrowing {
    type Client<'app>
        = &'app Borrowing
    where
        Self: 'app,
        'd: 'app;
    fn provide<'app>(context: Context<'_, 'app, 'd, Self>) -> Self::Client<'app>
    where
        Self: 'app,
        'd: 'app,
    {
        context.into_parts().0.get_ref()
    }
}

fn main() {}
