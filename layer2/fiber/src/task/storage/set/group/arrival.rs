/// Effect of releasing one completed task into its group.
pub enum Arrival {
    First,
    More,
}

const _: () = {
    assert!(core::mem::size_of::<Arrival>() == 1);
    assert!(core::mem::size_of::<Option<Arrival>>() == 1);
};
