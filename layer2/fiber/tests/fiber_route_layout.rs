use std::mem;

use dope_fiber::task::storage::fixed::{
    quota, schedule,
    schedule::{queue, selected, selected::route},
    tree,
};
use dope_test::fibers::Yield;

#[test]
fn exact_route_is_one_word() {
    assert_eq!(mem::size_of::<Route>(), mem::size_of::<usize>());
}

#[test]
fn selected_is_four_words() {
    assert_eq!(mem::size_of::<Selected>(), 4 * mem::size_of::<usize>());
    assert_eq!(mem::size_of::<Owned>(), 4 * mem::size_of::<usize>());
    assert_eq!(mem::size_of::<Entry>(), mem::size_of::<Selected>());
    assert_eq!(mem::size_of::<OwnedEntry>(), mem::size_of::<Owned>());
    assert_eq!(mem::align_of::<Entry>(), mem::align_of::<Selected>());
    assert_eq!(mem::align_of::<OwnedEntry>(), mem::align_of::<Owned>());
    assert_eq!(mem::size_of::<Option<Entry>>(), mem::size_of::<Entry>());
    assert_eq!(
        mem::size_of::<Option<OwnedEntry>>(),
        mem::size_of::<OwnedEntry>(),
    );
}

type Routes = tree::Leaf<schedule::Pool<'static, Yield, 1, ()>>;
type Route = schedule::Route<'static, 'static, Routes, tree::Here>;
type Selected = selected::Item<'static, 'static, 'static, Routes>;
type Entry = route::Entry<'static, 'static, 'static, Routes, tree::Here>;
type Owned =
    selected::Item<'static, 'static, 'static, Routes, quota::Physical, queue::Owned<'static, ()>>;
type OwnedEntry = route::Entry<
    'static,
    'static,
    'static,
    Routes,
    tree::Here,
    quota::Physical,
    queue::Owned<'static, ()>,
>;
