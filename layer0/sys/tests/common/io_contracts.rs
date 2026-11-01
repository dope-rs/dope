pub const CASES: [(&str, Option<&str>); 10] = [
    (
        r#"fn sequential<S: dope_sys::io::file::raw::Directory>(entries: &mut dope_sys::io::file::Entries<'_, S>) {
    let first = entries.try_next::<dope_sys::Errno>()
        .expect("entry read").expect("entry");
    assert!(!first.name.to_bytes().is_empty());
    let next = entries.try_next::<dope_sys::Errno>();
    assert!(next.is_ok());
}
"#,
        None,
    ),
    (
        r#"fn overlap<S: dope_sys::io::file::raw::Directory>(entries: &mut dope_sys::io::file::Entries<'_, S>) {
    let first = entries.try_next::<dope_sys::Errno>()
        .expect("entry read").expect("entry");
    let next = entries.try_next::<dope_sys::Errno>();
    assert!(!first.name.to_bytes().is_empty());
    assert!(next.is_ok());
}
"#,
        Some("error[E0499]:"),
    ),
    (
        r#"use dope_sys::io::socket::option::tuning::device::Admission;

fn fabricate(backend: &mut (), file: u8) -> Admission<'_, (), u8, u32, u32> {
    Admission { backend, file, slot: 0, index: 0 }
}
"#,
        Some("error[E0451]:"),
    ),
    (
        r#"use dope_sys::io::socket::option::tuning::device::Admission;

fn once(admission: Admission<'_, (), u8, u32, u32>) {
    let _ = admission.into_parts();
}
"#,
        None,
    ),
    (
        r#"use dope_sys::io::socket::option::tuning::device::Admission;

fn twice(admission: Admission<'_, (), u8, u32, u32>) {
    let first = admission.into_parts();
    let second = admission.into_parts();
}
"#,
        Some("error[E0382]:"),
    ),
    (
        r#"use dope_sys::{engine, memory::buffer::{self, provided}, ring::ready};

fn producer<P: provided::Producer>() {
    let _ = |owner: &mut P, _token: P::Pending| owner.flush();
}

fn ready<R: ready::Complete>(source: &mut R, other: &mut R) {
    if let Some((_, Some(Ok(token)))) = source.pop() {
        let _: R::Pending = token;
        other.flush();
    }
}

fn engine<E: engine::Complete>() {
    let _ = |owner: &mut E, _token: E::Pending| owner.next();
}

fn release<R: buffer::raw::Release<T>, T>() {
    let _ = |owner: &mut R, token: T| unsafe { owner.release(token) };
}
"#,
        None,
    ),
    (
        r#"use dope_sys::memory::buffer::provided;

fn defer<P: provided::Producer>() {
    let _ = |owner: &mut P, token: P::Pending| owner.defer(token);
}
"#,
        Some("error[E0599]:"),
    ),
    (
        r#"use dope_sys::ring::ready;

fn defer<R: ready::Complete>(source: &mut R, other: &mut R) {
    if let Some((_, Some(Ok(token)))) = source.pop() {
        other.defer(token);
    }
}
"#,
        Some("error[E0599]:"),
    ),
    (
        r#"use dope_sys::engine;

fn release<E: engine::Complete>() {
    let _ = |owner: &mut E, token: E::Pending| owner.release(token);
}
"#,
        Some("error[E0599]:"),
    ),
    (
        r#"use dope_sys::memory::buffer;

fn release<R: buffer::raw::Release<T>, T>() {
    let _ = |owner: &mut R, token: T| owner.release(token);
}
"#,
        Some("error[E0133]:"),
    ),
];
