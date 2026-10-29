fn main() {
    let _ = dope_gen::fiber!('_, crate = ::fibers => async move {
        std::future::ready(()).await;
    });
}
