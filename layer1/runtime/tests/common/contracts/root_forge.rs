use dope_runtime::executor::root;

fn main() {
    root::Context::<()>::new(loop {}, loop {}, loop {}, loop {});
}
