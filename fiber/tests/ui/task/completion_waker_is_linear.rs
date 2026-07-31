use dope::driver::ready::CompletionWaker;

fn consume(_: CompletionWaker<'_>) {}

fn duplicate(wake: CompletionWaker<'_>) {
    consume(wake);
    consume(wake);
}

fn main() {}
