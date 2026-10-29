use dope_runtime::process;

fn escape<'run, 'owner: 'run>() {
    let _ = |ctx: &process::Context<'run>| -> process::Terminal<'owner> { ctx.terminal() };
}

fn main() {}
