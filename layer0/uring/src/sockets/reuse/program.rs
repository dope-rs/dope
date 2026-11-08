use dope_sys::io::socket::route::{Plan, count::Index, selector::Bootstrap};

const LD_IMM: u16 = 0x00;
const LD_BYTE: u16 = 0x50;
const LD_WORD: u16 = 0x40;
const LD_MEM: u16 = 0x60;
const LD_LEN: u16 = 0x80;
const LDX_IMM: u16 = 0x01;
const LDX_MEM: u16 = 0x61;
const ST: u16 = 0x02;
const STX: u16 = 0x03;
const SUB_X: u16 = 0x1c;
const MUL: u16 = 0x24;
const AND: u16 = 0x54;
const MOD: u16 = 0x94;
const XOR: u16 = 0xa4;
const XOR_X: u16 = 0xac;
const JA: u16 = 0x05;
const JEQ: u16 = 0x15;
const JGT: u16 = 0x25;
const JGE: u16 = 0x35;
const JSET: u16 = 0x45;
const RET: u16 = 0x06;
const RET_A: u16 = 0x16;
const TAX: u16 = 0x07;
const HASH_STEP: usize = 7;
pub(super) const MAX: usize = 9 + 15 + 18 + Bootstrap::MAX as usize * (HASH_STEP + 2) + 2 + 4;
pub(super) const EMPTY: libc::sock_filter = step(LD_IMM, 0);

pub(super) fn build(
    code: &mut [libc::sock_filter; MAX],
    plan: Plan,
    worker: Option<Index>,
) -> usize {
    let base = if worker.is_some() { 8 } else { 0 };
    code[0] = step(LDX_IMM, base);
    let shape = if let Some(prefix) = plan.prefix() {
        let [first, second] = prefix.tags();
        code[1..9].copy_from_slice(&[
            step(LD_BYTE, 0),
            branch(JSET, u32::from(prefix.raw_mask().get()), 2, 9, 3),
            branch(JEQ, u32::from(first), 3, 5, 4),
            branch(JEQ, u32::from(second), 4, 5, 8),
            step(LDX_IMM, base + u32::from(prefix.extent().get())),
            step(LD_BYTE, 0),
            branch(JSET, u32::from(prefix.raw_mask().get()), 7, 9, 8),
            step(RET, 0),
        ]);
        9
    } else {
        1
    };
    let bootstrap = shape + 15;
    let reduce = plan.bootstrap().map_or(bootstrap, |config| {
        bootstrap
            + 18
            + usize::from(config.max().get()) * HASH_STEP
            + usize::from(config.max().get() - config.min().get()) * 2
    });
    let accept = reduce + if plan.bootstrap().is_some() { 2 } else { 0 };
    let drop = accept + if worker.is_some() { 2 } else { 1 };
    let fallback = if plan.bootstrap().is_some() {
        bootstrap
    } else {
        drop
    };
    let selector = plan.selector();
    let count = u32::from(plan.count().encode());
    code[shape..bootstrap].copy_from_slice(&[
        step(LD_LEN, 0),
        step(SUB_X, 0),
        branch(
            JGE,
            u32::from(selector.clear_min().get()),
            shape + 2,
            shape + 3,
            fallback,
        ),
        step(LD_BYTE, u32::from(selector.branch_at())),
        branch(
            JSET,
            u32::from(selector.branch_mask().get()),
            shape + 4,
            shape + 7,
            shape + 5,
        ),
        step(LD_BYTE, u32::from(selector.clear_key())),
        step(JA, link(shape + 6, shape + 13)),
        step(LD_LEN, 0),
        step(SUB_X, 0),
        branch(
            JGE,
            u32::from(selector.set_min().get()),
            shape + 9,
            shape + 10,
            fallback,
        ),
        step(LD_BYTE, u32::from(selector.length_at())),
        branch(
            JEQ,
            u32::from(selector.length()),
            shape + 11,
            shape + 12,
            fallback,
        ),
        step(LD_BYTE, u32::from(selector.set_key())),
        branch(JGE, count, shape + 13, fallback, shape + 14),
        step(JA, link(shape + 14, accept)),
    ]);
    if let Some(config) = plan.bootstrap() {
        let min = usize::from(config.min().get());
        let max = usize::from(config.max().get());
        code[bootstrap..bootstrap + 18].copy_from_slice(&[
            step(LD_BYTE, u32::from(selector.branch_at())),
            branch(
                JSET,
                u32::from(selector.branch_mask().get()),
                bootstrap + 1,
                bootstrap + 2,
                drop,
            ),
            step(LD_WORD, u32::from(config.version_at())),
            branch(JEQ, 0, bootstrap + 3, drop, bootstrap + 4),
            branch(
                JEQ,
                config.version().get(),
                bootstrap + 4,
                bootstrap + 7,
                bootstrap + 5,
            ),
            step(LD_IMM, 0),
            step(JA, link(bootstrap + 6, accept)),
            step(LD_BYTE, u32::from(selector.branch_at())),
            step(AND, u32::from(config.mask().get())),
            branch(
                JEQ,
                u32::from(config.value()),
                bootstrap + 9,
                bootstrap + 10,
                drop,
            ),
            step(LD_BYTE, u32::from(selector.length_at())),
            branch(JGE, min as u32, bootstrap + 11, bootstrap + 12, drop),
            branch(JGT, max as u32, bootstrap + 12, drop, bootstrap + 13),
            step(ST, 0),
            step(XOR, 0x811c9dc5),
            step(MUL, 0x01000193),
            step(ST, 1),
            step(STX, 2),
        ]);
        let mut at = bootstrap + 18;
        for byte in 0..max {
            code[at..at + HASH_STEP].copy_from_slice(&[
                step(LD_BYTE, u32::from(selector.set_key()) + byte as u32),
                step(TAX, 0),
                step(LD_MEM, 1),
                step(XOR_X, 0),
                step(MUL, 0x01000193),
                step(ST, 1),
                step(LDX_MEM, 2),
            ]);
            at += HASH_STEP;
            if byte + 1 >= min && byte + 1 < max {
                code[at] = step(LD_MEM, 0);
                code[at + 1] = branch(JEQ, byte as u32 + 1, at + 1, reduce, at + 2);
                at += 2;
            }
        }
        code[reduce] = step(LD_MEM, 1);
        code[reduce + 1] = if count.is_power_of_two() {
            step(AND, count - 1)
        } else {
            step(MOD, count)
        };
    }
    match worker {
        None => code[accept] = step(RET_A, 0),
        Some(index) => {
            code[accept] = branch(JEQ, index.into_raw() as u32, accept, accept + 1, drop);
            code[accept + 1] = step(RET, u32::MAX);
        }
    }
    code[drop] = step(RET, 0);
    drop + 1
}

const fn step(code: u16, k: u32) -> libc::sock_filter {
    libc::sock_filter {
        code,
        jt: 0,
        jf: 0,
        k,
    }
}

const fn branch(code: u16, k: u32, at: usize, yes: usize, no: usize) -> libc::sock_filter {
    libc::sock_filter {
        code,
        jt: link(at, yes) as u8,
        jf: link(at, no) as u8,
        k,
    }
}

const fn link(at: usize, target: usize) -> u32 {
    (target - at - 1) as u32
}

const _: () = assert!(MAX <= u8::MAX as usize);
