use std::mem;

use dope_sys::io::socket::route::{Plan, count::Count, selector};

use crate::sockets::reuse::{Error, Loaded, Map};

const LDX_DW_MEM: u8 = 0x79;
const LDX_B_MEM: u8 = 0x71;
const STX_W_MEM: u8 = 0x63;
const ALU64_ADD_K: u8 = 0x07;
const ALU64_MOV_X: u8 = 0xbf;
const ALU64_MOV_K: u8 = 0xb7;
const ALU32_MUL_K: u8 = 0x24;
const ALU32_OR_X: u8 = 0x4c;
const ALU32_AND_K: u8 = 0x54;
const ALU32_LSH_K: u8 = 0x64;
const ALU32_MOD_K: u8 = 0x94;
const ALU32_XOR_X: u8 = 0xac;
const ALU32_XOR_K: u8 = 0xa4;
const JMP_JGT_X: u8 = 0x2d;
const JMP_JGT_K: u8 = 0x25;
const JMP_JGE_K: u8 = 0x35;
const JMP_JLT_K: u8 = 0xa5;
const JMP_JEQ_K: u8 = 0x15;
const JMP32_JEQ_K: u8 = 0x16;
const JMP_JNE_K: u8 = 0x55;
const JMP_JSET_K: u8 = 0x45;
const JMP_JA: u8 = 0x05;
const JMP_CALL: u8 = 0x85;
const JMP_EXIT: u8 = 0x95;
const SELECT_HELPER: i32 = 82;
const UDP_HEADER_LEN: i32 = 8;
const SK_DROP: i32 = 0;
const SK_PASS: i32 = 1;

/// The zero instruction one `LD_IMM64` pair reserves for its high half.
const EMPTY: Instruction = instruction(0, 0, 0, 0, 0);

/// Slot of the `Set` label: the first instruction of the set-shape branch.
const SET: usize = 9;
/// Slot of the `Select` label: the dense-map selection both shape branches reach.
const SELECT: usize = 15;
/// Slot of the `Drop` label: the shared `SK_DROP` tail every guard jumps to.
const DROP: usize = 26;
/// Slot of the map handle the kernel program load replaces with the live route map.
const MAP_HANDLE: usize = 17;
/// Additional instructions before the shared selector body in a prefixed plan.
const PREFIX_LEN: usize = 14;
const PREFIX_BODY: usize = PREFIX_LEN + 2;
const LEN: usize = 28;

const HASH: usize = 62;
const HASH_STEP: usize = 7;
const FNV_BASIS: i32 = 0x811c9dc5_u32 as i32;
const FNV_PRIME: i32 = 0x01000193;
const BOOTSTRAP_LEN: usize = HASH + selector::Bootstrap::MAX as usize * HASH_STEP + 1;

/// The displacement of the instruction at slot `at` to the label at slot `target`.
const fn link(at: usize, target: usize) -> i16 {
    target as i16 - at as i16 - 1
}

#[repr(C)]
#[derive(Clone, Copy)]
pub(super) struct Instruction {
    code: u8,
    regs: u8,
    off: i16,
    imm: i32,
}

pub(super) fn load(map: &Map, plan: Plan) -> Result<Loaded, Error> {
    let body = shape(plan.selector(), plan.count(), map);
    match (plan.bootstrap(), plan.prefix()) {
        (None, None) => Loaded::load(&body),
        (None, Some(prefix)) => Loaded::load(&prefixed::<LEN, { LEN + PREFIX_LEN }>(prefix, body)),
        (Some(config), prefix) => {
            let (body, used) = bootstrap(plan.selector(), config, plan.count(), body);
            match prefix {
                None => Loaded::load(&body[..used]),
                Some(prefix) => Loaded::load(
                    &prefixed::<BOOTSTRAP_LEN, { BOOTSTRAP_LEN + PREFIX_LEN }>(prefix, body)
                        [..used + PREFIX_LEN],
                ),
            }
        }
    }
}

/// Selects the payload base before entering the unchanged selector body.
/// All of that body's relative branches retain their displacement when moved together.
fn prefixed<const BODY: usize, const LENGTH: usize>(
    prefix: selector::PrefixPlan,
    body: [Instruction; BODY],
) -> [Instruction; LENGTH] {
    const { assert!(BODY >= LEN && LENGTH == BODY + PREFIX_LEN) };
    let mut program = [EMPTY; LENGTH];
    let drop = PREFIX_LEN + DROP;
    program[..2].copy_from_slice(&body[..2]);
    program[PREFIX_BODY..].copy_from_slice(&body[2..]);
    let [first, second] = prefix.tags();
    let extent = i32::from(prefix.extent().get());
    let raw_mask = i32::from(prefix.raw_mask().get());
    program[2] = instruction(ALU64_MOV_X, 4, 2, 0, 0);
    program[3] = instruction(ALU64_ADD_K, 4, 0, 0, UDP_HEADER_LEN + 1);
    program[4] = instruction(JMP_JGT_X, 4, 3, link(4, drop), 0);
    program[5] = instruction(LDX_B_MEM, 7, 2, UDP_HEADER_LEN as i16, 0);
    program[6] = instruction(JMP_JSET_K, 7, 0, link(6, PREFIX_BODY), raw_mask);
    program[7] = instruction(JMP_JEQ_K, 7, 0, link(7, 9), i32::from(first));
    program[8] = instruction(JMP_JNE_K, 7, 0, link(8, drop), i32::from(second));
    program[9] = instruction(ALU64_MOV_X, 4, 2, 0, 0);
    program[10] = instruction(ALU64_ADD_K, 4, 0, 0, UDP_HEADER_LEN + extent + 1);
    program[11] = instruction(JMP_JGT_X, 4, 3, link(11, drop), 0);
    program[12] = instruction(ALU64_ADD_K, 2, 0, 0, extent);
    program[13] = instruction(LDX_B_MEM, 7, 2, UDP_HEADER_LEN as i16, 0);
    program[14] = instruction(JMP_JSET_K, 7, 0, link(14, PREFIX_BODY), raw_mask);
    program[15] = instruction(JMP_JA, 0, 0, link(15, drop), 0);
    program
}

/// The exact 28-slot reuseport selection program one `plan` compiles to.
///
/// Slot indices are this program's only control flow: every guard jumps forward to the
/// shared [`DROP`] tail, and both shape branches meet the selection at [`SET`]/[`SELECT`].
fn shape(selector: selector::Selector, count: Count, map: &Map) -> [Instruction; LEN] {
    let mut program = [EMPTY; LEN];
    program[0] = instruction(LDX_DW_MEM, 2, 1, 0, 0);
    program[1] = instruction(LDX_DW_MEM, 3, 1, 8, 0);
    program[2] = instruction(ALU64_MOV_X, 4, 2, 0, 0);
    program[3] = instruction(
        ALU64_ADD_K,
        4,
        0,
        0,
        UDP_HEADER_LEN + i32::from(selector.clear_min().get()),
    );
    program[4] = instruction(JMP_JGT_X, 4, 3, link(4, DROP), 0);
    program[5] = instruction(LDX_B_MEM, 7, 2, payload_offset(selector.branch_at()), 0);
    program[6] = instruction(
        JMP_JSET_K,
        7,
        0,
        link(6, SET),
        i32::from(selector.branch_mask().get()),
    );
    program[7] = instruction(LDX_B_MEM, 7, 2, payload_offset(selector.clear_key()), 0);
    program[8] = instruction(JMP_JA, 0, 0, link(8, SELECT), 0);
    program[9] = instruction(ALU64_MOV_X, 4, 2, 0, 0);
    program[10] = instruction(
        ALU64_ADD_K,
        4,
        0,
        0,
        UDP_HEADER_LEN + i32::from(selector.set_min().get()),
    );
    program[11] = instruction(JMP_JGT_X, 4, 3, link(11, DROP), 0);
    program[12] = instruction(LDX_B_MEM, 7, 2, payload_offset(selector.length_at()), 0);
    program[13] = instruction(
        JMP_JNE_K,
        7,
        0,
        link(13, DROP),
        i32::from(selector.length()),
    );
    program[14] = instruction(LDX_B_MEM, 7, 2, payload_offset(selector.set_key()), 0);
    program[15] = instruction(JMP_JGE_K, 7, 0, link(15, DROP), i32::from(count.encode()));
    program[16] = instruction(STX_W_MEM, 10, 7, -4, 0);
    program[MAP_HANDLE] = map.encode();
    program[19] = instruction(ALU64_MOV_X, 3, 10, 0, 0);
    program[20] = instruction(ALU64_ADD_K, 3, 0, 0, -4);
    program[21] = instruction(ALU64_MOV_K, 4, 0, 0, 0);
    program[22] = instruction(JMP_CALL, 0, 0, 0, SELECT_HELPER);
    program[23] = instruction(JMP_JNE_K, 0, 0, link(23, DROP), 0);
    program[24] = instruction(ALU64_MOV_K, 0, 0, 0, SK_PASS);
    program[25] = instruction(JMP_EXIT, 0, 0, 0, 0);
    program[DROP] = instruction(ALU64_MOV_K, 0, 0, 0, SK_DROP);
    program[27] = instruction(JMP_EXIT, 0, 0, 0, 0);
    program
}

fn bootstrap(
    selector: selector::Selector,
    config: selector::Bootstrap,
    count: Count,
    body: [Instruction; LEN],
) -> ([Instruction; BOOTSTRAP_LEN], usize) {
    let mut program = [EMPTY; BOOTSTRAP_LEN];
    program[..LEN].copy_from_slice(&body);
    // Only rejection edges enter bootstrap; the original selection tail always exits.
    for at in [4, 11, 13, 15] {
        program[at].off = link(at, LEN);
    }
    program[28] = instruction(ALU64_MOV_X, 4, 2, 0, 0);
    program[29] = instruction(
        ALU64_ADD_K,
        4,
        0,
        0,
        i32::from(payload_offset(selector.branch_at())) + 1,
    );
    program[30] = instruction(JMP_JGT_X, 4, 3, link(30, DROP), 0);
    program[31] = instruction(LDX_B_MEM, 8, 2, payload_offset(selector.branch_at()), 0);
    program[32] = instruction(
        JMP_JSET_K,
        8,
        0,
        link(32, 34),
        i32::from(selector.branch_mask().get()),
    );
    program[33] = instruction(JMP_JA, 0, 0, link(33, DROP), 0);

    program[34] = instruction(ALU64_MOV_X, 4, 2, 0, 0);
    program[35] = instruction(
        ALU64_ADD_K,
        4,
        0,
        0,
        i32::from(payload_offset(config.version_at())) + 4,
    );
    program[36] = instruction(JMP_JGT_X, 4, 3, link(36, DROP), 0);
    program[37] = instruction(LDX_B_MEM, 5, 2, payload_offset(config.version_at()), 0);
    for byte in 1..4 {
        let at = 38 + (byte - 1) * 3;
        program[at] = instruction(ALU32_LSH_K, 5, 0, 0, 8);
        program[at + 1] = instruction(
            LDX_B_MEM,
            6,
            2,
            payload_offset(config.version_at()) + byte as i16,
            0,
        );
        program[at + 2] = instruction(ALU32_OR_X, 5, 6, 0, 0);
    }
    program[47] = instruction(JMP_JEQ_K, 5, 0, link(47, DROP), 0);
    program[48] = instruction(
        JMP32_JEQ_K,
        5,
        0,
        link(48, 51),
        config.version().get() as i32,
    );
    program[49] = instruction(ALU64_MOV_K, 7, 0, 0, 0);
    program[50] = instruction(JMP_JA, 0, 0, link(50, SELECT + 1), 0);

    program[51] = instruction(ALU32_AND_K, 8, 0, 0, i32::from(config.mask().get()));
    program[52] = instruction(JMP_JNE_K, 8, 0, link(52, DROP), i32::from(config.value()));
    program[53] = instruction(ALU64_MOV_X, 4, 2, 0, 0);
    program[54] = instruction(
        ALU64_ADD_K,
        4,
        0,
        0,
        i32::from(payload_offset(selector.length_at())) + 1,
    );
    program[55] = instruction(JMP_JGT_X, 4, 3, link(55, DROP), 0);
    program[56] = instruction(LDX_B_MEM, 6, 2, payload_offset(selector.length_at()), 0);
    program[57] = instruction(
        JMP_JLT_K,
        6,
        0,
        link(57, DROP),
        i32::from(config.min().get()),
    );
    program[58] = instruction(
        JMP_JGT_K,
        6,
        0,
        link(58, DROP),
        i32::from(config.max().get()),
    );
    program[59] = instruction(ALU64_MOV_X, 7, 6, 0, 0);
    program[60] = instruction(ALU32_XOR_K, 7, 0, 0, FNV_BASIS);
    program[61] = instruction(ALU32_MUL_K, 7, 0, 0, FNV_PRIME);
    let min = usize::from(config.min().get());
    let max = usize::from(config.max().get());
    let reduce = HASH + 3 + 3 * min + HASH_STEP * (max - min);
    program[HASH] = instruction(ALU64_MOV_X, 4, 2, 0, 0);
    program[HASH + 1] = instruction(
        ALU64_ADD_K,
        4,
        0,
        0,
        i32::from(payload_offset(selector.set_key())) + min as i32,
    );
    program[HASH + 2] = instruction(JMP_JGT_X, 4, 3, link(HASH + 2, DROP), 0);
    let mut at = HASH + 3;
    for byte in 0..max {
        let offset = payload_offset(selector.set_key()) + byte as i16;
        if byte >= min {
            program[at] = instruction(ALU64_MOV_X, 4, 2, 0, 0);
            program[at + 1] = instruction(ALU64_ADD_K, 4, 0, 0, i32::from(offset) + 1);
            program[at + 2] = instruction(JMP_JGT_X, 4, 3, link(at + 2, DROP), 0);
            at += 3;
        }
        program[at] = instruction(LDX_B_MEM, 8, 2, offset, 0);
        program[at + 1] = instruction(ALU32_XOR_X, 7, 8, 0, 0);
        program[at + 2] = instruction(ALU32_MUL_K, 7, 0, 0, FNV_PRIME);
        at += 3;
        if byte + 1 >= min && byte + 1 < max {
            program[at] = instruction(JMP_JEQ_K, 6, 0, link(at, reduce), byte as i32 + 1);
            at += 1;
        }
    }
    let count = count.encode();
    program[reduce] = if count.is_power_of_two() {
        instruction(ALU32_AND_K, 7, 0, 0, i32::from(count - 1))
    } else {
        instruction(ALU32_MOD_K, 7, 0, 0, i32::from(count))
    };
    program[reduce + 1] = instruction(JMP_JA, 0, 0, link(reduce + 1, SELECT + 1), 0);
    (program, reduce + 2)
}

/// Encodes Linux BPF's native-endian register bitfields by folding the high
/// byte's low nibble into the low byte's high nibble.
pub(super) const fn instruction(code: u8, dst: u8, src: u8, off: i16, imm: i32) -> Instruction {
    let regs = u16::from_ne_bytes([dst, src]);
    Instruction {
        code,
        regs: (regs as u8) | ((regs >> 4) as u8 & 0xf0),
        off,
        imm,
    }
}

const fn payload_offset(offset: u8) -> i16 {
    offset as i16 + UDP_HEADER_LEN as i16
}

const _: () = {
    assert!(SET < SELECT && SELECT < DROP);
    assert!(DROP + 2 == LEN);
    assert!(mem::size_of::<Instruction>() == 8);
    assert!(PREFIX_BODY + LEN - 2 == LEN + PREFIX_LEN);
};
