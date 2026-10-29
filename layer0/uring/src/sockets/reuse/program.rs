use std::{marker, mem};

use dope_sys::io::socket::route::{Plan, count::Count, selector};

use crate::sockets::reuse::Map;

const LDX_DW_MEM: u8 = 0x79;
const LDX_B_MEM: u8 = 0x71;
const STX_W_MEM: u8 = 0x63;
const ALU64_ADD_K: u8 = 0x07;
const ALU64_MOV_X: u8 = 0xbf;
const ALU64_MOV_K: u8 = 0xb7;
const JMP_JGT_X: u8 = 0x2d;
const JMP_JGE_K: u8 = 0x35;
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

pub(super) struct Program<'a> {
    instructions: [Instruction; Program::LEN],
    map: marker::PhantomData<&'a Map>,
}

impl<'a> Program<'a> {
    const LEN: usize = 28;

    pub(super) fn compile(map: &'a Map, plan: Plan) -> Self {
        Self {
            instructions: shape(plan.selector(), plan.count(), map),
            map: marker::PhantomData,
        }
    }

    pub(super) const fn instructions(&self) -> &[Instruction] {
        &self.instructions
    }
}

/// The exact 28-slot reuseport selection program one `plan` compiles to.
///
/// Slot indices are this program's only control flow: every guard jumps forward to the
/// shared [`DROP`] tail, and both shape branches meet the selection at [`SET`]/[`SELECT`].
fn shape(selector: selector::Selector, count: Count, map: &Map) -> [Instruction; Program::LEN] {
    let mut program = [EMPTY; Program::LEN];
    program[0] = instruction(LDX_DW_MEM, 2, 1, 0, 0);
    program[1] = instruction(LDX_DW_MEM, 3, 1, 8, 0);
    program[2] = instruction(ALU64_MOV_X, 4, 2, 0, 0);
    program[3] = instruction(
        ALU64_ADD_K,
        4,
        0,
        0,
        UDP_HEADER_LEN + i32::from(selector.clear_min().get().get()),
    );
    program[4] = instruction(JMP_JGT_X, 4, 3, link(4, DROP), 0);
    program[5] = instruction(LDX_B_MEM, 7, 2, payload_offset(selector.branch_at()), 0);
    program[6] = instruction(
        JMP_JSET_K,
        7,
        0,
        link(6, SET),
        i32::from(selector.branch_mask().get().get()),
    );
    program[7] = instruction(LDX_B_MEM, 7, 2, payload_offset(selector.clear_key()), 0);
    program[8] = instruction(JMP_JA, 0, 0, link(8, SELECT), 0);
    program[9] = instruction(ALU64_MOV_X, 4, 2, 0, 0);
    program[10] = instruction(
        ALU64_ADD_K,
        4,
        0,
        0,
        UDP_HEADER_LEN + i32::from(selector.set_min().get().get()),
    );
    program[11] = instruction(JMP_JGT_X, 4, 3, link(11, DROP), 0);
    program[12] = instruction(LDX_B_MEM, 7, 2, payload_offset(selector.length_at()), 0);
    program[13] = instruction(
        JMP_JNE_K,
        7,
        0,
        link(13, DROP),
        i32::from(selector.length().get()),
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

const fn payload_offset(offset: selector::Offset) -> i16 {
    offset.get() as i16 + UDP_HEADER_LEN as i16
}

const _: () = {
    assert!(SET < SELECT && SELECT < DROP);
    assert!(DROP + 2 == Program::LEN);
    assert!(mem::size_of::<Instruction>() == 8);
    assert!(mem::size_of::<Program<'static>>() == 28 * mem::size_of::<Instruction>());
    assert!(mem::align_of::<Program<'static>>() == mem::align_of::<Instruction>());
};
