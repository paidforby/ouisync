use crate::block::BLOCK_SIZE;

// State shared among multiple instances of the same file.
#[derive(Debug)]
pub(crate) struct Core {
    pub len: u64,
    pub len_dirty: bool,
}

impl Core {
    // Total number of blocks in this blob including the possibly partially filled final block.
    pub fn block_count(&self) -> u32 {
        // https://stackoverflow.com/questions/2745074/fast-ceiling-of-an-integer-division-in-c-c
        (1 + (self.len + super::HEADER_SIZE as u64 - 1) / BLOCK_SIZE as u64)
            .try_into()
            .unwrap_or(u32::MAX)
    }
}
