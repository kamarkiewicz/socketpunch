use ahash::RandomState;
use std::sync::atomic::{AtomicU64, Ordering};

/// A lock-free, lossy deduplicator optimized for high-throughput tick streams.
/// It uses a fixed-size array of `AtomicU64` slots. When a message arrives,
/// its hash is computed. If the hash matches the value in the corresponding slot,
/// it's considered a duplicate. Otherwise, the slot is overwritten with the new hash.
pub struct Dedup {
    slots: Box<[AtomicU64]>,
    mask: usize,
    hasher: RandomState,
}

impl Dedup {
    /// `capacity_power_of_two` determines the size of the cache.
    /// E.g. 16 means 2^16 = 65,536 slots.
    pub fn new(capacity_power_of_two: u32) -> Self {
        let size = 1usize << capacity_power_of_two;
        let mut slots = Vec::with_capacity(size);
        for _ in 0..size {
            slots.push(AtomicU64::new(0));
        }
        Self {
            slots: slots.into_boxed_slice(),
            mask: size - 1,
            hasher: RandomState::new(),
        }
    }

    /// Checks if the payload is unique. Returns `true` if it's the first time
    /// this payload's hash has been seen in its assigned slot recently.
    pub fn is_unique(&self, data: &[u8]) -> bool {
        let hash = self.hasher.hash_one(data);
        let hash = if hash == 0 { 1 } else { hash }; // 0 is reserved for empty slot

        let idx = (hash as usize) & self.mask;
        let slot = &self.slots[idx];

        // Fast path: read without dirtying the cache line
        let current = slot.load(Ordering::Relaxed);
        if current == hash {
            return false; // Duplicate
        }

        // It's new (or a hash/slot collision which overwrites an old hash).
        slot.store(hash, Ordering::Relaxed);
        true
    }
}
