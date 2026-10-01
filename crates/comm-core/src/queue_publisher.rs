// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Jun-young Han <jyhan@katech.re.kr>
// KATECH SDV Platform Research Center

use crate::error::{Error, Result};
use crate::queue_shm::{monotonic_ns, QueueShm, MAX_DATA_SHMS};
use crate::WriteMode;

use core::marker::PhantomData;
use core::sync::atomic::Ordering;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub struct QueuePublisher<T: Copy> {
    queue_shm: QueueShm,
    current_write: Option<u32>,
    pending_pool: Vec<u32>,
    reuse_pool: Vec<u32>,
    mono_epoch: Instant,
    next_sequence: u64,
    _notify_placeholder: (),
    _marker: PhantomData<T>,
}

impl<T: Copy> QueuePublisher<T> {
    pub fn new(topic: &str, history: u32, margin: u32, threshold: u32, deadline_us: u32) -> Result<Self> {
        let data_size = core::mem::size_of::<T>() as u32;
        if data_size == 0 {
            return Err(Error::InvalidConfig);
        }

        let queue_shm = QueueShm::create(topic, history, margin, data_size, threshold, deadline_us)?;

        let mono_epoch = Instant::now();
        let pid = std::process::id();
        queue_shm
            .header()
            .publisher_pid
            .store(pid, Ordering::Release);
        queue_shm.header().heartbeat_ns.store(
            mono_epoch.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Release,
        );

        queue_shm.header().stats_start_ns.store(monotonic_ns(), Ordering::Release);

        let num = queue_shm.data_count();
        let mut reuse_pool = Vec::with_capacity(num as usize);
        for i in 0..num {
            reuse_pool.push(i);
        }

        Ok(Self {
            queue_shm,
            current_write: None,
            pending_pool: Vec::new(),
            reuse_pool,
            mono_epoch,
            next_sequence: 0,
            _notify_placeholder: (),
            _marker: PhantomData,
        })
    }

    pub fn attach(topic: &str) -> Result<Self> {
        let data_size = core::mem::size_of::<T>() as u32;
        if data_size == 0 {
            return Err(Error::InvalidConfig);
        }

        let queue_shm = QueueShm::attach(topic)?;

        if queue_shm.data_size() < data_size {
            return Err(Error::InvalidConfig);
        }

        let mono_epoch = Instant::now();

        let pid = std::process::id();
        queue_shm
            .header()
            .publisher_pid
            .store(pid, Ordering::Release);
        queue_shm.header().heartbeat_ns.store(
            mono_epoch.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Release,
        );

        let hdr = queue_shm.header();
        let num = hdr.num.load(Ordering::Acquire);
        let qlen = hdr.queue_len.load(Ordering::Acquire);
        let head = hdr.queue_head.load(Ordering::Acquire);
        if head >= MAX_DATA_SHMS as u32 {
            return Err(Error::QueueValidationFailed);
        }

        let mut in_queue = std::collections::HashSet::new();
        for i in 0..qlen {
            let pos = (head + i) % MAX_DATA_SHMS as u32;
            let idx = hdr.queue[pos as usize].load(Ordering::Acquire);
            in_queue.insert(idx);
        }

        let mut pending_pool = Vec::new();
        let mut reuse_pool = Vec::new();
        for i in 0..num {
            if in_queue.contains(&i) {
                continue;
            }
            if hdr.pending_flags[i as usize].load(Ordering::Acquire) != 0 {
                pending_pool.push(i);
            } else if let Ok(dh) = queue_shm.slot_header(i) {
                if dh.rx_count.load(Ordering::Acquire) > 0
                    || hdr.any_reader_active(i)
                {
                    hdr.pending_flags[i as usize].store(1, Ordering::Release);
                    pending_pool.push(i);
                } else {
                    reuse_pool.push(i);
                }
            } else {
                reuse_pool.push(i);
            }
        }

        for i in 0..num {
            if let Ok(dh) = queue_shm.slot_header(i) {
                let gen = dh.generation.load(Ordering::Acquire);
                if !gen.is_multiple_of(2) {
                    dh.generation
                        .store((gen.wrapping_add(1)) & !1u32, Ordering::Release);
                }
            }
        }

        let mut max_seq: u64 = 0;
        for i in 0..num {
            if let Ok(dh) = queue_shm.slot_header(i) {
                let seq = dh.sequence.load(Ordering::Acquire);
                if seq < u64::MAX && seq >= max_seq {
                    max_seq = seq + 1;
                }
            }
        }
        let next_sequence = max_seq.max(hdr.publish_count.load(Ordering::Acquire));

        Ok(Self {
            queue_shm,
            current_write: None,
            pending_pool,
            reuse_pool,
            mono_epoch,
            next_sequence,
            _notify_placeholder: (),
            _marker: PhantomData,
        })
    }

    pub fn borrow(&mut self, mode: WriteMode) -> Result<&mut T> {
        if self.current_write.is_some() {
            return Err(Error::AlreadyBorrowed);
        }

        if self.reuse_pool.is_empty() {
            self.scan_pending()?;
        }
        if self.reuse_pool.is_empty() {
            let qlen = self.queue_shm.header().queue_len.load(Ordering::Acquire);
            if qlen > 0 {
                self.handle_overflow()?;
            }
        }
        let data_idx = match self.reuse_pool.pop() {
            Some(idx) => idx,
            None => {
                self.queue_shm.header().pub_starvation_count.fetch_add(1, Ordering::Relaxed);
                return Err(Error::QueueFull);
            }
        };

        if mode == WriteMode::Fresh {
            let dst = self.queue_shm.slot_data_ptr(data_idx)?;
            unsafe {
                core::ptr::write_bytes(dst, 0, core::mem::size_of::<T>());
            }
        }

        if mode == WriteMode::Update {
            let hdr = self.queue_shm.header();
            let qlen = hdr.queue_len.load(Ordering::Acquire);
            if qlen > 0 {
                let pos = hdr.pos_write.load(Ordering::Acquire);
                let prev_pos = if pos == 0 {
                    MAX_DATA_SHMS as u32 - 1
                } else {
                    pos - 1
                };
                let latest_idx = hdr.queue[prev_pos as usize].load(Ordering::Acquire);
                if latest_idx < hdr.num.load(Ordering::Acquire) {
                    let src = self.queue_shm.slot_data_ptr(latest_idx)?;
                    let dst = self.queue_shm.slot_data_ptr(data_idx)?;
                    unsafe {
                        core::ptr::copy_nonoverlapping(src, dst, core::mem::size_of::<T>());
                    }
                }
            }
        }

        self.current_write = Some(data_idx);
        let ptr = self.queue_shm.slot_data_ptr(data_idx)? as *mut T;
        Ok(unsafe { &mut *ptr })
    }

    pub fn publish(&mut self) -> Result<()> {
        let data_idx = self.current_write.take().ok_or(Error::NoBorrow)?;

        if let Err(e) = self.scan_pending() {
            self.reuse_pool.push(data_idx);
            return Err(e);
        }

        let dh = match self.queue_shm.slot_header(data_idx) {
            Ok(dh) => dh,
            Err(_) => {
                self.reuse_pool.push(data_idx);
                return Err(Error::DataIndexOutOfRange);
            }
        };

        let gen = dh.generation.load(Ordering::Relaxed);
        dh.generation.store(gen.wrapping_add(1), Ordering::Release);

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let seq = self.next_sequence;
        self.next_sequence += 1;

        dh.timestamp.store(ts, Ordering::Release);
        dh.size.store(
            core::mem::size_of::<T>() as u32,
            Ordering::Release,
        );
        dh.sequence.store(seq, Ordering::Release);

        dh.generation.store(gen.wrapping_add(2), Ordering::Release);

        self.queue_push(data_idx);

        let history = self.queue_shm.header().history;
        while self.queue_shm.header().queue_len.load(Ordering::Acquire) > history {
            self.handle_overflow()?;
        }

        self.queue_shm
            .header()
            .publish_count
            .store(self.next_sequence, Ordering::Release);

        crate::queue_shm::futex_notify(self.queue_shm.header());
        let heartbeat = self.mono_epoch.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.queue_shm.header().heartbeat_ns.store(
            heartbeat,
            Ordering::Release,
        );

        Ok(())
    }

    pub fn discard(&mut self) {
        if let Some(data_idx) = self.current_write.take() {
            self.reuse_pool.push(data_idx);
        }
    }

    pub fn accept_notify_clients(&mut self) {}
}

impl<T: Copy> Drop for QueuePublisher<T> {
    fn drop(&mut self) {
        self.discard();
        self.queue_shm
            .header()
            .publisher_pid
            .store(0, Ordering::Release);
    }
}

impl<T: Copy> QueuePublisher<T> {
    fn queue_push(&mut self, data_idx: u32) {
        let hdr = self.queue_shm.header();
        let pos = hdr.pos_write.load(Ordering::Acquire);
        hdr.queue[pos as usize].store(data_idx, Ordering::Release);

        let next_pos = (pos + 1) % MAX_DATA_SHMS as u32;
        hdr.pos_write.store(next_pos, Ordering::Release);

        let new_len = hdr.queue_len.load(Ordering::Acquire) + 1;
        hdr.queue_len.store(new_len, Ordering::Release);

        hdr.latest_data_idx.store(data_idx, Ordering::Release);
    }

    fn queue_pop(&mut self) -> Option<u32> {
        let hdr = self.queue_shm.header();
        let qlen = hdr.queue_len.load(Ordering::Acquire);
        if qlen == 0 {
            return None;
        }

        let head = hdr.queue_head.load(Ordering::Acquire);
        let data_idx = hdr.queue[head as usize].load(Ordering::Acquire);
        hdr.queue[head as usize].store(u32::MAX, Ordering::Release);

        let next_head = (head + 1) % MAX_DATA_SHMS as u32;
        hdr.queue_head.store(next_head, Ordering::Release);
        hdr.queue_len.store(qlen - 1, Ordering::Release);

        Some(data_idx)
    }

    fn handle_overflow(&mut self) -> Result<()> {
        let evicted_idx = match self.queue_pop() {
            Some(idx) => idx,
            None => return Ok(()),
        };

        self.queue_shm.header().pub_overflow_count.fetch_add(1, Ordering::Relaxed);

        let rx = self.queue_shm.slot_header(evicted_idx)?.rx_count.load(Ordering::Acquire);
        let bitmask_active = self.queue_shm.header().any_reader_active(evicted_idx);

        if rx == 0 && !bitmask_active {
            self.reset_slot(evicted_idx)?;
            self.reuse_pool.push(evicted_idx);
        } else {
            let now_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            self.queue_shm
                .slot_header(evicted_idx)?
                .cnt_skip
                .store(1, Ordering::Release);
            self.queue_shm
                .slot_header(evicted_idx)?
                .timestamp
                .store(now_ns, Ordering::Release);
            self.queue_shm.header().pending_flags[evicted_idx as usize]
                .store(1, Ordering::Release);
            self.pending_pool.push(evicted_idx);

            if self.reuse_pool.is_empty() && !self.pending_pool.is_empty() {
                self.try_timeout_reclaim()?;
            }
        }

        Ok(())
    }

    fn try_timeout_reclaim(&mut self) -> Result<bool> {
        if self.pending_pool.is_empty() {
            return Ok(false);
        }

        let deadline_us = self.queue_shm.header().deadline_us;
        let timeout_ns: u64 = if deadline_us > 0 {
            deadline_us as u64 * 1_000
        } else {
            100_000_000
        };

        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let oldest_idx = self.pending_pool[0];
        let evict_ts = self.queue_shm.slot_header(oldest_idx)?.timestamp.load(Ordering::Acquire);

        if now_ns.saturating_sub(evict_ts) >= timeout_ns {
            let forced_idx = self.pending_pool.swap_remove(0);
            let sh = self.queue_shm.slot_header(forced_idx)?;
            sh.rx_count.store(0, Ordering::Release);
            self.queue_shm.header().clear_all_readers(forced_idx);
            self.reset_slot(forced_idx)?;
            self.reuse_pool.push(forced_idx);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn reset_slot(&mut self, index: u32) -> Result<()> {
        let dh = self.queue_shm.slot_header(index)?;
        let gen = dh.generation.load(Ordering::Relaxed);
        dh.generation.store(gen.wrapping_add(2), Ordering::Release);
        dh.cnt_skip.store(0, Ordering::Release);
        dh.cnt_read.store(0, Ordering::Release);
        self.queue_shm.header().pending_flags[index as usize].store(0, Ordering::Release);
        Ok(())
    }

    fn scan_pending(&mut self) -> Result<()> {
        let deadline_us = self.queue_shm.header().deadline_us;
        let timeout_ns: u64 = if deadline_us > 0 {
            deadline_us as u64 * 1_000
        } else {
            100_000_000
        };
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let mut i = 0;
        while i < self.pending_pool.len() {
            let idx = self.pending_pool[i];
            let sh = self.queue_shm.slot_header(idx)?;
            let rx = sh.rx_count.load(Ordering::Acquire);
            let bitmask_active = self.queue_shm.header().any_reader_active(idx);

            if rx == 0 && !bitmask_active {
                self.pending_pool.swap_remove(i);
                self.queue_shm.header().pending_flags[idx as usize]
                    .store(0, Ordering::Release);
                self.reset_slot(idx)?;
                self.reuse_pool.push(idx);
            } else {
                let evict_ts = sh.timestamp.load(Ordering::Acquire);
                if now_ns.saturating_sub(evict_ts) >= timeout_ns {
                    sh.rx_count.store(0, Ordering::Release);
                    self.queue_shm.header().clear_all_readers(idx);
                    self.pending_pool.swap_remove(i);
                    self.reset_slot(idx)?;
                    self.reuse_pool.push(idx);
                } else {
                    i += 1;
                }
            }
        }
        Ok(())
    }

    pub fn deadline_us(&self) -> u32 {
        self.queue_shm.deadline_us()
    }

    #[allow(dead_code)]
    pub fn queue_shm(&self) -> &QueueShm {
        &self.queue_shm
    }

    #[allow(dead_code)]
    pub(crate) fn queue_shm_mut(&mut self) -> &mut QueueShm {
        &mut self.queue_shm
    }

    pub fn shm_name(&self) -> String {
        crate::queue_shm::queue_shm_name(self.queue_shm.topic())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[repr(C)]
    #[derive(Clone, Copy, Default, Debug, PartialEq)]
    struct TestMsg {
        x: u32,
        y: u32,
    }

    #[test]
    fn borrow_fresh_and_publish() {
        let mut pub1 =
            QueuePublisher::<TestMsg>::new("test/qp_fresh", 4, 2, 5, 0).unwrap();
        {
            let msg = pub1.borrow(WriteMode::Fresh).unwrap();
            msg.x = 10;
            msg.y = 20;
        }
        pub1.publish().unwrap();

        let hdr = pub1.queue_shm.header();
        assert_eq!(hdr.queue_len.load(Ordering::Acquire), 1);
        assert_eq!(hdr.publish_count.load(Ordering::Acquire), 1);
    }

    #[test]
    fn borrow_update_copies_latest() {
        let mut pub1 =
            QueuePublisher::<TestMsg>::new("test/qp_update", 4, 2, 5, 0).unwrap();

        {
            let msg = pub1.borrow(WriteMode::Fresh).unwrap();
            msg.x = 100;
            msg.y = 200;
        }
        pub1.publish().unwrap();

        {
            let msg = pub1.borrow(WriteMode::Update).unwrap();
            assert_eq!(msg.x, 100);
            assert_eq!(msg.y, 200);
            msg.y = 300;
        }
        pub1.publish().unwrap();
    }

    #[test]
    fn fresh_zeroes_reused_slot() {
        let mut pub1 =
            QueuePublisher::<TestMsg>::new("test/qp_fresh_zero", 2, 2, 5, 0).unwrap();

        {
            let msg = pub1.borrow(WriteMode::Fresh).unwrap();
            msg.x = 0xDEAD;
            msg.y = 0xBEEF;
        }
        pub1.publish().unwrap();

        {
            let msg = pub1.borrow(WriteMode::Fresh).unwrap();
            msg.x = 1;
        }
        pub1.publish().unwrap();
        {
            let msg = pub1.borrow(WriteMode::Fresh).unwrap();
            msg.x = 2;
        }
        pub1.publish().unwrap();

        {
            let msg = pub1.borrow(WriteMode::Fresh).unwrap();
            assert_eq!(msg.x, 0, "Fresh should zero x on reused slot");
            assert_eq!(msg.y, 0, "Fresh should zero y on reused slot");
        }
        pub1.discard();
    }

    #[test]
    fn discard() {
        let mut pub1 =
            QueuePublisher::<TestMsg>::new("test/qp_discard", 4, 2, 5, 0).unwrap();
        let reuse_before = pub1.reuse_pool.len();
        {
            let msg = pub1.borrow(WriteMode::Fresh).unwrap();
            msg.x = 42;
        }
        assert_eq!(pub1.reuse_pool.len(), reuse_before - 1);
        pub1.discard();
        assert_eq!(pub1.reuse_pool.len(), reuse_before);
        assert!(pub1.current_write.is_none());
    }

    #[test]
    fn drop_discards_current_write() {
        let topic = "test/qp_drop_discard";
        {
            let mut pub1 =
                QueuePublisher::<TestMsg>::new(topic, 4, 2, 5, 0).unwrap();
            let _msg = pub1.borrow(WriteMode::Fresh).unwrap();
        }
        {
            let mut pub2 =
                QueuePublisher::<TestMsg>::new(topic, 4, 2, 5, 0).unwrap();
            assert_eq!(pub2.reuse_pool.len(), 6);
            let _msg = pub2.borrow(WriteMode::Fresh).unwrap();
            pub2.publish().unwrap();
        }
    }

    #[test]
    fn double_borrow_rejected() {
        let mut pub1 =
            QueuePublisher::<TestMsg>::new("test/qp_double", 4, 2, 5, 0).unwrap();
        let _msg = pub1.borrow(WriteMode::Fresh).unwrap();
        assert_eq!(
            pub1.borrow(WriteMode::Fresh).unwrap_err(),
            Error::AlreadyBorrowed
        );
        pub1.discard();
    }

    #[test]
    fn publish_without_borrow_fails() {
        let mut pub1 =
            QueuePublisher::<TestMsg>::new("test/qp_noborrow", 4, 2, 5, 0).unwrap();
        assert_eq!(pub1.publish().unwrap_err(), Error::NoBorrow);
    }

    #[test]
    fn queue_overflow_reuse() {
        let mut pub1 =
            QueuePublisher::<TestMsg>::new("test/qp_overflow", 2, 1, 5, 0).unwrap();

        for i in 0..3u32 {
            {
                let msg = pub1.borrow(WriteMode::Fresh).unwrap();
                msg.x = i;
            }
            pub1.publish().unwrap();
        }

        let hdr = pub1.queue_shm.header();
        assert_eq!(hdr.queue_len.load(Ordering::Acquire), 2);
        assert_eq!(hdr.publish_count.load(Ordering::Acquire), 3);
        assert!(!pub1.reuse_pool.is_empty());
    }

    #[test]
    fn queue_overflow_pending() {
        let mut pub1 =
            QueuePublisher::<TestMsg>::new("test/qp_pending", 2, 2, 5, 0).unwrap();

        for i in 0..2u32 {
            {
                let msg = pub1.borrow(WriteMode::Fresh).unwrap();
                msg.x = i;
            }
            pub1.publish().unwrap();
        }

        let hdr = pub1.queue_shm.header();
        let oldest_head = hdr.queue_head.load(Ordering::Acquire);
        let oldest_idx = hdr.queue[oldest_head as usize].load(Ordering::Acquire);
        pub1.queue_shm
            .slot_header(oldest_idx)
            .unwrap()
            .rx_count
            .fetch_add(1, Ordering::Release);

        {
            let msg = pub1.borrow(WriteMode::Fresh).unwrap();
            msg.x = 99;
        }
        pub1.publish().unwrap();

        assert!(
            pub1.pending_pool.contains(&oldest_idx),
            "evicted slot with active reader should be in pending pool"
        );
    }

    #[test]
    fn queue_overflow_timeout_reclaim() {
        let mut pub1 =
            QueuePublisher::<TestMsg>::new("test/qp_timeout", 2, 1, 5, 1).unwrap();

        for i in 0..2u32 {
            {
                let msg = pub1.borrow(WriteMode::Fresh).unwrap();
                msg.x = i;
            }
            pub1.publish().unwrap();
        }

        let hdr = pub1.queue_shm.header();
        let oldest_head = hdr.queue_head.load(Ordering::Acquire);
        let oldest_idx = hdr.queue[oldest_head as usize].load(Ordering::Acquire);
        pub1.queue_shm
            .slot_header(oldest_idx)
            .unwrap()
            .rx_count
            .fetch_add(1, Ordering::Release);

        {
            let msg = pub1.borrow(WriteMode::Fresh).unwrap();
            msg.x = 99;
        }
        pub1.publish().unwrap();

        std::thread::sleep(std::time::Duration::from_micros(50));
        {
            let msg = pub1.borrow(WriteMode::Fresh).unwrap();
            msg.x = 100;
        }
        pub1.publish().unwrap();

        assert!(
            !pub1.pending_pool.contains(&oldest_idx),
            "expired pending slot should have been reclaimed"
        );
        assert_eq!(
            pub1.queue_shm
                .slot_header(oldest_idx)
                .unwrap()
                .rx_count
                .load(Ordering::Acquire),
            0
        );
    }

    #[test]
    fn generation_seqlock() {
        let mut pub1 =
            QueuePublisher::<TestMsg>::new("test/qp_gen", 4, 2, 5, 0).unwrap();

        {
            let msg = pub1.borrow(WriteMode::Fresh).unwrap();
            msg.x = 1;
        }
        pub1.publish().unwrap();

        let hdr = pub1.queue_shm.header();
        let head = hdr.queue_head.load(Ordering::Acquire);
        let data_idx = hdr.queue[head as usize].load(Ordering::Acquire);
        let gen = pub1
            .queue_shm
            .slot_header(data_idx)
            .unwrap()
            .generation
            .load(Ordering::Acquire);
        assert!(gen > 0);
        assert_eq!(gen % 2, 0, "generation must be even after publish");
    }

    #[test]
    fn timestamp_monotonic() {
        let mut pub1 =
            QueuePublisher::<TestMsg>::new("test/qp_ts", 4, 2, 5, 0).unwrap();

        let mut prev_ts = 0u64;
        for i in 0..3u32 {
            {
                let msg = pub1.borrow(WriteMode::Fresh).unwrap();
                msg.x = i;
            }
            pub1.publish().unwrap();

            let hdr = pub1.queue_shm.header();
            let pos = hdr.pos_write.load(Ordering::Acquire);
            let prev_pos = if pos == 0 {
                MAX_DATA_SHMS as u32 - 1
            } else {
                pos - 1
            };
            let data_idx = hdr.queue[prev_pos as usize].load(Ordering::Acquire);
            let ts = pub1
                .queue_shm
                .slot_header(data_idx)
                .unwrap()
                .timestamp
                .load(Ordering::Acquire);
            assert!(ts >= prev_ts, "timestamp should be monotonically increasing");
            prev_ts = ts;
        }
    }

    #[test]
    fn sequence_monotonic() {
        let mut pub1 =
            QueuePublisher::<TestMsg>::new("test/qp_seq", 4, 2, 5, 0).unwrap();

        for i in 0..5u32 {
            {
                let msg = pub1.borrow(WriteMode::Fresh).unwrap();
                msg.x = i;
            }
            pub1.publish().unwrap();
        }

        assert_eq!(
            pub1.queue_shm.header().publish_count.load(Ordering::Acquire),
            5
        );
    }

    #[test]
    fn pending_recovery() {
        let mut pub1 =
            QueuePublisher::<TestMsg>::new("test/qp_recover", 2, 2, 5, 0).unwrap();

        for i in 0..2u32 {
            {
                let msg = pub1.borrow(WriteMode::Fresh).unwrap();
                msg.x = i;
            }
            pub1.publish().unwrap();
        }

        let hdr = pub1.queue_shm.header();
        let oldest_head = hdr.queue_head.load(Ordering::Acquire);
        let oldest_idx = hdr.queue[oldest_head as usize].load(Ordering::Acquire);
        pub1.queue_shm
            .slot_header(oldest_idx)
            .unwrap()
            .rx_count
            .fetch_add(1, Ordering::Release);

        {
            let msg = pub1.borrow(WriteMode::Fresh).unwrap();
            msg.x = 99;
        }
        pub1.publish().unwrap();
        assert!(pub1.pending_pool.contains(&oldest_idx));

        pub1.queue_shm
            .slot_header(oldest_idx)
            .unwrap()
            .rx_count
            .fetch_sub(1, Ordering::Release);

        {
            let msg = pub1.borrow(WriteMode::Fresh).unwrap();
            msg.x = 100;
        }
        pub1.publish().unwrap();
        assert!(
            !pub1.pending_pool.contains(&oldest_idx),
            "recovered slot should no longer be in pending pool"
        );
    }

    #[test]
    fn pending_not_recovered_while_reading() {
        let mut pub1 =
            QueuePublisher::<TestMsg>::new("test/qp_norecover", 2, 2, 5, 0).unwrap();

        for i in 0..2u32 {
            {
                let msg = pub1.borrow(WriteMode::Fresh).unwrap();
                msg.x = i;
            }
            pub1.publish().unwrap();
        }

        let hdr = pub1.queue_shm.header();
        let oldest_head = hdr.queue_head.load(Ordering::Acquire);
        let oldest_idx = hdr.queue[oldest_head as usize].load(Ordering::Acquire);
        pub1.queue_shm
            .slot_header(oldest_idx)
            .unwrap()
            .rx_count
            .fetch_add(1, Ordering::Release);

        {
            let msg = pub1.borrow(WriteMode::Fresh).unwrap();
            msg.x = 99;
        }
        pub1.publish().unwrap();
        assert!(pub1.pending_pool.contains(&oldest_idx));

        {
            let msg = pub1.borrow(WriteMode::Fresh).unwrap();
            msg.x = 100;
        }
        pub1.publish().unwrap();
        assert!(
            pub1.pending_pool.contains(&oldest_idx),
            "slot with active reader should stay in pending pool"
        );

        pub1.queue_shm
            .slot_header(oldest_idx)
            .unwrap()
            .rx_count
            .store(0, Ordering::Release);
    }

    #[test]
    fn many_publishes_no_memory_growth() {
        let mut pub1 =
            QueuePublisher::<TestMsg>::new("test/qp_bounded", 4, 2, 5, 0).unwrap();
        let initial_count = pub1.queue_shm.data_count();

        for i in 0..100u32 {
            {
                let msg = pub1.borrow(WriteMode::Fresh).unwrap();
                msg.x = i;
            }
            pub1.publish().unwrap();
        }

        assert_eq!(pub1.queue_shm.data_count(), initial_count);
        assert_eq!(
            pub1.queue_shm.header().publish_count.load(Ordering::Acquire),
            100
        );
    }

    #[test]
    fn multi_topic_data_isolation() {
        let mut pub_a =
            QueuePublisher::<TestMsg>::new("test/qp_mt_iso_a", 4, 2, 5, 0).unwrap();
        let mut pub_b =
            QueuePublisher::<TestMsg>::new("test/qp_mt_iso_b", 4, 2, 5, 0).unwrap();

        for i in 0..10u32 {
            {
                let msg = pub_a.borrow(WriteMode::Fresh).unwrap();
                msg.x = 1000 + i;
                msg.y = 2000 + i;
            }
            pub_a.publish().unwrap();

            {
                let msg = pub_b.borrow(WriteMode::Fresh).unwrap();
                msg.x = 5000 + i;
                msg.y = 6000 + i;
            }
            pub_b.publish().unwrap();
        }

        let hdr_a = pub_a.queue_shm.header();
        let pos_a = hdr_a.pos_write.load(Ordering::Acquire);
        let latest_a = if pos_a == 0 { MAX_DATA_SHMS as u32 - 1 } else { pos_a - 1 };
        let idx_a = hdr_a.queue[latest_a as usize].load(Ordering::Acquire);
        let ptr_a = pub_a.queue_shm.slot_data_ptr(idx_a).unwrap() as *const TestMsg;
        let data_a = unsafe { &*ptr_a };
        assert_eq!(data_a.x, 1009);
        assert_eq!(data_a.y, 2009);

        let hdr_b = pub_b.queue_shm.header();
        let pos_b = hdr_b.pos_write.load(Ordering::Acquire);
        let latest_b = if pos_b == 0 { MAX_DATA_SHMS as u32 - 1 } else { pos_b - 1 };
        let idx_b = hdr_b.queue[latest_b as usize].load(Ordering::Acquire);
        let ptr_b = pub_b.queue_shm.slot_data_ptr(idx_b).unwrap() as *const TestMsg;
        let data_b = unsafe { &*ptr_b };
        assert_eq!(data_b.x, 5009);
        assert_eq!(data_b.y, 6009);

        assert_eq!(hdr_a.publish_count.load(Ordering::Acquire), 10);
        assert_eq!(hdr_b.publish_count.load(Ordering::Acquire), 10);
    }

    #[test]
    fn multi_topic_simultaneous_overflow() {
        let mut pub_a =
            QueuePublisher::<TestMsg>::new("test/qp_mt_ovf_a", 2, 1, 5, 0).unwrap();
        let mut pub_b =
            QueuePublisher::<TestMsg>::new("test/qp_mt_ovf_b", 3, 1, 5, 0).unwrap();
        let mut pub_c =
            QueuePublisher::<TestMsg>::new("test/qp_mt_ovf_c", 1, 1, 0, 0).unwrap();

        for i in 0..20u32 {
            {
                let msg = pub_a.borrow(WriteMode::Fresh).unwrap();
                msg.x = i;
            }
            pub_a.publish().unwrap();

            {
                let msg = pub_b.borrow(WriteMode::Fresh).unwrap();
                msg.x = 100 + i;
            }
            pub_b.publish().unwrap();

            {
                let msg = pub_c.borrow(WriteMode::Fresh).unwrap();
                msg.x = 200 + i;
            }
            pub_c.publish().unwrap();
        }

        assert!(
            pub_a.queue_shm.header().queue_len.load(Ordering::Acquire) <= 2,
            "topic A queue_len exceeded history"
        );
        assert!(
            pub_b.queue_shm.header().queue_len.load(Ordering::Acquire) <= 3,
            "topic B queue_len exceeded history"
        );
        assert!(
            pub_c.queue_shm.header().queue_len.load(Ordering::Acquire) <= 1,
            "topic C queue_len exceeded history"
        );

        assert_eq!(pub_a.queue_shm.header().publish_count.load(Ordering::Acquire), 20);
        assert_eq!(pub_b.queue_shm.header().publish_count.load(Ordering::Acquire), 20);
        assert_eq!(pub_c.queue_shm.header().publish_count.load(Ordering::Acquire), 20);
    }

    #[test]
    fn multi_topic_lifecycle_independence() {
        let mut pub_a =
            QueuePublisher::<TestMsg>::new("test/qp_mt_lf_a", 4, 2, 5, 0).unwrap();
        let mut pub_b =
            QueuePublisher::<TestMsg>::new("test/qp_mt_lf_b", 4, 2, 5, 0).unwrap();
        let mut pub_c =
            QueuePublisher::<TestMsg>::new("test/qp_mt_lf_c", 4, 2, 5, 0).unwrap();

        for p in [&mut pub_a, &mut pub_b, &mut pub_c] {
            let msg = p.borrow(WriteMode::Fresh).unwrap();
            msg.x = 42;
            p.publish().unwrap();
        }

        drop(pub_b);

        {
            let msg = pub_a.borrow(WriteMode::Fresh).unwrap();
            msg.x = 100;
        }
        pub_a.publish().unwrap();
        assert_eq!(pub_a.queue_shm.header().publish_count.load(Ordering::Acquire), 2);

        {
            let msg = pub_c.borrow(WriteMode::Fresh).unwrap();
            msg.x = 200;
        }
        pub_c.publish().unwrap();
        assert_eq!(pub_c.queue_shm.header().publish_count.load(Ordering::Acquire), 2);

        let mut pub_b2 =
            QueuePublisher::<TestMsg>::new("test/qp_mt_lf_b", 4, 2, 5, 0).unwrap();
        {
            let msg = pub_b2.borrow(WriteMode::Fresh).unwrap();
            msg.x = 300;
        }
        pub_b2.publish().unwrap();
    }
}
