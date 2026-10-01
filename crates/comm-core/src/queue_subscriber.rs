// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Jun-young Han <jyhan@katech.re.kr>
// KATECH SDV Platform Research Center

use crate::error::{Error, Result};
use crate::queue_shm::{QueueShm, QueueShmHeader, ShmDataHeader, MAX_DATA_SHMS};
use crate::ReadMode;

use core::cell::Cell;
use core::marker::PhantomData;
use core::sync::atomic::Ordering;

pub struct QueueLockGuard<'a, T: Copy> {
    entries: Vec<LockedEntry<'a, T>>,
}

struct LockedEntry<'a, T> {
    data: &'a T,
    header: &'a ShmDataHeader,
    shm_header: &'a QueueShmHeader,
    data_idx: u32,
    reader_id: Option<u32>,
    #[allow(dead_code)]
    generation: u32,
    sequence: u64,
    timestamp: u64,
    deadline_us: u32,
}

impl<'a, T: Copy> QueueLockGuard<'a, T> {
    pub fn latest(&self) -> Option<&T> {
        self.entries.last().map(|e| e.data)
    }

    pub fn get(&self, i: usize) -> Option<&T> {
        self.entries.get(i).map(|e| e.data)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn sequence(&self, i: usize) -> Option<u64> {
        self.entries.get(i).map(|e| e.sequence)
    }

    pub fn timestamp(&self, i: usize) -> Option<u64> {
        self.entries.get(i).map(|e| e.timestamp)
    }

    pub fn is_deadline_missed(&self, i: usize) -> Option<bool> {
        self.entries.get(i).map(|e| {
            if e.deadline_us == 0 {
                return false;
            }
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            let deadline_ns = e.deadline_us as u64 * 1000;
            now_ns.saturating_sub(e.timestamp) > deadline_ns
        })
    }

    pub fn is_latest_deadline_missed(&self) -> bool {
        self.entries.last().is_some_and(|e| {
            if e.deadline_us == 0 {
                return false;
            }
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            let deadline_ns = e.deadline_us as u64 * 1000;
            now_ns.saturating_sub(e.timestamp) > deadline_ns
        })
    }

    pub fn age_ns(&self, i: usize) -> Option<u64> {
        self.entries.get(i).map(|e| {
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            now_ns.saturating_sub(e.timestamp)
        })
    }
}

impl<T: Copy> Drop for QueueLockGuard<'_, T> {
    fn drop(&mut self) {
        for entry in &self.entries {
            if let Some(rid) = entry.reader_id {
                let mask = !(1u64 << entry.data_idx);
                entry.shm_header.reader_bitmasks[rid as usize]
                    .slots
                    .fetch_and(mask, Ordering::Release);
            } else {
                let _ = entry.header.rx_count.fetch_update(
                    Ordering::Release,
                    Ordering::Acquire,
                    |r| Some(r.saturating_sub(1)),
                );
            }
        }
    }
}

pub struct QueueSubscriber<T: Copy> {
    queue_shm: QueueShm,
    reader_id: Option<u32>,
    mode: ReadMode,
    last_seen_seq: Cell<Option<u64>>,
    deadline_miss_count: Cell<u64>,
    cached_publish_count: Cell<u64>,
    expected_publisher_pid: u32,
    futex_cached: std::cell::Cell<u32>,
    _marker: PhantomData<T>,
}

impl<T: Copy> QueueSubscriber<T> {
    pub fn new(topic: &str) -> Result<Self> {
        Self::new_with_mode(topic, ReadMode::Latest)
    }

    pub fn new_with_mode(topic: &str, mode: ReadMode) -> Result<Self> {
        let queue_shm = QueueShm::attach(topic)?;
        let data_size = core::mem::size_of::<T>() as u32;
        if data_size > 0 && queue_shm.data_size() < data_size {
            return Err(Error::InvalidConfig);
        }
        let reader_id = queue_shm.header().alloc_reader_id();
        let expected_publisher_pid = queue_shm.header().publisher_pid.load(Ordering::Acquire);

        let futex_cached = std::cell::Cell::new(
            queue_shm.header().notify_efd.load(Ordering::Acquire)
        );

        Ok(Self {
            queue_shm,
            reader_id,
            mode,
            last_seen_seq: Cell::new(None),
            deadline_miss_count: Cell::new(0),
            cached_publish_count: Cell::new(0),
            expected_publisher_pid,
            futex_cached,
            _marker: PhantomData,
        })
    }

    pub fn set_mode(&mut self, mode: ReadMode) {
        self.mode = mode;
    }

    pub fn read_mode(&self) -> ReadMode {
        self.mode
    }

    pub fn recv(&self, timeout_ms: i32) -> Result<QueueLockGuard<'_, T>> {
        if !self.wait_for_publish(timeout_ms) {
            return Err(crate::error::Error::Timeout);
        }
        self.lock_latest()
    }

    pub fn wait_for_publish(&self, timeout_ms: i32) -> bool {
        let timeout_ns = timeout_ms as u64 * 1_000_000;
        let mut cached = self.futex_cached.get();
        let result = crate::queue_shm::futex_wait_for_publish(
            self.queue_shm.header(), &mut cached, timeout_ns
        );
        self.futex_cached.set(cached);
        result
    }

    pub fn notify_fd(&self) -> i32 { -1 }

    #[inline]
    pub fn has_new_data(&self) -> bool {
        let pc = self
            .queue_shm
            .header()
            .publish_count
            .load(Ordering::Acquire);
        pc != self.cached_publish_count.get()
    }

    pub fn lock_latest_new(&self) -> Result<QueueLockGuard<'_, T>> {
        let hdr = self.queue_shm.header();
        let pc = hdr.publish_count.load(Ordering::Acquire);
        if pc == self.cached_publish_count.get() {
            return Err(Error::NoData);
        }

        let data_idx = hdr.latest_data_idx.load(Ordering::Acquire);
        if data_idx == u32::MAX {
            return Err(Error::NoData);
        }

        let entry = self.acquire_locked(data_idx)?;

        if let Some(prev) = self.last_seen_seq.get() {
            if entry.sequence > prev + 1 {
                let gap = entry.sequence - prev - 1;
                hdr.sub_total_miss.fetch_add(gap, Ordering::Relaxed);
            }
        }
        self.last_seen_seq.set(Some(entry.sequence));
        self.cached_publish_count.set(pc);

        if entry.deadline_us > 0 {
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            let deadline_ns = entry.deadline_us as u64 * 1000;
            if now_ns.saturating_sub(entry.timestamp) > deadline_ns {
                self.deadline_miss_count.set(self.deadline_miss_count.get() + 1);
            }
        }
        Ok(QueueLockGuard {
            entries: vec![entry],
        })
    }

    pub fn lock_latest(&self) -> Result<QueueLockGuard<'_, T>> {
        let hdr = self.queue_shm.header();
        let qlen = hdr.queue_len.load(Ordering::Acquire);
        if qlen == 0 {
            return Err(Error::NoData);
        }

        let pos = hdr.pos_write.load(Ordering::Acquire);
        let latest_pos = if pos == 0 {
            MAX_DATA_SHMS as u32 - 1
        } else {
            pos - 1
        };
        let data_idx = hdr.queue[latest_pos as usize].load(Ordering::Acquire);

        let entry = self.acquire_locked(data_idx)?;

        if let Some(prev) = self.last_seen_seq.get() {
            if entry.sequence > prev + 1 {
                let gap = entry.sequence - prev - 1;
                hdr.sub_total_miss.fetch_add(gap, Ordering::Relaxed);
            }
        }
        self.last_seen_seq.set(Some(entry.sequence));
        if entry.deadline_us > 0 {
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            let deadline_ns = entry.deadline_us as u64 * 1000;
            if now_ns.saturating_sub(entry.timestamp) > deadline_ns {
                self.deadline_miss_count.set(self.deadline_miss_count.get() + 1);
            }
        }
        Ok(QueueLockGuard {
            entries: vec![entry],
        })
    }

    pub fn lock_latest_n(&self, n: u32) -> Result<QueueLockGuard<'_, T>> {
        let hdr = self.queue_shm.header();
        let qlen = hdr.queue_len.load(Ordering::Acquire);
        if qlen == 0 {
            return Err(Error::NoData);
        }

        let count = n.min(qlen);
        let pos = hdr.pos_write.load(Ordering::Acquire);

        let mut entries = Vec::with_capacity(count as usize);
        for i in 0..count {
            let target_pos =
                (pos + MAX_DATA_SHMS as u32 - count + i) % MAX_DATA_SHMS as u32;
            let data_idx = hdr.queue[target_pos as usize].load(Ordering::Acquire);
            match self.acquire_locked(data_idx) {
                Ok(entry) => entries.push(entry),
                Err(_) => {
                    for e in &entries {
                        if let Some(rid) = e.reader_id {
                            let mask = !(1u64 << e.data_idx);
                            e.shm_header.reader_bitmasks[rid as usize]
                                .slots
                                .fetch_and(mask, Ordering::Release);
                        } else {
                            let _ = e.header.rx_count.fetch_update(
                                Ordering::Release,
                                Ordering::Acquire,
                                |r| Some(r.saturating_sub(1)),
                            );
                        }
                    }
                    return Err(Error::NoData);
                }
            }
        }

        if let Some(last) = entries.last() {
            self.last_seen_seq.set(Some(last.sequence));
        }

        Ok(QueueLockGuard { entries })
    }

    pub fn lock_next(&self) -> Result<QueueLockGuard<'_, T>> {
        let target_seq = match self.last_seen_seq.get() {
            Some(last) => last + 1,
            None => {
                return self.lock_latest();
            }
        };

        let hdr = self.queue_shm.header();
        let qlen = hdr.queue_len.load(Ordering::Acquire);
        if qlen == 0 {
            return Err(Error::NoData);
        }

        let head = hdr.queue_head.load(Ordering::Acquire);
        let num = hdr.num.load(Ordering::Acquire);

        for i in 0..qlen {
            let pos = (head + i) % MAX_DATA_SHMS as u32;
            let data_idx = hdr.queue[pos as usize].load(Ordering::Acquire);
            if data_idx >= num {
                continue;
            }

            let sh = match self.queue_shm.slot_header(data_idx) {
                Ok(sh) => sh,
                Err(_) => continue,
            };
            let seq = sh.sequence.load(Ordering::Acquire);

            if seq == target_seq {
                let entry = self.acquire_locked(data_idx)?;
                self.last_seen_seq.set(Some(entry.sequence));

                if entry.deadline_us > 0 {
                    let now_ns = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as u64;
                    let deadline_ns = entry.deadline_us as u64 * 1000;
                    if now_ns.saturating_sub(entry.timestamp) > deadline_ns {
                        self.deadline_miss_count
                            .set(self.deadline_miss_count.get() + 1);
                    }
                }
                return Ok(QueueLockGuard {
                    entries: vec![entry],
                });
            }

            if seq > target_seq {
                let entry = self.acquire_locked(data_idx)?;
                let gap = entry.sequence - target_seq;
                hdr.sub_total_miss.fetch_add(gap, Ordering::Relaxed);
                self.last_seen_seq.set(Some(entry.sequence));
                return Ok(QueueLockGuard {
                    entries: vec![entry],
                });
            }
        }

        Err(Error::NoData)
    }

    pub fn lock(&self) -> Result<QueueLockGuard<'_, T>> {
        match self.mode {
            ReadMode::Latest => self.lock_latest(),
            ReadMode::Lossless => self.lock_next(),
        }
    }

    pub fn lock_range(
        &self,
        from_ts: u64,
        to_ts: u64,
    ) -> Result<QueueLockGuard<'_, T>> {
        if from_ts > to_ts {
            return Err(Error::InvalidRange);
        }

        let hdr = self.queue_shm.header();
        let qlen = hdr.queue_len.load(Ordering::Acquire);
        let head = hdr.queue_head.load(Ordering::Acquire);

        let mut entries = Vec::new();
        for i in 0..qlen {
            let pos = (head + i) % MAX_DATA_SHMS as u32;
            let data_idx = hdr.queue[pos as usize].load(Ordering::Acquire);
            let num = hdr.num.load(Ordering::Acquire);
            if data_idx >= num {
                continue;
            }
            let ts = match self.queue_shm.slot_header(data_idx) {
                Ok(sh) => sh.timestamp.load(Ordering::Acquire),
                Err(_) => continue,
            };
            if ts >= from_ts && ts <= to_ts {
                match self.acquire_locked(data_idx) {
                    Ok(entry) => entries.push(entry),
                    Err(_) => continue,
                }
            }
        }

        if entries.is_empty() {
            return Err(Error::NoData);
        }
        Ok(QueueLockGuard { entries })
    }

    pub fn missed_count(&self) -> u64 {
        let total = self
            .queue_shm
            .header()
            .publish_count
            .load(Ordering::Acquire);
        match self.last_seen_seq.get() {
            Some(last) => total.saturating_sub(last + 1),
            None => 0,
        }
    }

    pub fn has_missed(&self) -> bool {
        self.missed_count() > 0
    }

    pub fn total_published(&self) -> u64 {
        self.queue_shm
            .header()
            .publish_count
            .load(Ordering::Acquire)
    }

    pub fn last_seen_sequence(&self) -> Option<u64> {
        self.last_seen_seq.get()
    }

    pub fn deadline_miss_count(&self) -> u64 {
        self.deadline_miss_count.get()
    }

    pub fn reset_deadline_miss_count(&self) {
        self.deadline_miss_count.set(0);
    }

    pub fn topic_deadline_us(&self) -> u32 {
        self.queue_shm.deadline_us()
    }

    pub fn is_publisher_alive(&self) -> bool {
        let pid = self.queue_shm.header().publisher_pid.load(Ordering::Acquire);
        if pid == 0 {
            return false;
        }
        unsafe {
            libc::kill(pid as i32, 0) == 0
                || *libc::__errno_location() == libc::EPERM
        }
    }

    pub fn publisher_pid(&self) -> Option<u32> {
        let pid = self.queue_shm.header().publisher_pid.load(Ordering::Acquire);
        if pid == 0 {
            None
        } else {
            Some(pid)
        }
    }

    pub fn publisher_heartbeat_ns(&self) -> Option<u64> {
        let hb = self
            .queue_shm
            .header()
            .heartbeat_ns
            .load(Ordering::Acquire);
        if hb == 0 {
            None
        } else {
            Some(hb)
        }
    }

    fn acquire_locked(&self, data_idx: u32) -> Result<LockedEntry<'_, T>> {
        if self.queue_shm.readonly {
            return Err(Error::InvalidConfig);
        }
        let num = self.queue_shm.header().num.load(Ordering::Acquire);
        if data_idx >= num {
            return Err(Error::DataIndexOutOfRange);
        }

        let dh = self.queue_shm.slot_header(data_idx)?;
        let shm_header = self.queue_shm.header();

        let gen1 = dh.generation.load(Ordering::Acquire);
        if gen1 == 0 || !gen1.is_multiple_of(2) {
            return Err(Error::NoData);
        }

        if let Some(rid) = self.reader_id {
            let bit = 1u64 << data_idx;
            shm_header.reader_bitmasks[rid as usize]
                .slots
                .fetch_or(bit, Ordering::AcqRel);
        } else {
            dh.rx_count.fetch_add(1, Ordering::AcqRel);
        }

        let current_pid = shm_header.publisher_pid.load(Ordering::Acquire);
        if self.expected_publisher_pid != 0
            && current_pid != 0
            && current_pid != self.expected_publisher_pid
        {
            if let Some(rid) = self.reader_id {
                let mask = !(1u64 << data_idx);
                shm_header.reader_bitmasks[rid as usize]
                    .slots
                    .fetch_and(mask, Ordering::Release);
            } else {
                dh.rx_count.fetch_sub(1, Ordering::Release);
            }
            return Err(Error::Stale);
        }

        let seq = dh.sequence.load(Ordering::Acquire);
        let ts = dh.timestamp.load(Ordering::Acquire);

        let data_ptr =
            unsafe { self.queue_shm.slot_data_ptr_unchecked(data_idx) } as *const T;
        let data = unsafe { &*data_ptr };

        let gen2 = dh.generation.load(Ordering::Acquire);
        if gen2 != gen1 {
            if let Some(rid) = self.reader_id {
                let mask = !(1u64 << data_idx);
                shm_header.reader_bitmasks[rid as usize]
                    .slots
                    .fetch_and(mask, Ordering::Release);
            } else {
                dh.rx_count.fetch_sub(1, Ordering::Release);
            }
            return Err(Error::Stale);
        }

        Ok(LockedEntry {
            data,
            header: dh,
            shm_header,
            data_idx,
            reader_id: self.reader_id,
            generation: gen1,
            sequence: seq,
            timestamp: ts,
            deadline_us: self.queue_shm.deadline_us(),
        })
    }

    pub fn queue_shm(&self) -> &QueueShm {
        &self.queue_shm
    }
}

#[cfg(feature = "live_update")]
impl<T: Copy> QueueSubscriber<T> {
    pub fn reconnect_shm(&mut self, new_topic: &str) -> Result<()> {
        let new_shm = QueueShm::attach(new_topic)?;
        let data_size = core::mem::size_of::<T>() as u32;
        if data_size > 0 && new_shm.data_size() < data_size {
            return Err(Error::InvalidConfig);
        }
        self.queue_shm = new_shm;
        Ok(())
    }

    pub fn reconnect_with_shm(&mut self, new_shm: QueueShm) -> Result<()> {
        let data_size = core::mem::size_of::<T>() as u32;
        if data_size > 0 && new_shm.data_size() < data_size {
            return Err(Error::InvalidConfig);
        }
        self.queue_shm = new_shm;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue_publisher::QueuePublisher;
    use crate::WriteMode;

    #[repr(C)]
    #[derive(Clone, Copy, Default, Debug, PartialEq)]
    struct Msg {
        seq: u32,
        value: f32,
    }

    fn pub_sub_pair(
        topic: &str,
        history: u32,
        margin: u32,
    ) -> (QueuePublisher<Msg>, QueueSubscriber<Msg>) {
        let publ = QueuePublisher::<Msg>::new(topic, history, margin, 5, 0).unwrap();
        let sub = QueueSubscriber::<Msg>::new(topic).unwrap();
        (publ, sub)
    }

    #[test]
    fn data_size_mismatch_rejected() {
        #[repr(C, packed)]
        #[derive(Clone, Copy, Default, Debug)]
        struct Small { a: u32 }

        #[repr(C, packed)]
        #[derive(Clone, Copy, Debug)]
        struct Big { a: [u8; 32], b: [u8; 32] }

        let _pub = QueuePublisher::<Small>::new("test/qs_size_check", 4, 2, 5, 0).unwrap();

        let result = QueueSubscriber::<Big>::new("test/qs_size_check");
        assert!(result.is_err());
    }

    #[test]
    fn lock_latest_basic() {
        let (mut p, s) = pub_sub_pair("test/qs_lock", 4, 2);

        {
            let msg = p.borrow(WriteMode::Fresh).unwrap();
            msg.seq = 7;
            msg.value = 1.0;
        }
        p.publish().unwrap();

        let guard = s.lock_latest().unwrap();
        assert_eq!(guard.len(), 1);
        let data = guard.latest().unwrap();
        assert_eq!(data.seq, 7);

        let hdr = s.queue_shm.header();
        let pos = hdr.pos_write.load(Ordering::Acquire);
        let latest_pos = if pos == 0 {
            MAX_DATA_SHMS as u32 - 1
        } else {
            pos - 1
        };
        let data_idx = hdr.queue[latest_pos as usize].load(Ordering::Acquire);
        let rid = s.reader_id.unwrap();
        let bit = 1u64 << data_idx;
        assert_ne!(hdr.reader_bitmasks[rid as usize].slots.load(Ordering::Acquire) & bit, 0);

        drop(guard);

        assert_eq!(hdr.reader_bitmasks[rid as usize].slots.load(Ordering::Acquire) & bit, 0);
    }

    #[test]
    fn lock_latest_n() {
        let (mut p, s) = pub_sub_pair("test/qs_lockn", 8, 2);

        for i in 0..5u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
        }

        let guard = s.lock_latest_n(3).unwrap();
        assert_eq!(guard.len(), 3);
        assert_eq!(guard.get(0).unwrap().seq, 2);
        assert_eq!(guard.get(1).unwrap().seq, 3);
        assert_eq!(guard.get(2).unwrap().seq, 4);
        assert_eq!(guard.latest().unwrap().seq, 4);

        drop(guard);
    }

    #[test]
    fn lock_multiple_concurrent() {
        let (mut p, s) = pub_sub_pair("test/qs_multilock", 4, 2);

        {
            let msg = p.borrow(WriteMode::Fresh).unwrap();
            msg.seq = 1;
        }
        p.publish().unwrap();

        let g1 = s.lock_latest().unwrap();
        let g2 = s.lock_latest().unwrap();

        let hdr = s.queue_shm.header();
        let pos = hdr.pos_write.load(Ordering::Acquire);
        let latest_pos = if pos == 0 {
            MAX_DATA_SHMS as u32 - 1
        } else {
            pos - 1
        };
        let data_idx = hdr.queue[latest_pos as usize].load(Ordering::Acquire);
        let rid = s.reader_id.unwrap();
        let bit = 1u64 << data_idx;

        assert_ne!(hdr.reader_bitmasks[rid as usize].slots.load(Ordering::Acquire) & bit, 0);

        drop(g1);

        drop(g2);
        assert_eq!(hdr.reader_bitmasks[rid as usize].slots.load(Ordering::Acquire) & bit, 0);
    }

    #[test]
    fn missed_count_no_loss() {
        let (mut p, s) = pub_sub_pair("test/qs_miss_none", 8, 2);

        for i in 0..3u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
            let _ = s.lock_latest().unwrap();
        }

        assert_eq!(s.missed_count(), 0);
        assert!(!s.has_missed());
        assert_eq!(s.total_published(), 3);
        assert_eq!(s.last_seen_sequence(), Some(2));
    }

    #[test]
    fn missed_count_detects_gap() {
        let (mut p, s) = pub_sub_pair("test/qs_miss_gap", 8, 2);

        {
            let msg = p.borrow(WriteMode::Fresh).unwrap();
            msg.seq = 0;
        }
        p.publish().unwrap();
        let _ = s.lock_latest().unwrap();

        for i in 1..6u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
        }

        assert_eq!(s.missed_count(), 5);
        assert!(s.has_missed());

        let _ = s.lock_latest().unwrap();
        assert_eq!(s.missed_count(), 0);
    }

    #[test]
    fn publisher_alive() {
        let (_p, s) = pub_sub_pair("test/qs_alive", 4, 2);
        assert!(s.is_publisher_alive());
        assert_eq!(s.publisher_pid(), Some(std::process::id()));
        assert!(s.publisher_heartbeat_ns().is_some());
    }

    #[test]
    fn lock_held_causes_pending_on_overflow() {
        let (mut p, s) = pub_sub_pair("test/qs_lock_pending", 2, 2);

        for i in 0..2u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
        }

        let guard = s.lock_latest_n(1).unwrap();
        let locked_seq = guard.get(0).unwrap().seq;
        assert_eq!(locked_seq, 1);

        drop(guard);
        let guard_oldest = {
            let hdr = s.queue_shm.header();
            let head = hdr.queue_head.load(Ordering::Acquire);
            let data_idx = hdr.queue[head as usize].load(Ordering::Acquire);

            s.queue_shm
                .slot_header(data_idx)
                .unwrap()
                .rx_count
                .fetch_add(1, Ordering::Release);
            data_idx
        };

        let reading_before = s
            .queue_shm
            .slot_header(guard_oldest)
            .unwrap()
            .rx_count
            .load(Ordering::Acquire);
        assert_eq!(reading_before, 1, "slot should be locked");

        {
            let msg = p.borrow(WriteMode::Fresh).unwrap();
            msg.seq = 99;
        }
        p.publish().unwrap();

        let reading_after = s
            .queue_shm
            .slot_header(guard_oldest)
            .unwrap()
            .rx_count
            .load(Ordering::Acquire);
        assert_eq!(
            reading_after, 1,
            "locked slot should NOT be force-reclaimed (cnt_skip < threshold)"
        );

        let cnt_skip = s
            .queue_shm
            .slot_header(guard_oldest)
            .unwrap()
            .cnt_skip
            .load(Ordering::Acquire);
        assert_eq!(cnt_skip, 1, "skip count should be incremented");

        let pending = s.queue_shm.header().pending_flags[guard_oldest as usize]
            .load(Ordering::Acquire);
        assert_eq!(pending, 1, "slot should be marked as pending");

        s.queue_shm
            .slot_header(guard_oldest)
            .unwrap()
            .rx_count
            .store(0, Ordering::Release);
    }

    #[test]
    fn lock_released_recovers_pending() {
        let (mut p, s) = pub_sub_pair("test/qs_lock_recover", 2, 2);

        for i in 0..2u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
        }

        let hdr = s.queue_shm.header();
        let head = hdr.queue_head.load(Ordering::Acquire);
        let locked_idx = hdr.queue[head as usize].load(Ordering::Acquire);
        s.queue_shm
            .slot_header(locked_idx)
            .unwrap()
            .rx_count
            .fetch_add(1, Ordering::Release);

        {
            let msg = p.borrow(WriteMode::Fresh).unwrap();
            msg.seq = 10;
        }
        p.publish().unwrap();

        let pending = s.queue_shm.header().pending_flags[locked_idx as usize]
            .load(Ordering::Acquire);
        assert_eq!(pending, 1, "slot should be pending");

        s.queue_shm
            .slot_header(locked_idx)
            .unwrap()
            .rx_count
            .store(0, Ordering::Release);

        {
            let msg = p.borrow(WriteMode::Fresh).unwrap();
            msg.seq = 20;
        }
        p.publish().unwrap();

        let pending = s.queue_shm.header().pending_flags[locked_idx as usize]
            .load(Ordering::Acquire);
        assert_eq!(
            pending, 0,
            "slot should be recovered from pending after reading released"
        );
    }

    #[test]
    fn lock_held_timeout_reclaim() {
        let topic = "test/qs_lock_timeout";
        let mut p = QueuePublisher::<Msg>::new(topic, 2, 2, 2, 1).unwrap();
        let s = QueueSubscriber::<Msg>::new(topic).unwrap();

        for i in 0..2u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
        }

        let hdr = s.queue_shm.header();
        let head = hdr.queue_head.load(Ordering::Acquire);
        let locked_idx = hdr.queue[head as usize].load(Ordering::Acquire);
        s.queue_shm
            .slot_header(locked_idx)
            .unwrap()
            .rx_count
            .fetch_add(1, Ordering::Release);

        std::thread::sleep(std::time::Duration::from_micros(10));
        for i in 10..13u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
        }

        let rx = s
            .queue_shm
            .slot_header(locked_idx)
            .unwrap()
            .rx_count
            .load(Ordering::Acquire);
        assert_eq!(
            rx, 0,
            "rx_count should be 0 after timeout reclaim"
        );
    }

    #[test]
    fn guard_drop_allows_slot_reuse() {
        let (mut p, s) = pub_sub_pair("test/qs_guard_reuse", 2, 2);

        for i in 0..2u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
        }

        let guard = s.lock_latest().unwrap();
        let data = guard.latest().unwrap();
        assert_eq!(data.seq, 1);

        let hdr = s.queue_shm.header();
        let pos = hdr.pos_write.load(Ordering::Acquire);
        let latest_pos = if pos == 0 {
            MAX_DATA_SHMS as u32 - 1
        } else {
            pos - 1
        };
        let locked_idx = hdr.queue[latest_pos as usize].load(Ordering::Acquire);
        let rid = s.reader_id.unwrap();
        let bit = 1u64 << locked_idx;
        assert_ne!(hdr.reader_bitmasks[rid as usize].slots.load(Ordering::Acquire) & bit, 0);

        drop(guard);
        assert_eq!(hdr.reader_bitmasks[rid as usize].slots.load(Ordering::Acquire) & bit, 0);

        for i in 10..20u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
        }
        let guard = s.lock_latest().unwrap();
        assert_eq!(guard.latest().unwrap().seq, 19);
    }

    #[test]
    fn multithread_lock_vs_publish() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let topic = "test/qs_mt_lock_pub";
        let mut publ = QueuePublisher::<Msg>::new(topic, 4, 4, 3, 0).unwrap();

        for i in 0..4u32 {
            {
                let msg = publ.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            publ.publish().unwrap();
        }

        let barrier = Arc::new(Barrier::new(2));

        let b = barrier.clone();
        let t = topic.to_string();
        let sub_thread = thread::spawn(move || {
            let sub = QueueSubscriber::<Msg>::new(&t).unwrap();
            b.wait();

            let mut held_count = 0u32;
            for _ in 0..20 {
                if let Ok(guard) = sub.lock_latest_n(2) {
                    held_count += 1;
                    std::thread::sleep(std::time::Duration::from_micros(100));
                    let _len = guard.len();
                }
            }
            held_count
        });

        barrier.wait();
        let mut pub_count = 0u32;
        for i in 100..200u32 {
            {
                let msg = publ.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            publ.publish().unwrap();
            pub_count += 1;
            std::thread::sleep(std::time::Duration::from_micros(10));
        }

        let held = sub_thread.join().unwrap();

        assert!(pub_count == 100, "publisher should complete all 100 publishes");
        assert!(held > 0, "subscriber should have held locks at least once");

        let sub = QueueSubscriber::<Msg>::new(topic).unwrap();
        let guard = sub.lock_latest().unwrap();
        assert!(guard.latest().unwrap().seq >= 100, "latest should be from publisher's run");
    }

    #[test]
    fn multi_node_different_histories() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let configs: Vec<(&str, u32, u32, u32, u32)> = vec![
            ("test/mn_small",       2,      1,         2,            20),
            ("test/mn_medium",      8,      4,         5,            50),
            ("test/mn_large",      32,      8,         10,          100),
            ("test/mn_tiny",        1,      1,         0,            30),
        ];

        let mut publishers: Vec<_> = configs
            .iter()
            .map(|(topic, h, m, t, _)| {
                QueuePublisher::<Msg>::new(topic, *h, *m, *t, 0).unwrap()
            })
            .collect();

        for (i, (_, h, _, _, _)) in configs.iter().enumerate() {
            for seq in 0..*h {
                {
                    let msg = publishers[i].borrow(WriteMode::Fresh).unwrap();
                    msg.seq = seq;
                    msg.value = i as f32;
                }
                publishers[i].publish().unwrap();
            }
        }

        let barrier = Arc::new(Barrier::new(configs.len() + 1));

        let handles: Vec<_> = configs
            .iter()
            .map(|(topic, _, _, _, _)| {
                let b = barrier.clone();
                let t = topic.to_string();
                thread::spawn(move || {
                    let sub = QueueSubscriber::<Msg>::new(&t).unwrap();
                    b.wait();

                    let mut lock_count = 0u32;
                    for _ in 0..200 {
                        if let Ok(guard) = sub.lock_latest() {
                            lock_count += 1;
                            let _d = guard.latest();
                        }
                        std::hint::spin_loop();
                    }
                    (t, lock_count)
                })
            })
            .collect();

        barrier.wait();
        for (i, (_, _, _, _, pub_count)) in configs.iter().enumerate() {
            for seq in 1000..(1000 + *pub_count) {
                {
                    let msg = publishers[i].borrow(WriteMode::Fresh).unwrap();
                    msg.seq = seq;
                    msg.value = i as f32 * 100.0;
                }
                publishers[i].publish().unwrap();
            }
        }

        for h in handles {
            let (topic, locks) = h.join().unwrap();
            assert!(locks > 0, "{topic}: should have successful locks");
        }

        for (i, (topic, _, _, _, _)) in configs.iter().enumerate() {
            let sub = QueueSubscriber::<Msg>::new(topic).unwrap();
            let guard = sub.lock_latest().unwrap();
            let latest = guard.latest().unwrap();
            let expected_value = i as f32 * 100.0;
            assert!(
                (latest.value - expected_value).abs() < 1e-5,
                "{topic}: data cross-contamination! got {}, expected {}",
                latest.value,
                expected_value,
            );
        }
    }

    #[test]
    fn multi_node_different_msg_sizes() {
        #[repr(C)]
        #[derive(Clone, Copy, Default)]
        struct Small {
            id: u32,
        }

        #[repr(C)]
        #[derive(Clone, Copy, Default)]
        struct Medium {
            id: u32,
            data: [f64; 16],
        }

        #[repr(C)]
        #[derive(Clone, Copy)]
        struct Large {
            id: u32,
            _pad: u32,
            matrix: [f64; 64],
        }

        let mut p_s = QueuePublisher::<Small>::new("test/mn_sz_s", 4, 2, 5, 0).unwrap();
        let mut p_m = QueuePublisher::<Medium>::new("test/mn_sz_m", 8, 2, 5, 0).unwrap();
        let mut p_l = QueuePublisher::<Large>::new("test/mn_sz_l", 2, 2, 5, 0).unwrap();

        let s_s = QueueSubscriber::<Small>::new("test/mn_sz_s").unwrap();
        let s_m = QueueSubscriber::<Medium>::new("test/mn_sz_m").unwrap();
        let s_l = QueueSubscriber::<Large>::new("test/mn_sz_l").unwrap();

        for i in 0..20u32 {
            {
                let msg = p_s.borrow(WriteMode::Fresh).unwrap();
                msg.id = i;
            }
            p_s.publish().unwrap();
        }

        for i in 0..30u32 {
            {
                let msg = p_m.borrow(WriteMode::Fresh).unwrap();
                msg.id = i;
                msg.data[0] = i as f64 * 1.1;
            }
            p_m.publish().unwrap();
        }

        for i in 0..10u32 {
            {
                let msg = p_l.borrow(WriteMode::Fresh).unwrap();
                msg.id = i;
                msg.matrix[0] = i as f64 * 2.2;
                msg.matrix[63] = 99.9;
            }
            p_l.publish().unwrap();
        }

        let gs = s_s.lock_latest().unwrap();
        assert_eq!(gs.latest().unwrap().id, 19);
        drop(gs);

        let gm = s_m.lock_latest().unwrap();
        let rm = *gm.latest().unwrap();
        assert_eq!(rm.id, 29);
        assert!((rm.data[0] - 29.0 * 1.1).abs() < 1e-10);
        drop(gm);

        let gl = s_l.lock_latest().unwrap();
        let rl = *gl.latest().unwrap();
        assert_eq!(rl.id, 9);
        assert!((rl.matrix[0] - 9.0 * 2.2).abs() < 1e-10);
        assert!((rl.matrix[63] - 99.9).abs() < 1e-10);
        drop(gl);

        let all_s = s_s.lock_latest_n(4).unwrap();
        assert_eq!(all_s.len(), 4);
        drop(all_s);

        let all_m = s_m.lock_latest_n(8).unwrap();
        assert_eq!(all_m.len(), 8);
        drop(all_m);

        let all_l = s_l.lock_latest_n(2).unwrap();
        assert_eq!(all_l.len(), 2);
    }

    #[test]
    fn multi_node_concurrent_lock_overflow() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let topics = [
            ("test/mn_cl_a", 2u32, 2u32),
            ("test/mn_cl_b", 4, 4),
            ("test/mn_cl_c", 16, 4),
        ];

        let mut pubs: Vec<_> = topics
            .iter()
            .map(|(t, h, m)| QueuePublisher::<Msg>::new(t, *h, *m, 3, 0).unwrap())
            .collect();

        for (i, (_, h, _)) in topics.iter().enumerate() {
            for seq in 0..*h {
                {
                    let msg = pubs[i].borrow(WriteMode::Fresh).unwrap();
                    msg.seq = seq;
                    msg.value = i as f32;
                }
                pubs[i].publish().unwrap();
            }
        }

        let barrier = Arc::new(Barrier::new(topics.len() + 1));

        let mut handles = Vec::new();

        for (topic, _, _) in &topics {
            let b = barrier.clone();
            let t = topic.to_string();
            handles.push(thread::spawn(move || {
                let sub = QueueSubscriber::<Msg>::new(&t).unwrap();
                b.wait();
                let mut ok = 0u32;
                for _ in 0..50 {
                    if let Ok(g) = sub.lock_latest() {
                        ok += 1;
                        std::thread::sleep(std::time::Duration::from_micros(50));
                        let _ = g.latest();
                    }
                }
                ok
            }));
        }

        barrier.wait();

        for round in 0..30u32 {
            for (i, _) in topics.iter().enumerate() {
                {
                    let msg = pubs[i].borrow(WriteMode::Fresh).unwrap();
                    msg.seq = 1000 + round;
                    msg.value = i as f32;
                }
                pubs[i].publish().unwrap();
            }
            std::thread::sleep(std::time::Duration::from_micros(20));
        }

        for h in handles {
            let count = h.join().unwrap();
            assert!(count > 0, "each subscriber should have had successful reads");
        }

        for (topic, _, _) in &topics {
            let sub = QueueSubscriber::<Msg>::new(topic).unwrap();
            assert!(sub.lock_latest().is_ok(), "{topic} should be readable");
        }
    }

    #[test]
    fn multithread_lock_unlock() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let topic = "test/qs_mt_lock";
        let mut publ = QueuePublisher::<Msg>::new(topic, 8, 4, 5, 0).unwrap();

        for i in 0..4u32 {
            {
                let msg = publ.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            publ.publish().unwrap();
        }

        let barrier = Arc::new(Barrier::new(3));

        let handles: Vec<_> = (0..2)
            .map(|_| {
                let b = barrier.clone();
                let t = topic.to_string();
                thread::spawn(move || {
                    let sub = QueueSubscriber::<Msg>::new(&t).unwrap();
                    b.wait();
                    for _ in 0..100 {
                        if let Ok(guard) = sub.lock_latest() {
                            let _data = guard.latest();
                        }
                    }
                })
            })
            .collect();

        barrier.wait();
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn multi_topic_pubsub_isolation() {
        let topics = ["test/mt_iso_a", "test/mt_iso_b", "test/mt_iso_c"];
        let mut pubs: Vec<_> = topics
            .iter()
            .map(|t| QueuePublisher::<Msg>::new(t, 8, 2, 5, 0).unwrap())
            .collect();
        let subs: Vec<_> = topics
            .iter()
            .map(|t| QueueSubscriber::<Msg>::new(t).unwrap())
            .collect();

        for (i, p) in pubs.iter_mut().enumerate() {
            for seq in 0..5u32 {
                {
                    let msg = p.borrow(WriteMode::Fresh).unwrap();
                    msg.seq = (i as u32) * 1000 + seq;
                    msg.value = (i as f32) * 100.0 + seq as f32;
                }
                p.publish().unwrap();
            }
        }

        for (i, s) in subs.iter().enumerate() {
            let guard = s.lock_latest_n(5).unwrap();
            assert_eq!(guard.len(), 5, "topic {} should have 5 entries", i);
            for j in 0..guard.len() {
                let expected_seq = (i as u32) * 1000 + j as u32;
                assert_eq!(
                    { guard.get(j).unwrap().seq }, expected_seq,
                    "topic {} entry {} seq mismatch", i, j
                );
            }
        }

        for s in &subs {
            let _ = s.lock_latest().unwrap();
            assert_eq!(s.missed_count(), 0);
        }
    }

    #[test]
    fn multi_topic_concurrent_pubsub() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let num_topics = 4;
        let msgs_per_topic = 50u32;
        let barrier = Arc::new(Barrier::new(num_topics));

        let handles: Vec<_> = (0..num_topics)
            .map(|i| {
                let b = barrier.clone();
                thread::spawn(move || {
                    let topic = format!("test/mt_conc_{}", i);
                    let mut p = QueuePublisher::<Msg>::new(&topic, 16, 4, 5, 0).unwrap();
                    let s = QueueSubscriber::<Msg>::new(&topic).unwrap();

                    b.wait();

                    for seq in 0..msgs_per_topic {
                        {
                            let msg = p.borrow(WriteMode::Fresh).unwrap();
                            msg.seq = (i as u32) * 10000 + seq;
                            msg.value = seq as f32;
                        }
                        p.publish().unwrap();
                    }

                    let guard = s.lock_latest().unwrap();
                    let latest_seq = { guard.latest().unwrap().seq };
                    assert!(
                        latest_seq >= (i as u32) * 10000
                            && latest_seq < (i as u32) * 10000 + msgs_per_topic,
                        "topic {} got wrong seq {}", i, latest_seq
                    );
                    drop(guard);

                    let all = s.lock_latest_n(16).unwrap();
                    for j in 0..all.len() {
                        let seq = { all.get(j).unwrap().seq };
                        assert!(
                            seq >= (i as u32) * 10000
                                && seq < (i as u32) * 10000 + msgs_per_topic,
                            "topic {} has foreign seq {}", i, seq
                        );
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn multi_topic_subscriber_multiple_topics() {
        let num_topics = 5;
        let mut publishers: Vec<_> = (0..num_topics)
            .map(|i| {
                let topic = format!("test/mt_msub_{}", i);
                QueuePublisher::<Msg>::new(&topic, 4, 2, 5, 0).unwrap()
            })
            .collect();

        let subscribers: Vec<_> = (0..num_topics)
            .map(|i| {
                let topic = format!("test/mt_msub_{}", i);
                QueueSubscriber::<Msg>::new(&topic).unwrap()
            })
            .collect();

        for (i, p) in publishers.iter_mut().enumerate() {
            for seq in 0..(i as u32 + 1) * 3 {
                {
                    let msg = p.borrow(WriteMode::Fresh).unwrap();
                    msg.seq = seq;
                    msg.value = i as f32;
                }
                p.publish().unwrap();
            }
        }

        let history = 4u32;
        for (i, s) in subscribers.iter().enumerate() {
            let published = (i as u32 + 1) * 3;
            let expected_count = published.min(history);
            let guard = s.lock_latest_n(history).unwrap();
            assert_eq!(
                guard.len(),
                expected_count as usize,
                "topic {} entry count mismatch (published={}, history={})", i, published, history
            );
            drop(guard);

            let guard = s.lock_latest().unwrap();
            let v = { guard.latest().unwrap().value };
            assert!(
                (v - i as f32).abs() < 1e-5,
                "topic {} latest value={}, expected {}", i, v, i
            );
        }
    }

    #[test]
    fn multi_topic_drop_one_publisher_others_survive() {
        let mut pub_a = QueuePublisher::<Msg>::new("test/mt_drop_a", 4, 2, 5, 0).unwrap();
        let mut pub_b = QueuePublisher::<Msg>::new("test/mt_drop_b", 4, 2, 5, 0).unwrap();
        let sub_a = QueueSubscriber::<Msg>::new("test/mt_drop_a").unwrap();
        let sub_b = QueueSubscriber::<Msg>::new("test/mt_drop_b").unwrap();

        for i in 0..3u32 {
            {
                let msg = pub_a.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            pub_a.publish().unwrap();
            {
                let msg = pub_b.borrow(WriteMode::Fresh).unwrap();
                msg.seq = 10 + i;
            }
            pub_b.publish().unwrap();
        }

        drop(pub_a);

        assert!(!sub_a.is_publisher_alive());

        assert!(sub_b.is_publisher_alive());
        let gb = sub_b.lock_latest().unwrap();
        assert_eq!({ gb.latest().unwrap().seq }, 12);
        drop(gb);

        let ga = sub_a.lock_latest().unwrap();
        assert_eq!({ ga.latest().unwrap().seq }, 2);
        drop(ga);

        {
            let msg = pub_b.borrow(WriteMode::Fresh).unwrap();
            msg.seq = 99;
        }
        pub_b.publish().unwrap();

        let gb2 = sub_b.lock_latest().unwrap();
        assert_eq!({ gb2.latest().unwrap().seq }, 99);
    }

    #[test]
    fn deadline_miss_detection() {
        let mut p = QueuePublisher::<Msg>::new("test/qs_dl_miss", 4, 2, 5, 10_000).unwrap();
        let s = QueueSubscriber::<Msg>::new("test/qs_dl_miss").unwrap();

        {
            let msg = p.borrow(WriteMode::Fresh).unwrap();
            msg.seq = 1;
        }
        p.publish().unwrap();

        let guard = s.lock_latest().unwrap();
        assert_eq!(guard.is_latest_deadline_missed(), false, "immediate read should not miss 10ms deadline");
        assert_eq!(s.deadline_miss_count(), 0);
        drop(guard);

        {
            let msg = p.borrow(WriteMode::Fresh).unwrap();
            msg.seq = 2;
        }
        p.publish().unwrap();

        std::thread::sleep(std::time::Duration::from_millis(15));

        let guard2 = s.lock_latest().unwrap();
        assert_eq!(guard2.is_latest_deadline_missed(), true, "read after 15ms should miss 10ms deadline");
        assert_eq!(s.deadline_miss_count(), 1);
    }

    #[test]
    fn no_deadline_never_missed() {
        let mut p = QueuePublisher::<Msg>::new("test/qs_dl_none", 4, 2, 5, 0).unwrap();
        let s = QueueSubscriber::<Msg>::new("test/qs_dl_none").unwrap();

        {
            let msg = p.borrow(WriteMode::Fresh).unwrap();
            msg.seq = 1;
        }
        p.publish().unwrap();

        std::thread::sleep(std::time::Duration::from_millis(20));

        let guard = s.lock_latest().unwrap();
        assert_eq!(guard.is_latest_deadline_missed(), false, "no deadline should never miss");
        assert_eq!(s.deadline_miss_count(), 0);
    }

    #[test]
    fn lock_guard_deadline_check() {
        let mut p = QueuePublisher::<Msg>::new("test/qs_dl_lock", 4, 2, 5, 10_000).unwrap();
        let s = QueueSubscriber::<Msg>::new("test/qs_dl_lock").unwrap();

        {
            let msg = p.borrow(WriteMode::Fresh).unwrap();
            msg.seq = 1;
        }
        p.publish().unwrap();

        std::thread::sleep(std::time::Duration::from_millis(15));

        let guard = s.lock_latest().unwrap();
        assert_eq!(guard.is_latest_deadline_missed(), true);
        assert_eq!(guard.is_deadline_missed(0), Some(true));
        assert!(guard.age_ns(0).unwrap() > 10_000_000);
        assert_eq!(s.deadline_miss_count(), 1);
    }

    #[test]
    fn deadline_age_ns() {
        let mut p = QueuePublisher::<Msg>::new("test/qs_dl_age", 4, 2, 5, 0).unwrap();
        let s = QueueSubscriber::<Msg>::new("test/qs_dl_age").unwrap();

        {
            let msg = p.borrow(WriteMode::Fresh).unwrap();
            msg.seq = 1;
        }
        p.publish().unwrap();

        std::thread::sleep(std::time::Duration::from_millis(5));

        let guard = s.lock_latest().unwrap();
        let age = guard.age_ns(0).unwrap();
        assert!(age >= 4_000_000, "age_ns should be >= ~5ms, got {age}");
    }

    #[test]
    fn deadline_us_stored_in_shm() {
        let deadline = 50_000u32;
        let _p = QueuePublisher::<Msg>::new("test/qs_dl_stored", 4, 2, 5, deadline).unwrap();
        let s = QueueSubscriber::<Msg>::new("test/qs_dl_stored").unwrap();

        assert_eq!(s.topic_deadline_us(), deadline);
    }

    #[test]
    fn deadline_miss_count_accumulates() {
        let mut p = QueuePublisher::<Msg>::new("test/qs_dl_accum", 4, 2, 5, 1_000).unwrap();
        let s = QueueSubscriber::<Msg>::new("test/qs_dl_accum").unwrap();

        for i in 0..3u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            let _ = s.lock_latest().unwrap();
        }

        assert_eq!(s.deadline_miss_count(), 3, "all 3 reads should have missed 1ms deadline");

        s.reset_deadline_miss_count();
        assert_eq!(s.deadline_miss_count(), 0, "count should be reset");
    }

    #[test]
    fn default_mode_is_latest() {
        let _p = QueuePublisher::<Msg>::new("test/qs_mode_default", 4, 2, 5, 0).unwrap();
        let s = QueueSubscriber::<Msg>::new("test/qs_mode_default").unwrap();
        assert_eq!(s.read_mode(), ReadMode::Latest);
    }

    #[test]
    fn new_with_mode_lossless() {
        let _p = QueuePublisher::<Msg>::new("test/qs_mode_ll", 4, 2, 5, 0).unwrap();
        let s = QueueSubscriber::<Msg>::new_with_mode("test/qs_mode_ll", ReadMode::Lossless).unwrap();
        assert_eq!(s.read_mode(), ReadMode::Lossless);
    }

    #[test]
    fn set_mode_changes_mode() {
        let _p = QueuePublisher::<Msg>::new("test/qs_mode_set", 4, 2, 5, 0).unwrap();
        let mut s = QueueSubscriber::<Msg>::new("test/qs_mode_set").unwrap();
        assert_eq!(s.read_mode(), ReadMode::Latest);
        s.set_mode(ReadMode::Lossless);
        assert_eq!(s.read_mode(), ReadMode::Lossless);
    }

    #[test]
    fn lock_dispatches_by_mode_latest() {
        let (mut p, s) = pub_sub_pair("test/qs_lock_dispatch_l", 4, 2);
        for i in 0..3u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
        }
        let guard = s.lock().unwrap();
        assert_eq!(guard.latest().unwrap().seq, 2);
    }

    #[test]
    fn lock_next_sequential_read() {
        let (mut p, s) = pub_sub_pair("test/qs_lnext_seq", 8, 2);

        for i in 0..5u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
        }

        let g = s.lock_next().unwrap();
        assert_eq!(g.latest().unwrap().seq, 4);
        drop(g);

        for i in 5..8u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
        }

        let g = s.lock_next().unwrap();
        assert_eq!(g.latest().unwrap().seq, 5);
        drop(g);

        let g = s.lock_next().unwrap();
        assert_eq!(g.latest().unwrap().seq, 6);
        drop(g);

        let g = s.lock_next().unwrap();
        assert_eq!(g.latest().unwrap().seq, 7);
        drop(g);

        assert!(s.lock_next().is_err());
    }

    #[test]
    fn lock_next_no_loss() {
        let (mut p, s) = pub_sub_pair("test/qs_lnext_noloss", 16, 4);

        let total = 20u32;
        for i in 0..total {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
        }

        let g = s.lock_next().unwrap();
        let _first_seq = g.latest().unwrap().seq;
        drop(g);

        let more = 10u32;
        for i in total..(total + more) {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();

            let g = s.lock_next().unwrap();
            assert_eq!(
                g.latest().unwrap().seq, i,
                "lossless should read seq {} but got {}",
                i,
                g.latest().unwrap().seq
            );
            drop(g);
        }

        assert_eq!(s.missed_count(), 0);
    }

    #[test]
    fn lock_next_eviction_recovery() {
        let (mut p, s) = pub_sub_pair("test/qs_lnext_evict", 2, 2);

        {
            let msg = p.borrow(WriteMode::Fresh).unwrap();
            msg.seq = 0;
        }
        p.publish().unwrap();
        let g = s.lock_next().unwrap();
        assert_eq!(g.latest().unwrap().seq, 0);
        drop(g);

        for i in 1..10u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
        }

        let g = s.lock_next().unwrap();
        let recovered_seq = g.latest().unwrap().seq;
        assert!(recovered_seq > 0, "should skip ahead after eviction");
        drop(g);
    }

    #[test]
    fn lock_unified_lossless_mode() {
        let topic = "test/qs_lock_unified_ll";
        let mut p = QueuePublisher::<Msg>::new(topic, 8, 2, 5, 0).unwrap();
        let s = QueueSubscriber::<Msg>::new_with_mode(topic, ReadMode::Lossless).unwrap();

        for i in 0..3u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
        }

        let g = s.lock().unwrap();
        assert_eq!(g.latest().unwrap().seq, 2);
        drop(g);

        for i in 3..5u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
        }

        let g = s.lock().unwrap();
        assert_eq!(g.latest().unwrap().seq, 3);
        drop(g);

        let g = s.lock().unwrap();
        assert_eq!(g.latest().unwrap().seq, 4);
        drop(g);
    }

    #[test]
    fn two_subscribers_different_modes() {
        let topic = "test/qs_two_modes";
        let mut p = QueuePublisher::<Msg>::new(topic, 8, 2, 5, 0).unwrap();
        let s_latest = QueueSubscriber::<Msg>::new(topic).unwrap();
        let s_lossless = QueueSubscriber::<Msg>::new_with_mode(topic, ReadMode::Lossless).unwrap();

        for i in 0..5u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
        }

        let g = s_latest.lock().unwrap();
        assert_eq!(g.latest().unwrap().seq, 4);
        drop(g);

        let g = s_lossless.lock().unwrap();
        assert_eq!(g.latest().unwrap().seq, 4);
        drop(g);

        for i in 5..8u32 {
            {
                let msg = p.borrow(WriteMode::Fresh).unwrap();
                msg.seq = i;
            }
            p.publish().unwrap();
        }

        let g = s_latest.lock().unwrap();
        assert_eq!(g.latest().unwrap().seq, 7);
        drop(g);

        let g = s_lossless.lock().unwrap();
        assert_eq!(g.latest().unwrap().seq, 5);
        drop(g);

        let g = s_lossless.lock().unwrap();
        assert_eq!(g.latest().unwrap().seq, 6);
        drop(g);

        let g = s_lossless.lock().unwrap();
        assert_eq!(g.latest().unwrap().seq, 7);
        drop(g);
    }
}
