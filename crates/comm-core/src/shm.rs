// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Jun-young Han <jyhan@katech.re.kr>
// KATECH SDV Platform Research Center

#[cfg(target_os = "android")]
extern "C" {
    fn ASharedMemory_create(name: *const libc::c_char, size: libc::size_t) -> libc::c_int;

    #[allow(dead_code)]
    fn ASharedMemory_setProt(fd: libc::c_int, prot: libc::c_int) -> libc::c_int;
}

fn fnv1a_hash(topic: &str) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &b in topic.as_bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

pub fn topic_to_shm_name(topic: &str) -> String {
    if cfg!(target_os = "android") {
        format!("comm_{:08x}", fnv1a_hash(topic))
    } else {
        format!("/comm_{:08x}", fnv1a_hash(topic))
    }
}

#[cfg(not(target_os = "android"))]
pub fn list_shm_regions() -> Vec<String> {
    let mut names = Vec::new();
    if let Ok(entries) = std::fs::read_dir("/dev/shm") {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with("kos_q_") && name.len() == 14 {
                    names.push(format!("/{name}"));
                }
                if name.starts_with("comm_") && name.len() == 13 {
                    names.push(format!("/{name}"));
                }
            }
        }
    }
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_hash_deterministic() {
        let a = topic_to_shm_name("vehicle/perception/lidar/scan");
        let b = topic_to_shm_name("vehicle/perception/lidar/scan");
        assert_eq!(a, b);
        assert!(a.starts_with("/comm_"));
    }

    #[test]
    fn topic_hash_different_topics() {
        let a = topic_to_shm_name("topic_a");
        let b = topic_to_shm_name("topic_b");
        assert_ne!(a, b);
    }
}
