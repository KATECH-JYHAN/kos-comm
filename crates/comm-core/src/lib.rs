// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Jun-young Han <jyhan@katech.re.kr>
// KATECH SDV Platform Research Center

pub mod backend;
pub mod error;
pub mod fd_registry;
pub mod shm;
pub mod types;

pub mod queue_shm;
pub mod queue_publisher;
pub mod queue_subscriber;

pub use error::{Error, Result};
pub use fd_registry::FdRegistry;

pub use queue_publisher::QueuePublisher as Publisher;
pub use queue_subscriber::{QueueSubscriber as Subscriber, QueueLockGuard};

pub use queue_publisher::QueuePublisher;
pub use queue_subscriber::QueueSubscriber;
pub use queue_shm::{QueueShm, QueueShmHeader, ShmDataHeader, CACHE_LINE, MAX_DATA_SHMS};

#[cfg(not(target_os = "android"))]
pub use queue_shm::cleanup_stale_queue;
#[cfg(not(target_os = "android"))]
pub use queue_shm::cleanup_stale_queue as cleanup_stale;

pub use shm::list_shm_regions;
pub use types::*;

#[cfg(feature = "ivshmem")]
pub use backend::ivshmem::{self, IvshmemConfig, init as ivshmem_init};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    Fresh,
    Update,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadMode {
    #[default]
    Latest,
    Lossless,
}
