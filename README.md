# KOS-comm

Repositories: [kos-exec](https://github.com/KATECH-JYHAN/kos-exec) · [kos-comm](https://github.com/KATECH-JYHAN/kos-comm) · [kos-safety](https://github.com/KATECH-JYHAN/kos-safety)

A cross-process pub/sub library built on Linux shared memory (POSIX SHM) (`comm-core`).
It is the default transport of [KOS-exec](https://github.com/KATECH-JYHAN/kos-exec).

- One SHM file per topic; fixed-size `#[repr(C)]` messages exchanged zero-copy
- One publisher, many subscribers, the last N messages retained
- New-data notification via futex (0% CPU while waiting)
- Single dependency: `libc`

> Linux only

## Installation

```toml
[dependencies]
comm-core = { git = "https://github.com/KATECH-JYHAN/kos-comm" }
```

## Usage

Messages are `#[repr(C)]` structs that implement `Copy`.

```rust
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Imu {
    x: f32,
    y: f32,
    z: f32,
}
```

**Publisher**

```rust
use comm_core::{Publisher, WriteMode};

// topic, history, spare slots, reclaim threshold, deadline (μs, 0 = none)
let mut publisher = Publisher::<Imu>::new("sensor/imu", 8, 2, 5, 0)?;

let slot = publisher.borrow(WriteMode::Fresh)?;   // write directly into the SHM slot
*slot = Imu { x: 1.0, y: 2.0, z: 3.0 };
publisher.publish()?;
```

**Subscriber**

```rust
use comm_core::Subscriber;

// Returns Err if the publisher does not exist yet — retry until it does
let subscriber = Subscriber::<Imu>::new("sensor/imu")?;

let guard = subscriber.recv(1000)?;               // wait for new data (timeout 1000ms)
let imu = guard.latest().unwrap();                // zero-copy reference
println!("{} {} {}", imu.x, imu.y, imu.z);
```

| Method | Behavior |
|--------|----------|
| `recv(timeout_ms)` | Waits for new data, then returns the latest value |
| `lock_latest()` | Latest value without waiting (`Error::NoData` if none) |
| `lock_latest_new()` | Latest value only if new since the last read |
| `lock_latest_n(n)` | The last n values (`guard.get(i)`, `guard.len()`) |
| `wait_for_publish(timeout_ms)` | Waits for a new-data notification only (`bool`) |
| `is_publisher_alive()` | Whether the publisher process is alive |
| `missed_count()` | Messages overwritten before they were read |

- A slot is not overwritten while its guard (`QueueLockGuard`) is alive. Do not hold guards for long.
- SHM names are hashes of the topic name (`/dev/shm/kos_q_<hash>`). Remove leftover regions with `comm_core::cleanup_stale()`.

## Build and test

```bash
cargo build --release
cargo test
```

## Author

Jun-young Han, Senior Researcher — KATECH SDV Platform Research Center · jyhan@katech.re.kr

## License

Copyright (c) 2026 Jun-young Han. Licensed under the [Apache License 2.0](LICENSE).
