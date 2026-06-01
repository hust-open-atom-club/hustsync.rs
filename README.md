# hustsync.rs

`hustsync.rs` is a Rust port of [tunasync](https://github.com/tuna/tunasync), a mirror synchronization service built around a manager-worker architecture. The goal is to provide the same core operating model while using Rust, Tokio, and a Cargo workspace split by component.

## Overview

HustSync manages and runs mirror synchronization tasks through three main components:

- **Manager**: The central control plane. It stores mirror job state, handles worker registration, and exposes an HTTP/HTTPS API for workers and clients. It uses `redb` by default, with Redis support when configured.
- **Worker**: The execution engine. It connects to the manager, retrieves schedules, and runs mirror jobs from its local configuration.
- **hustsynctl**: The command-line control tool for listing workers/jobs and sending job commands through the manager.

## Project Status

The Rust implementation currently covers the core manager, worker, and control CLI flows. Worker providers currently include `rsync`, `two-stage-rsync`, and `command`.

Some Go-only worker features are not implemented yet, including Docker wrapping, cgroup enforcement, ZFS hooks, and btrfs snapshot hooks. Treat `tunasync` as the compatibility reference when porting or validating behavior.

## Architecture & Project Structure

The project is organized as a Cargo workspace with the following core crates:

- `crates/hustsync`: The main application entry point. It can run as either a manager (`hustsync m`) or a worker (`hustsync w`).
- `crates/hustsynctl`: The user-facing CLI control tool.
- `crates/hustsync-config-parser`: Configuration parsing, defaulting, include loading, and validation.
- `crates/hustsync-manager`: The manager API and persistence layer, built with Axum.
- `crates/hustsync-worker`: The worker runtime, scheduler, providers, and hooks.
- `crates/hustsync-internal`: Shared protocol types, status conversion, logging, and utilities.

## Build

Building this project requires the Rust toolchain, including Cargo.

Run the following command in the root directory of the project:

```bash
cargo build --release
```

The compiled binaries will be available in the `target/release/` directory:

- `hustsync`: The manager/worker daemon.
- `hustsynctl`: The control CLI.

## Usage

### 1. Start the Manager

```bash
# The example test config writes state under /tmp/hustsync_test.
mkdir -p /tmp/hustsync_test

target/release/hustsync m --config docs/example/test_manager.toml

# Or using command line arguments
target/release/hustsync m --addr 127.0.0.1 --port 14242 --db-file /tmp/manager.db
```

### 2. Start a Worker

```bash
target/release/hustsync w --config docs/example/test_worker.toml
```

### 3. Control with `hustsynctl`

```bash
# List all mirror status
target/release/hustsynctl list --all

# Start a specific mirror job on a specific worker
target/release/hustsynctl start -w test_worker elvish

# View worker status
target/release/hustsynctl workers
```

The generic files under `docs/example/manager.conf` and `docs/example/worker.conf` are templates. Fill in worker names, mirror names, upstream URLs, and storage paths before using them in production.

## Contributors

<a href="https://github.com/hust-open-atom-club/hustsync.rs/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=hust-open-atom-club/hustsync.rs" alt="Contributors" />
</a>
