# Getting started

Run a manager and a worker on one machine and watch syncs with `hustsynctl`.

## Building

```bash
cargo build --release
```

The binaries land in `target/release/`: `hustsync` (manager and worker) and `hustsynctl` (the client).

## Starting the manager

`manager.conf`:

```toml
debug = false

[server]
addr = "127.0.0.1"
port = 14242
ssl_cert = ""
ssl_key = ""

[files]
db_type = "redb"
db_file = "/tmp/hustsync/manager.db"
ca_cert = ""
```

```bash
hustsync manager --config manager.conf
```

## Starting the worker

`worker.conf`:

```toml
[global]
name = "test_worker"
log_dir = "/tmp/hustsync/log/{{.Name}}"
mirror_dir = "/tmp/hustsync/data"
concurrent = 10
interval = 1

[manager]
api_base = "http://localhost:14242"

[server]
hostname = "localhost"
listen_addr = "127.0.0.1"
listen_port = 6000

[[mirrors]]
name = "elvish"
provider = "rsync"
upstream = "rsync://rsync.elv.sh/elvish/"
use_ipv6 = false
```

```bash
hustsync worker --config worker.conf
```

`interval` is in minutes; 1 makes it easy to watch.

## Checking status

```bash
hustsynctl list -p 14242 --all
hustsynctl workers -p 14242
```

`list --all` prints the `/jobs` the front end reads.

Logs go in `log_dir`, one timestamped file per sync, with `latest` pointing at the newest.

## Next steps

- Keys: [configs.md](configs.md)
- Production deployment: [deploy.md](deploy.md)
