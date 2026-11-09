# Running mirrors in containers

Sync scripts with heavy dependencies can run in a container; the host only needs Docker.

## Enabling

```toml
[docker]
enable = true
volumes = ["/etc/hustsync/scripts:/scripts:ro"]
options = ["--init"]

[[mirrors]]
name = "crates"
provider = "command"
upstream = "https://crates.io/api/v1/crates"
command = "/usr/local/bin/panamax sync /mirror"
docker_image = "panamaxrs/panamax"
docker_volumes = ["/srv/mirror/crates:/mirror"]
docker_options = ["--entrypoint="]
memory_limit = "2G"
```

A mirror runs in a container only when `[docker] enable` is true and it sets `docker_image`; other mirrors run on the host as usual. Docker takes precedence over cgroups.

## How it runs

Each sync runs once as:

```text
docker run --rm -a STDOUT -a STDERR --name hustsync-job-<mirror> -w <mirror dir> -u <uid>:<gid>
  -v <volume>... -e <variable>... [-m <bytes>] <option>... <image> <command>
```

| Aspect | Detail |
|:--:|:--:|
| Volumes | `[docker] volumes`, `docker_volumes`, `exclude_file` read-only, the log directory, the log file and the mirror directory; the last three use the same path on the host and in the container |
| Environment | Only variable names are passed; the Docker client reads the values from its own environment, so secrets stay out of the process list |
| User | The worker's uid and gid |
| Memory | `memory_limit` in bytes, passed to `-m` |
| Stop | `docker stop -t 2`, then the Docker client is terminated |
| Cleanup | Waits up to 10 s for `--rm` to remove the container, so the next run's name is free |

## Permissions

The worker user needs access to Docker, usually through the `docker` group. That amounts to root on the host, so enable it only on trusted workers.
