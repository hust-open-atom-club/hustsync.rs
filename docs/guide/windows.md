# Windows

The manager, the worker and `hustsynctl` build and run on Windows; sync jobs use native executables.

| Item | On Windows |
|:--:|:--:|
| `command` mirrors | `.exe` files run directly; write scripts as `cmd /C ...` or `powershell -Command ...` |
| `exec_on_success` `exec_on_failure` | Run through `cmd /C`; variables are written `%HUSTSYNC_MIRROR_NAME%` |
| Stop and timeout | The whole process tree ends |
| `memory_limit` | Applies with `[cgroup] enable = true`, through one Job Object per job; an allocation over the limit fails and the run is recorded as out of memory |
| Worker killed | With `[cgroup]` enabled, its job processes end too |
| `latest` log | A symlink where allowed, otherwise a hard link |
| Hot reload | `hustsynctl reload -w <worker>` |

On Windows `[cgroup]` reads only `enable`. `fpsync`, `lsyncd` and `ftpsync` need Linux and are unavailable.

## rsync

Use an rsync built with Cygwin or MSYS2, such as MSYS2's `pacman -S rsync`, and put its path in the mirror's `command`:

```toml
[[mirrors]]
name = "debian"
provider = "rsync"
command = 'C:\msys64\usr\bin\rsync.exe'
upstream = "rsync://mirrors.example.org/debian/"
```

Write `mirror_dir` and a local `upstream` as ordinary Windows paths; the worker converts them to `/proc/cygdrive/c/...` for rsync.

## Running as a service

With `--service`, the service control manager starts and stops the manager or the worker; `--log-file` appends logs to a file. A service's working directory is `C:\Windows\System32`, so use absolute paths for configs and logs.

```powershell
New-Service hustsync-worker -StartupType Automatic -BinaryPathName 'C:\hustsync\hustsync.exe worker -c C:\hustsync\worker.conf --service --log-file C:\hustsync\worker.log'
sc.exe failure hustsync-worker reset= 86400 actions= restart/5000
Start-Service hustsync-worker
```

When the service stops, the worker terminates running syncs before it exits. If the worker exits on an error, the service control manager restarts it as `sc.exe failure` specifies.
