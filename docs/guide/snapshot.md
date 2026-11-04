# ZFS

Linux only. Keys match tunasync, so existing configs work as they are.

Each mirror's working directory must be a ZFS dataset of its own. If the directory is missing, not a mount point or not on ZFS, the sync does not run and the log gives the commands to create the dataset.

```toml
[zfs]
enable = true
zpool = "tank/mirrors"
```

`zpool` is the pool or parent dataset for the mirrors' datasets. With the config above, mirror `debian` uses `tank/mirrors/debian`:

```sh
zfs create tank/mirrors/debian
zfs set mountpoint=/srv/mirror/debian tank/mirrors/debian
chown hustsync /srv/mirror/debian
```

hustsync only checks; it never creates datasets.
