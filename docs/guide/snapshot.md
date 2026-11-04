# ZFS and Btrfs

Both are Linux only. Keys match tunasync, so existing configs work as they are.

## ZFS

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

## Btrfs snapshots

The working directory is a Btrfs subvolume. After each successful sync it is snapshotted into a publish directory, and only the snapshot is served, so users never see a sync half done.

```toml
[btrfs_snapshot]
enable = true
snapshot_path = "/srv/publish"

[[mirrors]]
name = "elvish"
snapshot_path = "/srv/publish/extra/elvish"
```

- The snapshot goes in `snapshot_path/<mirror>`, or the mirror's own `snapshot_path` when set.
- Children do not inherit their parent's `snapshot_path`, or they would overwrite each other.
- A missing working directory is created as a subvolume; an existing one that is not a subvolume stops the sync.
- A failed sync keeps the previous snapshot.

The new snapshot is created under a hidden name in the publish directory, swapped with the old one in a single `rename`, and the old one is then deleted. The published path always has content; there is no gap between deleting and creating.

It calls the kernel directly and does not need `btrfs-progs`.

## Running without root

The worker user needs write and execute permission on both the working and the publish directories, and the Btrfs filesystem must be mounted with `user_subvol_rm_allowed`, or old snapshots cannot be deleted:

```sh
mount -o remount,user_subvol_rm_allowed /srv
```

For a complete example, see [docs/example/snapshot/worker.conf](../example/snapshot/worker.conf).
