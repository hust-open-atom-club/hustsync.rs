# ZFS 与 Btrfs

两项都只在 Linux 上可用，键名与 tunasync 相同，旧配置原样可用。

## ZFS

每个镜像的工作目录必须是一个独立的 ZFS 数据集。目录不存在、不是挂载点、或挂的不是 ZFS，这次同步都不执行，并在日志里给出建数据集的命令。

```toml
[zfs]
enable = true
zpool = "tank/mirrors"
```

`zpool` 是镜像数据集所在的池或父数据集。按上面的配置，镜像 `debian` 的数据集是 `tank/mirrors/debian`：

```sh
zfs create tank/mirrors/debian
zfs set mountpoint=/srv/mirror/debian tank/mirrors/debian
chown hustsync /srv/mirror/debian
```

hustsync 不建数据集，只检查。

## Btrfs 快照

工作目录是一个 Btrfs 子卷。每次同步成功后，给它拍一份快照放到发布目录，对外只发布快照，用户就看不到同步到一半的文件。

```toml
[btrfs_snapshot]
enable = true
snapshot_path = "/srv/publish"

[[mirrors]]
name = "elvish"
snapshot_path = "/srv/publish/extra/elvish"
```

- 快照放在 `snapshot_path/镜像名`；镜像自己写了 `snapshot_path` 时用镜像的。
- 子镜像不继承父镜像的 `snapshot_path`，否则会互相覆盖。
- 工作目录不存在时建成子卷；已存在却不是子卷时，同步不执行。
- 同步失败时保留上一份快照。

新快照先建在发布目录里的隐藏名下，再用一次 `rename` 与旧快照对调，最后删掉旧的。发布路径始终有内容，不存在先删后建的空窗。

直接调用内核接口，不依赖 `btrfs-progs`。

## 不用 root 运行

worker 用户要对工作目录与发布目录都有写和执行权限，Btrfs 分区要带 `user_subvol_rm_allowed` 挂载，否则删不掉旧快照：

```sh
mount -o remount,user_subvol_rm_allowed /srv
```

完整样例见 [docs/example/snapshot/worker.conf](../example/snapshot/worker.conf)。
