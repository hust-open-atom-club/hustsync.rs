# 资源隔离

启用后每个镜像在自己的 cgroup 里同步：限制内存，同步结束时清理所有残留进程，超出内存上限时上报为 OOM。支持 cgroup v1 与 v2，自动识别。Windows 上同样的配置由 Job Object 实现，见 [windows.md](windows.md)。

## 配置

```toml
[cgroup]
enable = true
base_path = "/sys/fs/cgroup"
group = ""

[[mirrors]]
name = "debian"
memory_limit = "512M"
```

| 项 | 说明 |
|:--:|:--:|
| 组名 | 镜像名，建在父组下；上次异常退出留下的同名组先清掉 |
| 入组 | 子进程在 exec 之前加入，派生的所有进程都在组内 |
| 内存 | v2 写 `memory.max`，v1 写 `memory.limit_in_bytes`；同时禁止使用 swap，超出即触发 OOM |
| 清理 | 杀掉组内残留进程，最多三轮，再删除组 |
| OOM | 进程失败且组内 OOM 计数大于 0 时，上报 `killed by the OOM killer, memory_limit exceeded` |

`memory_limit` 写法：`256M`、`1.5G`、`512MiB`，按 1024 进制。

## 父组

`group` 为空时用 worker 自己所在的组，worker 先把自己移进其中的 `__worker` 子组，因为 v2 中有子组的组自身不能有进程。`group` 不为空时使用该组，它必须已存在且没有进程。

## v2：交给 systemd

```ini
# /etc/systemd/system/hustsync-worker.service.d/cgroup.conf
[Service]
Delegate=yes
```

`group` 留空即可。worker 以普通用户运行时，systemd 把服务所在的组交给它管理。

## v1

先建组并交给 worker 用户：

```bash
cgcreate -t hustsync -a hustsync -g memory:hustsync
```

配置 `group = "hustsync"`。

## 测试

测试二进制以 root 运行，编译仍用普通用户：

```bash
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER='sudo -E'   cargo test -p hustsync-worker --test cgroup_root -- --ignored --test-threads=1
```

v1 在 qemu 中测试，脚本自动下载 Ubuntu 24.04 的内核：

```bash
scripts/cgroup-v1-qemu.sh
```
