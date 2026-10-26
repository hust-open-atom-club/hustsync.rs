# 在容器内同步

依赖复杂的同步脚本可以放进容器，宿主机只需要 docker。

## 启用

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

`[docker] enable` 为真且镜像设置了 `docker_image` 时才进容器，其余镜像照常在宿主机上运行。容器优先于 cgroup。

## 运行方式

每次同步执行一次：

```text
docker run --rm -a STDOUT -a STDERR --name hustsync-job-<镜像名> -w <镜像目录> -u <uid>:<gid>
  -v <卷>... -e <变量名>... [-m <字节数>] <选项>... <镜像> <命令>
```

| 项 | 说明 |
|:--:|:--:|
| 卷 | `[docker] volumes`、`docker_volumes`、`exclude_file` 只读挂载、日志目录、日志文件、镜像目录，后三者宿主与容器路径相同 |
| 环境变量 | 只传变量名，值由 docker 客户端从自身环境读取，口令不出现在进程列表里 |
| 用户 | 与 worker 相同的 uid 和 gid |
| 内存 | `memory_limit` 换算为字节传给 `-m` |
| 停止 | `docker stop -t 2`，再终止 docker 客户端 |
| 收尾 | 等 `--rm` 删除容器，最多 10 秒，避免下次同名冲突 |

## 权限

worker 用户需要能访问 docker，通常是加入 `docker` 组。这等同于宿主机 root 权限，只在可信的 worker 上启用。
