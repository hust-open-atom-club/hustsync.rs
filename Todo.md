## 相关历史工作参考 [Timeline.md](./Timeline.md)

未发行，暂无 Changelog 编写

## TODO


Rust 中没有 bolt，相近选用 Redb。继续 Trait 迁移，实现 Redb Adapter 内部实现。

预期先实现
```
debug = false

[server]
addr = "127.0.0.1"
port = 12345
ssl_cert = ""
ssl_key = ""

[files]
db_type = "redb"
db_file = "/tmp/tunasync/manager.db"
ca_cert = ""
```

的数据库创建和相关简易测试

### Next Line?

暂定 2025-11-28

预期 Redb 完成简易测试
