## 2025-11-26

- 数据库：Rust 无 bolt，相似 embedded key-value store 采用 redb
- 数据库 trait 初步迁移

## 更早

- CLI：设置并统一日志与配置传递
- 配置解析：保证 Manager/Worker 的配置解析正确并带测试覆盖
- 公共状态：统一内部共享的状态结构以减少重复和错误
- 管理器初始化：确保配置加载与日志/tracing

