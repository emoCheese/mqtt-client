# mqtt_client 技术文档

本目录是 `mqtt_client` 子项目的**内部技术储备**：面向维护者与二次开发者，
说明架构、数据结构、线程模型、函数调用链与核心机制（生命周期 / 保活）的设计与理由。

面向使用者的快速上手与 API 语义，请看 [`../README.md`](../README.md)。

## 文档索引

| 文档 | 内容 | 什么时候看 |
| --- | --- | --- |
| [`01-architecture.md`](01-architecture.md) | 分层架构、模块职责、依赖关系、端到端数据流、构建产物 | 第一次接触代码库 |
| [`02-data-structures.md`](02-data-structures.md) | 核心数据结构、字段语义、**不变式清单** | 改数据结构 / 排查状态错乱 |
| [`03-thread-model.md`](03-thread-model.md) | 线程角色分工、同步原语、锁序、内存序、死锁防护 | 改并发逻辑 / 写压测 |
| [`04-call-chains.md`](04-call-chains.md) | 8 条完整调用链（启动、注册、派发、摘除、move、重连、发布、异步） | 跟代码 / 定位问题 |
| [`05-lifecycle-keepalive.md`](05-lifecycle-keepalive.md) | 四类生命周期、保活凭据、两段等待不变式、异步契约与边界 | 改生命周期 / 排查悬垂与卡死 |
| [`06-design-tradeoffs.md`](06-design-tradeoffs.md) | 需求→复杂度推导、被否决的简单方案、复杂度账单、裁剪建议 | 评审 / 决定是否简化或替换 |

推荐阅读顺序：`01 → 04 → 05 → 02 → 03 → 06`。
只想改一个 handler 的业务逻辑，读 `04` 的「③ 派发链」与 `05` 的「异步契约」即可。

## 术语表

| 术语 | 含义 |
| --- | --- |
| **topic filter** | 订阅用的过滤器，可含 `+`（单级通配）与 `#`（多级通配），如 `robot/+/cmd` |
| **topic** | 消息实际到达的具体主题，如 `robot/1/cmd`；一条消息会命中**所有**匹配的 filter |
| **handler** | 一次注册对应的处理逻辑：`InvokeFn = void(*)(void*, const MessageView&)` |
| **slot / 句柄** | `install()` 返回的 `uint64_t`，代表"一个 Topic 对象的一次注册" |
| **登记项** | 目录内部的 `Slot` 结构，与 slot 一一对应（含目标指针、两个计数器） |
| **thunk** | 编译期生成的静态跳板 `Topic<Svc>::invoke`，把类型擦除的 `void*` 还原成 `Topic<Svc>*` |
| **借用视图** | `MessageView`：`topic`/`payload`/`props` 指向底层 paho 消息，不拥有数据 |
| **保活凭据** | `KeepAlive`：引用计数句柄，同时持有底层消息与登记项，随 `MessageView` 副本流动 |
| **在飞调用** | 正在执行 `h->fn(...)` 的调用帧，由 `Slot::inflight` 计数 |
| **异步副本** | handler 投递到线程池后仍存活的 `MessageView` 副本，由 `Slot::async_refs` 计数 |
| **会话订阅集合** | `session_subs_`：本进程认为 broker 侧当前已存在的订阅，用于重连时只补差异 |

## 文档约定

- 引用代码一律使用 **文件名 + 符号名**（如 `TopicDirectory::remove`），不写行号——行号会随重构失效；
  相对路径按本目录为基准，如 [`../topic_directory.cpp`](../topic_directory.cpp)。
- 文中"必须/不得"表示**不变式**，破坏它会引入 UAF、死锁或永久阻塞；
  每条机制都尽量给出对应的测试用例（见 [`05`](05-lifecycle-keepalive.md) 末节的可追溯性矩阵）。
- 文档与代码同步维护：改动 [`../topic_directory.cpp`](../topic_directory.cpp)、
  [`../topic_directory.hpp`](../topic_directory.hpp)、[`../message.hpp`](../message.hpp) 中的
  生命周期协议时，必须同步更新 `02`、`03`、`05`。

## 代码地图

```
mqtt_client/
├── topic.hpp                  用户层：Topic<Svc> / TopicFn / make_topic
├── imqtt_client.h             契约层：start/stop/install/uninstall/rebind
├── mqtt_client.hpp/.cpp       传输层：连接、消费线程、订阅补齐、发布、本地注入
├── topic_directory.hpp/.cpp   路由层：filter→handler 目录 + 生命周期协议（本库核心）
├── topic_filter.hpp           规则层：MQTT filter 匹配（+ / # / $）
├── message.hpp                数据层：MessageView / KeepAlive / Owned
├── tests/                     单元测试（自带极简框架，无外部依赖）
│   └── integration/           端到端测试 + 极简 MQTT v5 mock broker（Python）
├── examples/topic_demo.cpp    用法示例（离线 / 在线 / 异步三种）
└── docs/                      本文档目录
```
