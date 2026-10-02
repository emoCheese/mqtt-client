# 01 · 架构

> 目标：一页看懂"谁负责什么、谁依赖谁、一条消息从 broker 到业务代码经过哪些层"。

## 1.1 设计目标与约束

| 目标 | 具体含义 | 对架构的硬性影响 |
| --- | --- | --- |
| **以 topic 为核心** | 应用的唯一描述方式是"哪个 filter 交给谁处理"；连接、订阅、重连、退订都不出现在业务代码里 | 必须有独立的**路由层**（目录），且它同时是订阅状态的唯一真相源 |
| **通用** | 支持任意 `Svc` 的成员函数（含 const）与任意可调用对象 | 目录层必须**类型擦除**：只认 `void* + 函数指针 thunk` |
| **零拷贝** | 高频消息不复制 payload | `MessageView` 必须是借用视图 → 引出生命周期管理（见 `05`） |
| **允许异步** | handler 可把消息投递到线程池 | 调用帧 ≠ 工作结束 → 必须引入可传递的保活凭据（见 `05`） |
| **允许回调内再注册** | handler 里 `open()`/`close()` 别的 Topic 不得死锁 | 派发期间**不得持有目录锁**执行用户代码 |

四条约束叠加的结果：路由层必须自带一套显式的生命周期协议。这是本库复杂度的唯一来源。

## 1.2 分层

```
┌─ 用户层 ────────────────────────────────────────────────────────────────┐
│  topic.hpp                                                              │
│    Topic<Svc>        cli, filter, svc, &Svc::method, qos                 │
│    TopicFn           = Topic<std::function<void(const MessageView&)>>    │
│    make_topic(...)   lambda 便捷工厂                                     │
│  职责：把"一个 filter 绑定到一个处理器"表达出来；open/close 幂等；move 自动 rebind │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │ install / uninstall / rebind / publish
┌─ 契约层 ─────────────────────┴──────────────────────────────────────────┐
│  imqtt_client.h                                                         │
│    IMqttClient  { start, stop, is_running, install, uninstall, rebind }  │
│  职责：订阅侧最小契约；不泄漏 paho 细节，便于替换/测试替身                  │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │ 继承实现
┌─ 传输层 ─────────────────────┴──────────────────────────────────────────┐
│  mqtt_client.hpp / mqtt_client.cpp                                      │
│    MqttClient                                                           │
│      · 连接 / 重连 / 会话补齐（on_connected, subscribe_all）              │
│      · 唯一消费线程（consume_loop）+ 事件到消息的翻译                      │
│      · 发布（publish / publish_binary）                                  │
│      · 本地注入 dispatch()（离线测试、手工喂消息）                          │
│  职责：所有"与 broker 打交道"的事；不认识任何业务类型                       │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │ dir_.add / remove / rebind / dispatch
┌─ 路由层 ─────────────────────┴──────────────────────────────────────────┐
│  topic_directory.hpp / topic_directory.cpp   ← 本库的核心与复杂度集中地    │
│    TopicDirectory                                                       │
│      · filter → 多播 handler 列表（dir_）                                 │
│      · slot → filter 反查（by_slot_）                                     │
│      · 通配 filter 索引（wild_filters_）                                  │
│      · 生命周期协议：alive / inflight / async_refs + Lease                │
│  职责：路由 + 引用计数 + 派发期与摘除期的生命周期安全                        │
└───────────────┬─────────────────────────────────┬───────────────────────┘
                │ 匹配规则                          │ 数据契约
┌───────────────┴───────────────┐   ┌─────────────┴───────────────────────┐
│  topic_filter.hpp             │   │  message.hpp                        │
│    is_wildcard_filter()       │   │    MessageView（借用视图）            │
│    topic_matches_filter()     │   │    MessageView::Owned（自持有副本）   │
│    （+ / # / $ 规则，constexpr）│   │    KeepAlive（保活凭据）              │
└───────────────────────────────┘   └─────────────────────────────────────┘
```

## 1.3 依赖关系

```
topic.hpp ──► mqtt_client.hpp ──► topic_directory.hpp ──► topic_filter.hpp
     │                │                   │
     │                │                   └────────────► message.hpp ──► <mqtt/properties.h>
     │                └────────────► <mqtt/async_client.h>
     └────────────► imqtt_client.h ──► message.hpp
```

- 公开头文件直接暴露 paho 类型（`mqtt::async_client`、`mqtt::delivery_token_ptr`、
  `mqtt::properties`），因此 CMake 中 paho 必须是 **PUBLIC** 依赖；
  这是刻意的取舍：避免 pimpl 带来的间接层与分配。
- `message.hpp` 只依赖 `<mqtt/properties.h>`，不依赖 async_client——数据契约保持轻量。
- `TopicDirectory` 认识 `MessageView` 与 `InvokeFn`，但**不认识 paho**：
  消息的具体类型（`mqtt::const_message_ptr`）由传输层塞进 `KeepAlive`（类型擦除）。

## 1.4 端到端数据流

```
        ┌────────────── 用户线程 ──────────────┐
        │ Topic<Svc> t{cli,"robot/+/cmd",...}  │
        │ t.open()  ──► install()              │
        └───────────────┬──────────────────────┘
                        │ ① 注册（filter, thunk, void*）
                        ▼
               ┌──────────────────┐   ② 首次注册该 filter
               │  TopicDirectory  │ ─────────────► SUBSCRIBE(filter, max_qos)
               │  dir_/by_slot_   │
               └────────┬─────────┘
                        │ ③ 消费线程 dispatch(topic, MessageView)
                        ▼
   broker ──► paho 内部线程 ──► consumer queue ──► consume_loop
                                                      │ ④ 组 MessageView + KeepAlive(消息)
                                                      ▼
                                             TopicDirectory::dispatch
                                                      │ ⑤ 精确 + 通配匹配，收集 Snapshot
                                                      │ ⑥ 每个 handler：Lease（async_refs+1）
                                                      ▼
                                             Topic<Svc>::invoke ──► svc.on_x(view)
                                                      │
                     ⑦ 同步返回 → inflight-1          │  ⑦' 异步：pool.post([copy=view])
                     ⑧ 摘除：等 inflight → 等 async_refs │      → 任务结束才释放凭据
```

## 1.5 构建产物与开关

| 目标 | 类型 | 说明 |
| --- | --- | --- |
| `mqtt_client` / `mqttx::mqtt_client` | STATIC 库 | 库本体（源文件：`mqtt_client.cpp`、`topic_directory.cpp`） |
| `mqttx_tests` | 可执行 | 单元测试：filter 规则、目录、Topic、生命周期、并发（不需要 broker） |
| `mqttx_online_test` | 可执行 | 端到端测试客户端（需要 broker，由 python 驱动脚本拉起 mock broker） |
| `mqttx_topic_demo` | 可执行 | 示例：离线派发 / 异步 handler / 在线收发 |

| CMake 选项 | 默认 | 说明 |
| --- | --- | --- |
| `MQTTX_BUILD_TESTS` | ON | 构建单元测试与端到端测试 |
| `MQTTX_BUILD_EXAMPLES` | ON | 构建示例 |
| `MQTTX_SANITIZERS` | 空 | `address,undefined` 或 `thread`；作用于本目录及以下全部目标 |

编译选项：`-Wall -Wextra -Wpedantic -Wshadow -Wnon-virtual-dtor -Woverloaded-virtual`，
公开传播 `cxx_std_20`（依赖 `std::jthread`、`std::atomic::wait/notify`、
`std::erase_if`、concepts、designated initializers）。

## 1.6 与 `mqtt_client0` 的关系

`mqtt_client0/` 是早期实现（`map<topic, std::function>` + 队列 + 单线程派发），
它证明了"topic 回调"这个方向的可用性，但没有解决多播、通配匹配、生命周期与重连补齐。
两者互不依赖，新代码使用 `mqtt_client/`。

## 1.7 明确不做的事

| 不做 | 理由 |
| --- | --- |
| 不做 broker 侧的持久化 / 消息落盘 | 属于 MQTT 会话与 broker 的职责 |
| 不做 QoS2 的完整状态机 | 交给 paho；本库只传递 qos 参数 |
| 不做 TLS / 认证配置封装 | `MqttConfig` 只透传用户名口令；证书等复杂配置需要时再扩 |
| 不做消息分发到多进程 | 单进程内多线程模型足够；跨进程请用 MQTT 本身 |
| 不做 handler 的线程安全保证 | handler 由唯一消费线程调用；它自己派生的异步任务由用户负责同步 |
