# mqtt_client —— 以 topic 为核心的 MQTT 客户端

`mqtt_client` 把 MQTT 的**订阅**抽象成一张“topic 目录”：
应用只描述 *哪个 topic（filter）由谁处理*，其余（连接、重连、订阅补齐、退订、线程、
生命周期）全部由库负责。

```
     Topic<Svc, M> / TopicFn         TopicDirectory                paho.mqtt.cpp
   ┌────────────────────┐        ┌──────────────────────┐       ┌───────────────┐
   │ robot/+/cmd → svc  │ open() │ robot/+/cmd → [h1,h2]│ SUB→  │ async_client  │
   │ robot/#     → λ    │ ─────► │ robot/#     → [h3]   │ ◄─MSG │ consumer queue│
   └────────────────────┘ close()└──────────────────────┘       └───────────────┘
                                        │ dispatch(topic, msg)
                                        ▼
                              按 MQTT filter 规则匹配后多播给 handler
```

## 目录结构

| 文件 | 作用 |
| --- | --- |
| `topic.hpp` | `Topic<Svc, M>` / `TopicFn`：把一个 topic filter 绑定到成员函数或可调用对象 |
| `mqtt_client.hpp/.cpp` | `MqttClient`：连接、消费线程、订阅补齐、发布 |
| `topic_directory.hpp/.cpp` | `TopicDirectory`：topic filter → handler 的目录（多播、引用计数、生命周期安全） |
| `topic_filter.hpp` | MQTT topic filter 匹配规则（`+` / `#` / `$` 规则） |
| `message.hpp` | `MessageView`：消息的借用视图与 `deep_copy()` |
| `imqtt_client.h` | 订阅侧最小契约 |
| `tests/` | 单元测试（自带极简框架，无外部依赖） |
| `tests/integration/` | 端到端测试 + 极简 mock broker（Python，仅标准库） |
| `examples/topic_demo.cpp` | 用法示例（离线派发 + 异步 handler + 在线收发） |
| `docs/` | **技术文档**：架构、数据结构、线程模型、调用链、生命周期与保活、设计取舍 |

## 技术文档（维护者向）

[`docs/`](docs/README.md) 目录说明内部实现与设计理由，与本文（使用者向）互补：

| 文档 | 内容 |
| --- | --- |
| [`docs/01-architecture.md`](docs/01-architecture.md) | 分层架构、模块职责、依赖关系、端到端数据流 |
| [`docs/02-data-structures.md`](docs/02-data-structures.md) | 核心数据结构与 **15 条不变式** |
| [`docs/03-thread-model.md`](docs/03-thread-model.md) | 线程角色、同步原语、锁序与内存序、防死锁规则 |
| [`docs/04-call-chains.md`](docs/04-call-chains.md) | 8 条函数调用链 + 错误路径一览 |
| [`docs/05-lifecycle-keepalive.md`](docs/05-lifecycle-keepalive.md) | 生命周期与保活机制、两段等待证明、异步契约、机制↔测试对照 |
| [`docs/06-design-tradeoffs.md`](docs/06-design-tradeoffs.md) | 需求→复杂度推导、被否决的简单方案、裁剪档位、评审清单 |


## 快速上手

```cpp
#include "mqtt_client.hpp"
#include "topic.hpp"

struct Robot {
    void on_cmd(const mqttx::MessageView& m)            { /* ... */ }
    void on_status(const mqttx::MessageView& m) const   { /* ... */ }
};

MqttConfig cfg;
cfg.broker_url = "tcp://127.0.0.1:1883";
mqttx::MqttClient cli{cfg};

Robot robot;
mqttx::Topic<Robot, &Robot::on_cmd>    cmd{cli, "robot/+/cmd", robot, /*qos=*/1};
mqttx::Topic<Robot, &Robot::on_status> status{cli, "robot/+/status", robot, 0};
auto alarm = mqttx::make_topic(cli, "robot/alarm", [](const mqttx::MessageView& m) {
    // lambda 形式
});

cmd.open();      // 显式注册；析构自动 close()
status.open();
alarm.open();

cli.start();                      // 连接 + 启动消费线程（失败抛异常）
cli.publish("robot/1/cmd", R"({"v":1})");
cli.stop();                       // 幂等；析构也会调用
```

> **为什么要显式 `open()`？**
> `Topic` 构造期间不把 `this` 交给别的线程，必须等对象完全构造后再注册。
> 典型写法是在 Session 构造体末尾统一 `open()`（见 `examples/topic_demo.cpp`）。

## 核心语义

### 1. topic（filter）是唯一的路由键

- 订阅、注册、派发、退订全部以 topic filter 为键；
- 同一个 filter 可以绑定多个 handler（**多播**），互不干扰；
- 派发时按 MQTT 规则匹配：具体 topic 会命中**所有**与之匹配的 filter
  （`robot/+/cmd`、`robot/#`、`robot/1/cmd` 可以同时收到同一条消息）。

匹配规则见 `topic_filter.hpp`：`+` 匹配一级，`#` 匹配父级及之后所有级，
`$` 开头的系统 topic 不被以通配符开头的 filter 命中。

### 2. 引用计数与订阅推导

`TopicDirectory` 按 filter 引用计数，`MqttClient` 据此自动收发订阅报文：

| 事件 | 目录行为 | 网络行为 |
| --- | --- | --- |
| 首次注册某 filter | `subscribe = true` | `SUBSCRIBE` |
| 再次注册同一 filter | 只增加 handler | 不重复订阅 |
| 某 handler 的 QoS 提升 | `subscribe = true` | 按更高 QoS 重新 `SUBSCRIBE` |
| 摘除最后一个 handler | `unsubscribe = true` | `UNSUBSCRIBE` |

### 3. 连接与重连

- `start()` 先 `start_consuming()` 再 `connect()`，避免连接建立后丢消息；
- 连接失败会**回滚** `running_` 状态并抛异常，不会留下半死不活的对象；
- 自动重连成功后按会话状态补齐订阅：
  - `session_present == false`：会话是新的，清空记录并全量订阅；
  - `session_present == true`：只补订阅本进程内新增的 topic
    （首次连接时无从得知 broker 侧集合，仍然全量订阅，宁可多收一次 retained，也不漏订阅）。

### 4. 生命周期与线程安全

- **派发线程只有一个**（消费线程），`dispatch()` 不持有目录锁调用用户代码，
  因此在 handler 里 `open()` / `close()` 别的 Topic（甚至自己）都不会死锁；
- 同一个 `Topic<Svc, M>` 被 move 之后会自动 `rebind`，目录里的指针始终指向存活对象；
- `close()` / 析构会等待该 handler 的**在飞调用返回**，并且继续等待 handler
  派生出去的**异步任务释放消息副本**（见下一节），之后目标对象才会被回收；
- `Topic::publish()` / `client()` 在被 move 走的对象上调用会抛 `std::logic_error`；
- 不要在 handler 里析构 `MqttClient` 本身（那会 join 当前线程）。

### 5. 异步 handler（线程池）

`MessageView` 是**借用视图**：它的 `topic` / `payload` / `props` 指向底层 paho 消息。
派发帧返回后消息随时可能被释放，因此**异步使用时必须把视图一起带走**：

```cpp
auto topic = mqttx::make_topic(cli, "robot/+/telemetry", [&pool](const mqttx::MessageView& m) {
    pool.post([copy = m] {                 // 按值捕获整个 MessageView
        use(copy.topic, copy.payload);     // 任务执行期间数据一直有效
    });
}, 0);
```

库为**每一次 handler 调用**准备一份保活凭据（`MessageView::keep_alive`，内部是
引用计数句柄，同时持有底层消息与登记项）。于是：

| 场景 | 行为 |
| --- | --- |
| 同步 handler，不保留副本 | 凭据随调用结束释放，`close()` / 析构**零等待** |
| 异步任务按值捕获了视图 | 任务存活期间消息数据有效；`close()` / 析构会**等到任务释放副本**才回收目标对象 |
| 先 `deep_copy()` 再投递 | `Owned` 自带字符串副本，同时保留凭据，同样安全 |

所以不会再出现“`h->fn(obj, msg)` 早已返回、guard 也析构了，而 handler 还在飞”的悬垂：

```cpp
cli.dispatch("robot/3/telemetry", view);  // 派发帧立刻返回
topic.close();                            // 阻塞到 worker 释放副本（示例中约 150ms）
```

两条使用约束：

1. **不要**把 `MessageView` 拆成裸 `string_view` / 裸指针再投递（那等于放弃凭据）；
   要投递就按值捕获整个视图或 `deep_copy()`。若任务只需要数据副本、但仍希望
   保护业务对象不被提前析构，可以只拷数据 + 单独捕获凭据：

   ```cpp
   pool.post([ka = m.keep_alive, payload = std::string{m.payload}, this] { ... });
   ```

2. 异步任务若要访问业务对象（Session 里的 service），请保证**成员声明顺序是
   「service 在前、Topic 在后」**，这样 `~Session` 会先析构 Topic（等待异步任务），
   再析构 service。示例见 `examples/topic_demo.cpp` 的 `demo_async()`。

代价：每次 handler 调用会有一次小的凭据分配（百纳秒量级），同步路径用完即释放，
不产生额外等待。

## 构建与测试

```bash
cmake -S . -B build -G Ninja
cmake --build build
ctest --test-dir build --output-on-failure
./build/mqtt_client/examples/mqttx_topic_demo                      # 离线演示
./build/mqtt_client/examples/mqttx_topic_demo tcp://127.0.0.1:1883 # 在线演示
```

测试分两层，都不需要外部 broker：

| 测试 | 内容 |
| --- | --- |
| `mqttx_tests` | 单元测试：filter 匹配规则、多播、引用计数、QoS、move/rebind、回调内自析构、异常隔离、并发竞争、**异步 handler 保活/等待语义** |
| `mqttx_online` | 端到端测试：由 `tests/integration/mock_broker.py`（纯标准库实现的极简 MQTT v5 broker）驱动，验证 连接 → 订阅 → 发布 → 回环 → 按 filter 派发 → 异步任务读取 → 退订 的完整链路 |

也可以手工跑端到端测试：

```bash
python3 mqtt_client/tests/integration/mock_broker.py 18830 &
./build/mqtt_client/tests/mqttx_online_test tcp://127.0.0.1:18830
```

可选开关：

| 选项 | 说明 |
| --- | --- |
| `-DMQTTX_BUILD_TESTS=OFF` | 不构建单元测试 |
| `-DMQTTX_BUILD_EXAMPLES=OFF` | 不构建示例 |
| `-DMQTTX_SANITIZERS=address,undefined` | ASan + UBSan |
| `-DMQTTX_SANITIZERS=thread` | TSan（数据竞争） |

```bash
cmake -S . -B build-asan -DMQTTX_SANITIZERS=address,undefined && cmake --build build-asan
ctest --test-dir build-asan --output-on-failure

# TSan 在新内核上若报 "unexpected memory mapping"，需关闭 ASLR：
cmake -S . -B build-tsan -DMQTTX_SANITIZERS=thread && cmake --build build-tsan
setarch $(uname -m) -R env TSAN_OPTIONS=halt_on_error=0 ctest --test-dir build-tsan --output-on-failure
```

## 与 `mqtt_client0` 的关系

`mqtt_client0/` 是早期的 topic 回调实现（topic → `std::function`，队列 + 单线程派发），
保留作为参考；新代码请使用 `mqtt_client/`。
