# mqtt_client —— 以 topic 为核心的 MQTT 客户端

`mqtt_client` 把 MQTT 的**订阅**抽象成一张“topic 目录”：
应用只描述 *哪个 topic（filter）由谁处理*，其余（连接、重连、订阅补齐、退订、线程、
生命周期）全部由库负责。

```
  Binding / Subscription / Publisher      TopicDirectory              paho.mqtt.cpp
   ┌──────────────────────────┐        ┌──────────────────────┐     ┌───────────────┐
   │ robot/+/cmd → Handler    │ open() │ robot/+/cmd → [h1,h2]│ SUB→ │ async_client  │
   │ robot/#     → λ          │ ─────► │ robot/#     → [h3]   │ ◄─MSG│ consumer queue│
   └──────────────────────────┘ close()└──────────────────────┘     └───────────────┘
        Publisher ── publish ─────────────► MqttClient ──► paho
                                        │ dispatch(topic, msg)
                                        ▼
                              按 MQTT filter 规则匹配后多播给 handler
```

## 目录结构

| 文件 | 作用 |
| --- | --- |
| `handler.hpp` | `Handler`：类型擦除后的处理器；成员函数零分配，可调用对象一次分配 |
| `binding.hpp` | `Binding`（纯描述符）、`Subscription`（RAII 激活凭据）、`SubscriptionGroup`、`make_binding` / `subscribe` |
| `publisher.hpp` | `Publisher`：发布侧值对象，构造期拒绝通配符 topic |
| `topic.hpp` | `Topic<Svc, M>` / `TopicFn` / `make_topic`：兼容门面，内部是 `Binding + Subscription` |
| `mqtt_client.hpp/.cpp` | `MqttClient`：连接、消费线程、订阅补齐、发布、目录级 QoS 更新 |
| `topic_directory.hpp/.cpp` | `TopicDirectory`：topic filter → handler 的目录（多播、引用计数、生命周期安全） |
| `topic_filter.hpp` | MQTT topic filter 匹配规则（`+` / `#` / `$` 规则） |
| `message.hpp` | `MessageView`：消息的借用视图与 `deep_copy()` |
| `imqtt_client.h` | 订阅侧最小契约 |
| `tests/` | 单元测试（自带极简框架，无外部依赖） |
| `tests/integration/` | 端到端测试 + 极简 mock broker（Python，仅标准库） |
| `examples/topic_demo.cpp` | 用法示例（离线 / 绑定 API / 异步 handler / 在线收发） |
| `docs/` | **技术文档**：架构、数据结构、线程模型、调用链、生命周期与保活、设计取舍、绑定模型 |

## 绑定模型

绑定被拆成“描述”和“激活”两层，避免旧的 `Topic` 同时管理 handler 所有权、
slot、move/rebind 与发布：

| 类型 | 角色 | 生命周期 |
| --- | --- | --- |
| `Handler` | 类型擦除后的处理器（成员模式零分配 / 可调用模式一次分配） | 由 `Binding` / 目录 `Slot` 持有 |
| `Binding` | 纯描述符 `(filter, Handler, qos)`；构造期校验，可复用产生多个注册 | 应用决定；无副作用 |
| `Subscription` | 一次激活的 RAII 凭据；析构/`close()` 退订 | 只移动；move 只搬 slot，无 rebind |
| `SubscriptionGroup` | 批量绑定 + 批量激活，`open_all` 失败回滚 | 组析构关闭全部 |
| `Publisher` | 发布目标 `(topic, qos, retain)`，构造期拒绝 `+`/`#` | 无生命周期语义 |

```cpp
auto binding = mqttx::make_binding("robot/+/cmd",
                                   mqttx::on<&Robot::on_cmd>(robot), 1);
auto sub = binding.open(cli);      // 激活；sub 析构即退订

mqttx::SubscriptionGroup group;
group.add(mqttx::make_binding("robot/alarm", [](const mqttx::MessageView&) {}, 2));
group.open_all(cli);               // 批量激活，异常自动回滚

mqttx::Publisher pub{cli, "robot/1/cmd", 1};
pub.publish(payload);              // 发布与订阅完全解耦
```

只需要一步式时用 `subscribe(...)`；需要在 Session 构造期延迟激活时用
`make_binding(...)` + `open()`。完整语义见
[`docs/07-binding-model.md`](docs/07-binding-model.md)。

## 技术文档（维护者向）

[`docs/`](docs/README.md) 目录说明内部实现与设计理由，与本文（使用者向）互补：

| 文档 | 内容 |
| --- | --- |
| [`docs/01-architecture.md`](docs/01-architecture.md) | 分层架构、模块职责、依赖关系、端到端数据流 |
| [`docs/02-data-structures.md`](docs/02-data-structures.md) | 核心数据结构与 **17 条不变式** |
| [`docs/03-thread-model.md`](docs/03-thread-model.md) | 线程角色、同步原语、锁序与内存序、防死锁规则 |
| [`docs/04-call-chains.md`](docs/04-call-chains.md) | 8 条函数调用链 + 错误路径一览 |
| [`docs/05-lifecycle-keepalive.md`](docs/05-lifecycle-keepalive.md) | 生命周期与保活机制、两段等待证明、异步契约、机制↔测试对照 |
| [`docs/06-design-tradeoffs.md`](docs/06-design-tradeoffs.md) | 需求→复杂度推导、被否决的简单方案、裁剪档位、评审清单 |
| [`docs/07-binding-model.md`](docs/07-binding-model.md) | Handler / Binding / Subscription / Group / Publisher 的职责、RAII 边界、QoS 与迁移对照 |


## 快速上手

```cpp
#include "binding.hpp"
#include "publisher.hpp"

struct Robot {
    void on_cmd(const mqttx::MessageView& m)            { /* ... */ }
    void on_status(const mqttx::MessageView& m) const   { /* ... */ }
};

MqttConfig cfg;
cfg.broker_url = "tcp://127.0.0.1:1883";
mqttx::MqttClient cli{cfg};

Robot robot;

// 推荐：描述符 + RAII 凭据；析构 Subscription 即退订
auto cmd = mqttx::subscribe(cli, "robot/+/cmd",
                            mqttx::on<&Robot::on_cmd>(robot), 1);
auto status = mqttx::subscribe(cli, "robot/+/status",
                               mqttx::on<&Robot::on_status>(robot), 0);
auto alarm = mqttx::subscribe(cli, "robot/alarm",
                              [](const mqttx::MessageView& m) { /* lambda */ }, 2);

// 发布是独立的值对象；构造期拒绝含 +/# 的 topic
mqttx::Publisher pub{cli, "robot/1/cmd", 1};

cli.start();                      // 连接 + 启动消费线程（失败抛异常）
pub.publish(R"({"v":1})");
cli.stop();                       // 幂等；析构也会调用
```

需要在 Session 构造期延迟注册时，用两步式（推荐给成员变量）：

```cpp
auto binding = mqttx::make_binding("robot/+/cmd", mqttx::on<&Robot::on_cmd>(robot), 1);
// ... 对象完全构造后
auto subscription = binding.open(cli);
```

需要批量延迟激活时用 `SubscriptionGroup`：

```cpp
mqttx::SubscriptionGroup group;
group.add(mqttx::make_binding<&Robot::on_cmd>("robot/+/cmd", robot, 1));
group.add(mqttx::make_binding("robot/alarm", [](const mqttx::MessageView&) {}, 2));
group.open_all(cli);   // 任一失败回滚；group 析构关闭全部
```

> **为什么要显式 `open()` / `subscribe` 二选一？**
> `Binding` 构造不把 `this` 交给别的线程；必须等对象完全构造后再激活。
> `subscribe(...)` 等价于 `make_binding(...).open(cli)`，适合已完全构造的场景。
> 旧的 `Topic<Svc, M>` 仍可用，内部就是 `Binding + Subscription`。

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
| 某 handler 的 QoS 下降 | 重算最大 QoS，必要时 `resubscribe = true` | 按新的最大 QoS 重新 `SUBSCRIBE` |
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
  因此在 handler 里调用 `Binding::open()` / `Subscription::close()`（甚至关闭自己）都不会死锁；
- 目录的 `Slot` 直接持有 `Handler`，`Subscription` / `Topic` move 只搬运 slot，
  **不存在 rebind**；moved-from 的 `Topic` 是惰性的；
- `Subscription::close()` / 析构会等待该 handler 的**在飞调用返回**，并且继续等待 handler
  派生出去的**异步任务释放消息副本**（见下一节），之后 handler 状态才会被回收；
- `Topic::publish()` / `client()` / `service()` 在被 move 走的对象上调用会抛 `std::logic_error`；
- 需要在 handler 内一次性关闭一组订阅时用 `SubscriptionGroup`；其析构按 RAII 关闭全部；
- 不要在 handler 里析构 `MqttClient` 本身（那会 join 当前线程）。

### 5. 异步 handler（线程池）

`MessageView` 是**借用视图**：它的 `topic` / `payload` / `props` 指向底层 paho 消息。
派发帧返回后消息随时可能被释放，因此**异步使用时必须把视图一起带走**：

```cpp
auto sub = mqttx::subscribe(cli, "robot/+/telemetry", [&pool](const mqttx::MessageView& m) {
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

所以不会再出现“`h->handler(msg)` 早已返回、guard 也析构了，而 handler 还在飞”的悬垂：

```cpp
cli.dispatch("robot/3/telemetry", view);  // 派发帧立刻返回
sub.close();                              // 阻塞到 worker 释放副本（示例中约 150ms）
```

两条使用约束：

1. **不要**把 `MessageView` 拆成裸 `string_view` / 裸指针再投递（那等于放弃凭据）；
   要投递就按值捕获整个视图或 `deep_copy()`。若任务只需要数据副本、但仍希望
   保护业务对象不被提前析构，可以只拷数据 + 单独捕获凭据：

   ```cpp
   pool.post([ka = m.keep_alive, payload = std::string{m.payload}, this] { ... });
   ```

2. 异步任务若要访问业务对象（Session 里的 service），请保证**成员声明顺序是
   「client → service → Subscription/Binding」**，这样 `~Session` 会先析构订阅
   （等待异步任务），再析构 service。示例见 `examples/topic_demo.cpp` 的 `demo_async()`。

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
| `mqttx_tests` | 单元测试：filter 匹配规则、多播、引用计数、QoS 升降级、`Binding`/`Subscription` move 与 RAII、`SubscriptionGroup`、`Publisher`、回调内自析构、异常隔离、并发竞争、**异步 handler 保活/等待语义** |
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
setarch $(uname -m) -R env TSAN_OPTIONS=halt_on_error=1 ctest --test-dir build-tsan --output-on-failure
```

> TSan 构建只注册 `mqttx_tests`：paho 的 `token::on_success5` 在
> `MqttClient::start()` 的 `wait_for` 路径上会触发 TSan 的
> "double lock of a mutex"，属于第三方库内部锁，与本库代码无关。
> ASan/UBSan 构建仍会跑完整的 `mqttx_tests + mqttx_online`。

## 与 `mqtt_client0` 的关系

`mqtt_client0/` 是早期的 topic 回调实现（topic → `std::function`，队列 + 单线程派发），
保留作为参考；新代码请使用 `mqtt_client/`。
