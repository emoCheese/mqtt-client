# 07 · 绑定模型：Handler / Binding / Subscription / Publisher

> 目标：说明“一个 topic 绑定到谁”为什么被拆成四层，以及每一层拥有什么、何时释放。
> 本文面向使用者也面向维护者；API 语义以 [`../binding.hpp`](../binding.hpp)、
> [`../handler.hpp`](../handler.hpp)、[`../publisher.hpp`](../publisher.hpp) 为准。

## 7.1 旧模型的问题

旧的 `Topic<Svc, M>` 同时承担了五件事：

1. 描述绑定：`(filter, handler, qos)`；
2. 持有 handler：成员模式的外部 `Svc*`，或可调用模式的 `unique_ptr<Svc>`；
3. 管理注册：`slot`、`open()`、`close()`、SUB/UNSUB；
4. 处理移动：move 之后调用 `rebind`，把目录里的 `void*` 改成新地址；
5. 发布与 RPC：`publish()`、`RpcTopic::request()`。

这带来三个结构性问题：

| 问题 | 旧实现 | 新模型 |
| --- | --- | --- |
| move 需要与目录握手 | 目录存指向 `Topic` 的 `void*`，move 后 `rebind` | 目录的 `Slot` 直接持有 `Handler`，move 只搬 slot |
| moved-from 可访问悬垂目标 | `svc_` 未置空，`service()` 无守卫（ASan 可复现 UAF） | move 后 `cli_`/`svc_` 置空，`service()` 抛 `std::logic_error` |
| 双模式状态机 | `M == nullptr` 哨兵 + `svc_/owned_` 双存储 | `Handler` 内部只在构造时选择一次策略 |

## 7.2 类型一览

| 类型 | 角色 | 是否持有注册 | 生命周期 | 拷贝/移动 |
| --- | --- | --- | --- | --- |
| `Handler` | 类型擦除后的处理器 | 否 | 由 `Binding` / `Slot` 持有 | 可拷贝；move 后源为空 |
| `Binding` | 绑定的纯描述符 `(filter, Handler, qos)` | 否 | 应用决定；无副作用 | 可拷贝 |
| `Subscription` | 一次激活的 RAII 凭据 | 是（slot） | 析构/`close()` 退订 | 只移动 |
| `SubscriptionGroup` | 一组 `Binding` + 一组 `Subscription` | 是（批量） | 组析构关闭全部 | 只移动 |
| `Publisher` | 发布目标 `(topic, qos, retain)` | 否 | 无生命周期语义 | 可拷贝 |
| `Topic<Svc, M>` | 兼容门面 | 通过内部 `Subscription` | 应用决定 | 只移动 |

`Binding` 与 `Subscription` 的关系类似“任务描述”与“运行中的任务句柄”：
**描述可以复制/复用，激活凭据必须唯一**。

## 7.3 Handler

```cpp
// 成员函数：零堆分配
Handler h1 = Handler::member<Robot, &Robot::on_cmd>(robot);
Handler h2 = mqttx::on<&Robot::on_cmd>(robot);       // 语法糖

// 可调用对象：一次分配，callable 内联在 State<F> 中
Handler h3 = Handler::callable([&robot](const MessageView& m) {
    robot.on_cmd(m);
});
```

- const 成员函数与普通成员函数都支持；thunk 内通过 `if constexpr` 选择
  `(const Svc*->*M)(msg)` 或 `(svc->*M)(msg)`。
- 成员模式保存 `Svc*`，**不拥有**业务对象：service 必须长于订阅。
- 可调用模式保存 `shared_ptr<State<F>>`；多个 `Binding`/`Slot` 可共享同一状态。
- `Handler` move 后 `target_`/`fn_` 置空，调用空 Handler 是 no-op；目录注册时会拒绝
  无效 Handler（`std::invalid_argument`）。

## 7.4 Binding：纯描述符

```cpp
auto binding = mqttx::make_binding<&Robot::on_cmd>("robot/+/cmd", robot, 1);

// 同一个描述符可以产生多个独立注册（多播）
auto a = binding.open(cli);
auto b = binding.open(cli);
```

构造期校验并抛 `std::invalid_argument`：

- filter 为空；
- handler 无效；
- QoS 不在 `[-1, 2]`。

`Binding::open(MqttClient&)` 返回 `Subscription`，可能抛 `std::bad_alloc`；
除此之外注册失败以异常表达，不再出现“`bool` 与异常混用”。

**两阶段激活**：构造期不产生注册，适合 Session 成员：

```cpp
class Session {
    RobotService svc_;
    mqttx::Binding cmd_{mqttx::make_binding<&RobotService::on_cmd>(
        "robot/+/cmd", svc_, 1)};
    std::optional<mqttx::Subscription> cmd_sub_;

public:
    explicit Session(mqttx::MqttClient& cli) {
        cmd_sub_.emplace(cmd_.open(cli));  // 所有成员构造完成后
    }
};
```

## 7.5 Subscription：唯一的激活凭据

```cpp
auto sub = binding.open(cli);

sub.active();          // true
sub.filter();          // "robot/+/cmd"
sub.qos();             // 请求值（-1 表示客户端默认）
sub.effective_qos();   // 目录中实际生效的最大 QoS

sub.set_qos(2);        // 目录级更新，slot 不变
sub.close();           // 幂等；最后一个绑定者触发 UNSUBSCRIBE
```

- 构造函数私有，只能由 `Binding::open()` 产生，避免“未绑定但看起来可用”的 token。
- 只移动；move 只交换 `cli_`/`slot_`，**不触碰目录**，不需要 rebind。
- `close()` 后不可复活；需要重新订阅时用原 `Binding` 再 `open()`。
- 析构会等待在飞 handler 与异步 `MessageView` 副本释放（详见 `05`）。

## 7.6 SubscriptionGroup：批量作用域

```cpp
mqttx::SubscriptionGroup group;
group.add(mqttx::make_binding<&Robot::on_cmd>("robot/+/cmd", robot, 1));
group.add(mqttx::make_binding("robot/alarm",
                              [](const MessageView& m) { /* ... */ }, 2));

group.open_all(cli);   // 任一失败会关闭已打开的部分并重新抛出
group.close_all();     // 幂等
// group 析构时自动 close_all()
```

- `add()` 只能在 `open_all()` 之前调用；已打开时添加会抛 `std::logic_error`，
  避免“新绑定存在但未生效”的静默歧义。
- `open_all()` 使用临时 vector + 回滚，保证不留下半开状态。
- 组只移动；move 后源组为空，不会重复退订。
- 适合 Session 聚合、动态插件集合；不需要批量时优先用单个 `Subscription`。

## 7.7 Publisher：发布与绑定解耦

```cpp
mqttx::Publisher pub{cli, "robot/1/cmd", 2, /*retain=*/false};
pub.publish(R"({"v":1})");
pub.publish_binary(data, len);
```

构造期校验：

- topic 非空；
- `!is_wildcard_filter(topic)`（MQTT 规范禁止向 topic name 发送 `+`/`#`）；
- QoS 在 `[-1, 2]`。

`Publisher` 没有析构逻辑，也没有订阅状态，因此不参与 RAII 生命周期。
`Topic::publish()` 仍保留为兼容入口，但会先做同样的通配符检查。

## 7.8 生命周期与线程契约

### 所有权顺序

```
MqttClient  >  Subscription / SubscriptionGroup  >  service 对象
```

- `MqttClient` 必须长于所有订阅；
- 成员声明顺序必须是 `client → service → Subscription/Binding`，逆序析构时
  先关闭订阅并等待在飞 handler，再销毁 service；
- `Binding` 可以先于 `MqttClient` 存在（它不持有 client），但 `open()` 时需要 client。

### 线程

| 操作 | 线程安全性 |
| --- | --- |
| 目录的 `add/remove/set_qos/dispatch` | 内部加锁，线程安全 |
| 同一个 `Subscription` 的 `open/close/set_qos` | **不保证并发安全**，需外部同步或仅在构造期操作 |
| 同一个 `SubscriptionGroup` 的 `open_all/close_all` | 同上 |
| 不同 `Subscription` 的并发操作 | 安全（目录内部同步） |
| `Handler` 的并发调用 | 目录可以在并发 dispatch 下调用同一个 handler；业务对象需自行保证可重入 |

### moved-from

- `Subscription` move 后源对象 `active() == false`，析构不会退订；
- `Topic` move 后 `open()` 返回 false，`publish()`/`client()`/`service()` 抛
  `std::logic_error`；
- `SubscriptionGroup` move 后源组 `opened() == false`。

## 7.9 QoS 语义

目录按 filter 保存所有 handler 的 QoS，`Entry.qos = max(handler.qos)`：

| 操作 | 结果 |
| --- | --- |
| 新 handler 的 QoS 更高 | 提升并重新 SUBSCRIBE |
| 新 handler 的 QoS 更低 | 不改变当前最大值 |
| `Subscription::set_qos` | slot 不变；重算最大值，变化时重新 SUBSCRIBE |
| 摘除 QoS 最大的 handler | 重算剩余最大值；下降时按新 QoS 重新 SUBSCRIBE |
| 摘除最后一个 handler | UNSUBSCRIBE |

- `qos()` 是请求值（`-1` 表示使用 `MqttConfig::qos`）；
- `effective_qos()` 查询目录中实际生效值；
- 发布默认 QoS 来自 `MqttClient`，不会自动继承订阅的 QoS；需要时显式传给 `Publisher`。

## 7.10 迁移对照

| 旧 API | 新 API |
| --- | --- |
| `Topic<Svc, M> t{cli, filter, svc, qos}; t.open();` | `auto sub = subscribe(cli, filter, on<&Svc::M>(svc), qos);` |
| `TopicFn` / `make_topic(cli, filter, lambda, qos)` | `make_binding(filter, lambda, qos)` + `open(cli)`，或直接 `subscribe(...)` |
| `Topic::publish` | `Publisher` |
| `Topic::set_qos` | `Subscription::set_qos`（目录级更新） |
| move 后 `rebind` | 无 rebind；move 只搬 slot |
| `RpcTopic` | 已删除；RPC 需要独立的请求/响应协议对象，不复用绑定门面 |

## 7.11 测试对照

| 机制 | 测试 |
| --- | --- |
| 描述符校验 | `binding_validates_arguments` |
| RAII 退订 / 幂等 | `binding_open_returns_raii_subscription`、`subscription_destructor_unsubscribes` |
| move 不触碰目录 | `subscription_move_preserves_registration`、`topic_move_*_preserves_registration` |
| 描述符复用（多播） | `binding_can_open_multiple_subscriptions` |
| 工厂 / concept | `binding_factories_and_subscribe` |
| QoS 请求值 vs 生效值 | `subscription_effective_qos_and_set_qos` |
| 批量与回滚 | `subscription_group_opens_and_closes_atomically`、`subscription_group_move_transfers_ownership` |
| 发布守卫 | `publisher_validates_topic_and_qos`、`topic_publish_rejects_wildcard_filter` |
| moved-from 惰性 | `topic_moved_from_object_is_inert` |
