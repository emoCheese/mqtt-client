# 05 · 生命周期与保活机制

> 这是本库最核心、也最容易改错的部分。改动 [`../topic_directory.cpp`](../topic_directory.cpp)
> 或 [`../message.hpp`](../message.hpp) 中任何与 `alive` / `inflight` / `async_refs` / `Lease` /
> `KeepAlive` 相关的代码前，请先读完本文。

## 5.1 问题的提出：四类生命周期天然不同步

| 对象 | 谁拥有 | 何时死亡 | 库能否观测 |
| --- | --- | --- | --- |
| `mqtt::message`（payload/topic/props 的真实载体） | paho + 消费循环里的 `const_message_ptr` | 本轮消费循环迭代结束 | ✅ |
| handler **调用帧** | 消费线程栈 | `h->fn(...)` 返回 | ✅（`inflight`） |
| handler **派生出的异步任务** | 用户线程池 | 未知 | ❌（只能靠用户传递的凭据） |
| `Topic` / 业务 service | 应用 | 应用决定 | ❌（库只持有裸 `void*`） |

`MessageView` 的三个视图字段是**借用**的（零拷贝的代价），目录里的 `obj` 是**裸指针**
（类型擦除与零开销多态的代价）。两者叠加"未知时刻"的异步任务，就必须有一个
**跨线程、可传递、库可观测**的信号来界定"什么时候才算真的没人用了"。

## 5.2 为什么 `inflight` 一个计数器不够

```
[消费线程]  h->fn(obj, msg) ──► pool.post(...) ──► return
                  │                                    │
            inflight = 1                          inflight = 0   ← 调用帧结束
                                                       │
                                       任务仍在飞（库看不见）  │
                                                       ▼
[应用线程]  close()/析构 ──► 看到 inflight==0 ──► 回收登记项 ──► ✗ UAF
```

`inflight` 的语义边界是**调用栈帧**。库无法观测用户线程池，因此不可能在 `inflight`
上表达"任务还没做完"。唯一可行的办法是把**计数权交给数据本身**：
谁还持有消息副本，谁就持有一份引用计数凭据。这就是 `Lease`。

## 5.3 保活凭据的三个组成部分

```
MessageView（交给 handler 的每次调用专属副本）
├── topic / payload / props   借用视图（零拷贝）
└── keep_alive : KeepAlive
        └── shared_ptr<Lease> ──► Lease {
                                      SlotPtr   slot;    ← ① 保"登记项"（目标对象的句柄）
                                      KeepAlive source;  ← ② 保"底层消息"（视图的数据源）
                                  }
                                  async_refs: 构造 +1 / 析构 -1 并 notify
```

- **① 保登记项**：没有它，`remove` 已经 erase 了目录项，最后一份凭据析构时会写已释放的 `Slot`；
  有它，`Slot` 的生命周期自动被凭据延长（`SlotPtr` 是 `shared_ptr`）。
- **② 保底层消息**：没有它，异步任务里读到的是已释放的 payload buffer——
  **比悬垂 `this` 更危险，因为它通常不崩溃，只产生错误数据**。
  有它，"按值拷贝 `MessageView`"这种最自然的写法才成立（不需要强制深拷贝）。

凭据的**双重角色**：

| 角色 | 谁填写 | 作用 |
| --- | --- | --- |
| 输入 | `MqttClient::consume_loop` 填入 `const_message_ptr` | 让视图副本能延长消息寿命 |
| 输出 | `TopicDirectory::dispatch` 为每次 handler 调用换入新凭据 | 把"该 handler 的登记项"也绑定到这份副本上 |

因此用户侧**不需要写任何样板代码**：`[copy = m] { ... }` 即声明"我还在使用"。

## 5.4 两段等待：不变式与证明

```
remove(slot):
    unique_lock(mtx_)
        alive = false                    ← (a) 先失效
        erase(dir_ / by_slot_)           ← (b) 再摘除
    unlock
    wait_until_zero(inflight)            ← (c) 同步调用帧
    wait_until_zero(async_refs)          ← (d) 异步副本
```

**断言**：只要 (c) 返回，(d) 观察到的计数就是完整的。

证明依赖三条事实：

1. `dispatch` 的"检查 `alive` + `inflight++`"发生在 **shared_lock** 内，(a)(b) 发生在
   **unique_lock** 内 ⇒ 两者互斥 ⇒ 任何被收进批次的 handler，其 `inflight` 计数在
   (b) 之前就已对 `remove` 可见（不变式 I6）；
2. `Lease` 的构造（`async_refs++`）发生在 `dispatch` **仍持有该 slot 的 `inflight`** 的窗口内
   （不变式 I8）⇒ (c) 归零时，该 slot 的所有凭据都已创建完毕；
3. (b) 之后不可能再有新批次包含该 slot（新的批次需要 shared_lock，且 `dir_` 里已无此项）
   ⇒ 不会有"迟到的" `async_refs++`。

**若把 (c)(d) 调换顺序**，就会出现：`async_refs` 此刻为 0 → `remove` 返回 → 对象析构 →
另一个仍在批次中的线程才执行 `Lease` 构造并调用 handler ⇒ UAF。

> 相关用例：`directory_remove_waits_for_inflight_dispatch`、
> `directory_remove_waits_for_async_view_copy`。

## 5.5 异步契约（用户侧规则）

### 三种正确写法

```cpp
// A. 按值捕获整个视图（推荐，零额外拷贝）
pool.post([copy = m] { use(copy.topic, copy.payload); });

// B. 先深拷贝再投递（数据自成一份，同时保留凭据）
auto owned = m.deep_copy();
pool.post([owned] { use(owned.topic, owned.payload); });

// C. 只拷数据 + 显式捕获凭据（只想延长登记项寿命时）
pool.post([ka = m.keep_alive, payload = std::string{m.payload}, this] { use(payload); });
```

### 反例（都会在压力下变成 UAF 或数据错乱）

| 反例 | 后果 |
| --- | --- |
| `pool.post([&m]{ ... })` 捕获引用 | 调用帧一结束 `m` 即析构，捕获的是悬垂引用 |
| `auto v = m.payload; pool.post([v]{...})` | `string_view` 指向的消息可能已释放 → 读已释放内存 |
| `pool.post([this]{ ... })` 不捕获任何视图 | 没有凭据 → `close()` 不等 → `this` 悬垂 |
| 把 `MessageView` 存进长生命周期容器且永不释放 | `close()` 会一直等（见 5.8）——需要显式 `keep_alive.reset()` |

### `deep_copy()` 的语义

`Owned` 里的 `topic` / `payload` / `props` 都是**自己的一份**，因此：

- 只要 `Owned` 活着，数据永远有效（与凭据无关）；
- `Owned` 仍带 `keep_alive`，用于**保护登记项/业务对象**；
- 若你只是想把数据缓存下来、不想因此拖住 `Topic` 的析构，
  可以显式 `owned.keep_alive.reset()`（数据不受影响，因为它本来就是自己的）。

## 5.6 自等豁免（R3）与它的代价

`remove` 若发现 `t_active_slot == 目标 slot`（"在回调里析构自己"），**跳过两段等待**。

- **为什么必须豁免**：此时调用帧就是当前线程自己，等待等于自等 → 必然死锁；
- **代价**：该场景下不提供异步保护。这在语义上本就自相矛盾
  （"我正在销毁这个 Topic，同时我派生的任务还要用它"）。库选择"宁可不管，也绝不死锁"；
- 注意：豁免只影响**该 slot**；同一批次里其它 slot 的计数照常归还。

> 相关用例：`directory_self_removal_inside_handler_does_not_deadlock`。

## 5.7 Session 成员顺序约定（容易踩的坑）

```
class Session {
    RobotService            svc_;      // ① 先声明 → 后析构
    mqttx::Topic<RobotService> cmd_;   // ② 后声明 → 先析构（会等待异步任务）
    mqttx::Topic<RobotService> st_;
};
```

C++ 的析构顺序与声明顺序相反：`~Session` 先析构 `cmd_/st_`（`remove` 等待异步任务释放凭据），
再析构 `svc_`。**顺序反了就会出现"Topic 已等到异步完成、但 service 早被销毁"的假安全。**

> 示例见 [`../examples/topic_demo.cpp`](../examples/topic_demo.cpp) 的 `RobotSession` 与 `demo_async()`。

## 5.8 边界与残余风险（诚实清单）

| 边界 | 说明 | 应对 |
| --- | --- | --- |
| **协作式** | 不捕获凭据就没有保护，库无法强制 | 代码评审 + 本节反例清单；必要时把"必须捕获副本"写进团队规范 |
| **teardown 可能被长任务拖住** | 异步任务持有凭据期间，`close()`/析构会阻塞 | 让任务有明确上界；或在不需要保护时用 `keep_alive.reset()` |
| **消息被延长持有** | 凭据持有 `const_message_ptr`，异步期间消息不会归还 paho | 正常（这正是目的）；避免把视图长期缓存 |
| **自析构 + 异步** | 5.6 的豁免场景无保护 | 不要写这种代码；需要就改成"先投递、再让别的线程 close" |
| **同一 `Topic` 跨线程 move** | `Topic` 是普通对象，move 不是原子的 | 由调用方保证：move 只在单线程进行 |
| **重启客户端** | handler 内 `stop()` 后立刻 `start()` 可能触发自 join | 见 [`03-thread-model.md`](03-thread-model.md) §3.7 |
| **paho 版本依赖** | 消费事件 API 在 paho ≥1.5 才是指针重载 | 升级 paho 时复查 `consume_loop` 的事件处理分支 |

## 5.9 成本模型

| 场景 | 额外成本 |
| --- | --- |
| 同步 handler，未捕获副本 | 每 handler 一次 `make_shared<Lease>` + 4~6 次原子操作，调用结束即归还，**无等待** |
| 异步 handler，捕获了副本 | 同上 + 每个存活副本占一份引用计数；`close()` 需要等待任务结束 |
| 完全不使用异步 | 可以把协议裁掉（见 [`06-design-tradeoffs.md`](06-design-tradeoffs.md) §6.5） |

## 5.10 可追溯性矩阵（机制 ↔ 测试）

| 机制 / 不变式 | 覆盖用例 | 备注 |
| --- | --- | --- |
| filter 规则（`+` / `#` / `$`）| `filter_*`（[`../tests/test_topic_filter.cpp`](../tests/test_topic_filter.cpp)） | 对照 MQTT 规范用例 |
| 多播 / 引用计数 / QoS 取最大 | `directory_multicast_same_topic`、`directory_qos_is_max_of_handlers` | |
| 通配路由 | `directory_routes_by_topic_filter` | 精确 + `+` + `#` 同时命中 |
| 摘除等待在飞调用（I6/I7-c） | `directory_remove_waits_for_inflight_dispatch` | 50ms 未返回即判定仍在等待 |
| 摘除等待异步副本（I7-d/I8） | `directory_remove_waits_for_async_view_copy` | 复现"调用已返回、异步还在飞" |
| 异步数据保活（I9/I10） | `directory_async_view_keeps_message_data_alive`、`topic_close_waits_for_async_handler_copy` | 后者走真实 paho 消息 |
| 同步路径零等待 | `sync_handler_leaves_nothing_to_wait_for` | 防止"过度等待"回归 |
| 自等豁免（R3） | `directory_self_removal_inside_handler_does_not_deadlock` | 无豁免会挂死，由 ctest 超时兜底 |
| 异常隔离 | `directory_handler_exception_is_isolated` | 抛异常的 handler 不影响同批 |
| 并发 add/remove/dispatch | `directory_concurrent_add_remove_dispatch` | ASan / TSan 下运行 |
| move 后 rebind（I15） | `topic_move_ctor_rebinds_registration`、`topic_move_assignment_rebinds_registration`、`topic_moved_from_object_is_inert` | |
| 重连只补差异订阅（I12） | 端到端测试的 `SUBSCRIBE` 次数断言（`mock_broker.py` 日志） | 首次连接全量、连接事件不重复订阅 |
| 端到端（真实 socket + 消费线程） | `mqttx_online`（[`../tests/integration/test_online.cpp`](../tests/integration/test_online.cpp)） | 含异步 handler 读回载荷 |

跑法：

```bash
ctest --test-dir build --output-on-failure                                   # 单元 + 端到端
cmake -S . -B build-asan -DMQTTX_SANITIZERS=address,undefined && ctest --test-dir build-asan   # UAF / UB
cmake -S . -B build-tsan -DMQTTX_SANITIZERS=thread && setarch $(uname -m) -R ctest --test-dir build-tsan  # 数据竞争
```
