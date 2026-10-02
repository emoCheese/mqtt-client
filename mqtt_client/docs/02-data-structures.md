# 02 · 核心数据结构与不变式

> 目标：把"状态放在哪里、谁拥有它、什么时刻它一定成立"讲清楚。
> 破坏本文的不变式 ⇒ UAF、死锁或永久阻塞。

## 2.1 所有权一览

| 结构 | 拥有者 | 生命周期 | 备注 |
| --- | --- | --- | --- |
| `MqttClient` | 应用 | 最长（不要晚于其 Session/service） | 不可拷贝不可移动 |
| `Topic<Svc>` | 应用（通常作为 Session 成员） | 由应用决定；析构即摘除注册 | 可 move，不可拷贝 |
| 业务 service 对象 | 应用 | **必须长于**引用它的 Topic（见 `05` 成员顺序约定） | 库不持有 |
| `Slot` | `TopicDirectory`（`by_slot_` + `dir_` 各持一份 `shared_ptr`） | 最后一个引用消失时 | 也被 `Lease` 持有 |
| `Lease` | 由 handler 调用产生的 `MessageView` 副本共享 | 最后一份副本消失时 | 每次调用一份 |
| `mqtt::message` | paho 与 `KeepAlive` | 最后一个 `const_message_ptr` 消失时 | 视图借用它的内部 buffer |
| `mqtt::async_client` | `MqttClient::inner_` | 与 `MqttClient` 同寿 | paho 自己管理其内部线程 |

## 2.2 传输层：`MqttConfig` / `MqttClient`

```cpp
struct MqttConfig {                    // mqtt_client.hpp
    std::string broker_url;            // 空 ⇒ 构造抛 std::invalid_argument
    std::string client_id;             // 空 ⇒ 自动生成 mqttx-<12 hex>
    std::string username, password;    // 非空才写入 connect options
    int  qos{1};                       // 默认 QoS：install/publish 传 -1 时生效
    bool clean_session{false};         // MQTT v5 的 clean_start
    int  keep_alive_sec{30};
    std::chrono::milliseconds reconnect_min{1000};
    std::chrono::milliseconds reconnect_max{30000};
    std::chrono::milliseconds poll_timeout{100};     // 消费线程单次事件等待上限
    std::chrono::milliseconds connect_timeout{5000}; // start() 等待连接的上限
};
```

| 成员 | 类型 | 语义 |
| --- | --- | --- |
| `cfg_` | `MqttConfig` | 构造后不可变（`client_id` 缺省时在构造期补齐） |
| `inner_` | `unique_ptr<mqtt::async_client>` | paho 句柄；所有网络动作的唯一出口 |
| `consumer_` | `std::jthread` | **唯一**派发线程；`stop_token` 驱动退出 |
| `running_` | `atomic<bool>` | 生命周期闸门：`start` 抢到 true 才继续；`stop` 抢到 false 才执行清理 |
| `dir_` | `TopicDirectory` | 路由与生命周期协议的全部状态 |
| `session_mtx_` | `std::mutex` | 保护下面两项 |
| `session_subs_` | `unordered_set<string, SvHash, SvEqual>` | 本会话 broker 侧**已有**的订阅（重连只补差异） |
| `connected_once_` | `bool` | 本进程是否成功连过一次；决定 `session_subs_` 是否可信 |

## 2.3 路由层：`TopicDirectory`

```cpp
struct Slot {                          // 一次 install = 一个 Slot（目录内部）
    std::uint64_t      slot;           // 句柄 id，单调递增，永不复用
    std::atomic<void*> obj;            // 目标对象（Topic 句柄本体）；move 时被 rebind 改写
    InvokeFn           fn;             // 编译期 thunk：void(*)(void*, const MessageView&)
    std::atomic<bool>  alive;          // false ⇒ 已被摘除，同批后续不再进入
    std::atomic<int>   inflight;       // 正在执行 h->fn(...) 的调用帧数
    std::atomic<int>   async_refs;     // 外部仍持有的保活凭据份数（= 存活的 Lease 数）
};

struct Entry { int qos; std::vector<SlotPtr> handlers; };   // 一个 filter 一份，支持多播
struct Record { std::string topic; SlotPtr slot; };         // slot → filter 反查

using SlotPtr = std::shared_ptr<Slot>;

std::shared_mutex         mtx_;           // 保护下面三个容器
DirMap                    dir_;           // filter → Entry（透明哈希，string_view 可查）
std::vector<std::string>  wild_filters_;  // 含 '+' / '#' 的 filter（派发时需扫描）
SlotMap                   by_slot_;       // uint64 → Record
std::atomic<std::uint64_t> next_slot_{1};
```

```cpp
struct Lease {                         // 定义在 topic_directory.cpp（头文件只前置声明）
    SlotPtr   slot;                    // ① 保登记项本身：摘除后本对象仍可安全析构
    KeepAlive source;                  // ② 保底层消息：让借用视图在异步期间有效
    Lease(...)  { slot->async_refs.fetch_add(1, acq_rel); }
    ~Lease()    { if (slot->async_refs.fetch_sub(1, acq_rel) == 1) notify_all(); }
    // 不可拷贝：只以 shared_ptr<Lease> 形式被共享
};
```

为什么是**两个**容器而不是一个：`dir_` 负责"匹配"（按 filter 找 handler），
`by_slot_` 负责"反查"（按句柄找 filter）。两者在 `mtx_` 的同一个临界区内同步更新。

为什么 `wild_filters_` 单独存一份：绝大多数应用只注册少量通配 filter，
把"需要做规则匹配的 filter"与"可以 O(1) 精确命中的 filter"分开，
使常见路径（具体 filter）零额外开销。

## 2.4 数据层：`MessageView` / `KeepAlive`

```cpp
class KeepAlive {                      // 类型擦除的引用计数句柄
    std::shared_ptr<const void> token_;
public:
    bool valid() const;                // 是否持有有效凭据
    void reset() noexcept;             // 提前释放
    long use_count() const;            // 调试用
};

struct MessageView {
    std::string_view        topic;      // 借用：指向 mqtt::message 内部
    std::string_view        payload;    // 借用：可能是二进制
    const mqtt::properties* props;      // 借用：指向 mqtt::message 内部
    int  qos; bool retain;
    KeepAlive keep_alive;               // 由库在派发时填写（见下）

    struct Owned {                      // deep_copy() 的结果：字符串自持有 + 仍带凭据
        std::string topic, payload; mqtt::properties props; int qos; bool retain; KeepAlive keep_alive;
        MessageView view() const;       // 指向自己的成员（本对象移动/析构后失效）
    };
    Owned deep_copy() const;
};
```

`keep_alive` 的双重角色：

1. **输入**：调用方（`MqttClient::consume_loop`）把自己掌握的数据寿命塞进去
   （实际是 `mqtt::const_message_ptr`），使视图副本能延长消息寿命；
2. **输出**：`TopicDirectory::dispatch` 为每次 handler 调用**换入新的凭据**
   （持有消息 + 该 handler 的 `Slot`），于是"拷贝视图"就自动等价于"声明我还在使用"。

## 2.5 用户层：`Topic<Svc>`

```cpp
MqttClient*   cli_;        // nullptr ⇒ 已被 move（此时 open/publish/client 会拒绝或抛错）
std::string   topic_;      // topic filter（可含 + / #）
Svc*          svc_;        // 处理者：成员函数模式下指向外部对象；拥有型模式下指向 owned_
unique_ptr<Svc> owned_;    // 仅"可调用对象"模式使用
Method        method_;     // void (Svc::*)(const MessageView&)
ConstMethod   cmethod_;    // void (Svc::*)(const MessageView&) const
int           qos_;        // -1 ⇒ 用 MqttClient 的默认 QoS
uint64_t      slot_;       // 0 ⇒ 未注册
```

三种处理器形态在 `invoke()` 里分支：非 const 成员函数 → const 成员函数 → 拥有型可调用对象
（第三支用 `if constexpr (std::is_invocable_v<Svc&, const MessageView&>)` 做编译期裁剪，
因此 `Svc` 不可调用时不会产生错误代码）。

## 2.6 不变式清单

> 标注 **[锁]** 的条目表示"在 `mtx_` 的保护下成立"；其余为无锁/跨线程必须始终成立的约束。

| 编号 | 不变式 | 破坏后果 |
| --- | --- | --- |
| **I1** | **[锁]** `dir_` 中每个 `Entry.handlers` 里的 `SlotPtr` 非空，且同一个 `Slot` 只属于一个 `Entry` | 同一 handler 被重复调用 / 计数错乱 |
| **I2** | **[锁]** `by_slot_[id]` 存在 ⟺ 对应 `Slot` 在 `dir_[Record.topic].handlers` 中 | `remove` 找不到目标 → 泄漏或 UAF |
| **I3** | **[锁]** `wild_filters_` 无重复，且 `f ∈ wild_filters_` ⟺ `is_wildcard_filter(f) && dir_.count(f)` | 通配 filter 漏派发或重复派发 |
| **I4** | **[锁]** `dir_[f].qos == max(该 filter 下所有 handler 的注册 qos)`，且只增不减直到该 filter 清空 | 订阅 QoS 低于 handler 期望 |
| **I5** | `alive == false` ⇒ 该 slot 已不在 `dir_`/`by_slot_` 中（摘除时先在锁内 `alive=false` 再 erase） | 同批派发进入已摘除的 handler |
| **I6** | 被 `dispatch` 收进批次的 slot，其 `inflight` 自增**先于** `remove` 的 erase 可见（两者都在各自锁内完成） | `remove` 提前返回 → UAF |
| **I7** | `remove` 只在 `inflight == 0 && async_refs == 0` 时返回（自等豁免见 I10） | 目标对象在异步任务仍在使用时被析构 |
| **I8** | `async_refs` == 存活 `Lease` 份数；`Lease` 只在 `inflight > 0` 的窗口内构造 | 异步任务持有的副本不被计入 → I7 失效 |
| **I9** | 只要存在一份 `KeepAlive` 副本，其 `Lease` 就同时持有 `SlotPtr` 与底层消息 | 借用视图悬垂（数据错乱）/ 凭据析构踩已释放内存 |
| **I10** | `MessageView` 的 `topic/payload/props` 只在①同步调用期间，或②该副本的 `keep_alive` 存活期间有效 | 异步任务读到已释放内存 |
| **I11** | `next_slot_` 单调递增，slot id 永不复用 | 陈旧句柄误命中新注册 |
| **I12** | **[session_mtx_]** `session_subs_ ⊆ {dir_ 中的 filter}`；只在本进程成功连过之后才作为"可信差异基线" | 漏订阅或重复订阅（重复触发 retained） |
| **I13** | 生产路径中 `dispatch` 仅由消费线程调用（`TopicDirectory` 本身支持并发 dispatch，但 `MqttClient` 不这么做） | 不再是"单派发者"，`t_active_slot` 语义变复杂 |
| **I14** | 调用 `h->fn(...)` 时**不得**持有 `mtx_` 或 `session_mtx_` | 回调内注册/摘除死锁 |
| **I15** | `Topic::slot_ != 0` ⟺ 已注册；`cli_ == nullptr` ⟺ 已被 move | move 后误用 → 空指针 / 双重摘除 |

## 2.7 已知开销与可选优化（技术储备）

单条消息、单次派发的分配与同步成本：

| 项 | 次数 | 说明 |
| --- | --- | --- |
| `std::vector<SlotPtr> batch` 扩容 | 每条消息 1 次 | 可改为"消费线程私有 + 复用"的 `thread_local` 缓冲（I13 保证只有一个派发者时安全） |
| `make_shared<Lease>` | **每个 handler 调用 1 次** | 保活协议的代价；若明确禁止异步，可整段删除（见 `06` 的裁剪建议） |
| `atomic` RMW | 每 handler 4~6 次 | `alive` 读、`inflight` ±1、`async_refs` ±1 |
| `shared_ptr` 引用计数 | 每 handler 数次 | 快照 + 凭据共享 |
| 锁 | 每条消息 1 次 `shared_lock` | 与其他线程的注册/摘除互斥 |

可选的进一步优化方向（当前**未**实现，保持实现简单）：

- `batch` 用 `thread_local` 复用，去掉每条消息一次分配；
- 当 `dir_` 中不存在任何通配 filter 时，跳过 `wild_filters_` 循环（当前循环本身为空则无成本）；
- 若某 filter 的 handler 长期只有 1 个且从不异步，可退化为"无 Lease 直调"的快路径
  （需要用户显式声明，代价是 API 变复杂）。
