# 03 · 线程模型与同步协议

> 目标：说清"哪段代码跑在哪个线程上、用什么同步、为什么不会死锁"。

## 3.1 线程角色

| # | 线程 | 谁创建 | 运行什么 | 数量 |
| --- | --- | --- | --- | --- |
| T1 | **应用线程**（任意个） | 应用 | `Topic::open/close/set_qos/publish`、`MqttClient::start/stop/publish/install/uninstall`、`deep_copy` 等 | 0..N |
| T2 | **消费线程** | `MqttClient::start()` 里的 `std::jthread` | `consume_loop` → `dispatch` → `Topic::invoke` → 用户 handler | **恰好 1** |
| T3 | **paho 网络线程** | paho.mqtt.c | socket 收发、协议状态机、把消息/事件压入 consumer queue | paho 内部管理 |
| T4 | **用户异步线程** | 应用（线程池 / worker） | handler 派生的后续处理；库**只能**通过 `Lease` 引用计数感知 | 0..N |

关键事实：**T2 是唯一的派发者**（不变式 I13）。库的全部生命周期协议都建立在
"派发是单线程的、摘除可以来自任何线程"这一前提上。

## 3.2 调用点 → 线程矩阵

| 函数 | T1 应用线程 | T2 消费线程（handler 内） | 备注 |
| --- | --- | --- | --- |
| `MqttClient::start` | ✅ | ❌ 不要 | 见 3.7 重启注意 |
| `MqttClient::stop` | ✅ | ⚠️ 允许但不 join 自己 | 不会自死锁，但不要随即析构 `MqttClient` |
| `MqttClient::install/uninstall/rebind` | ✅ | ✅（`Topic::open/close`） | 这是"回调内再注册"设计支持的核心场景 |
| `TopicDirectory::dispatch` | ✅（离线注入） | ✅（正常路径） | 目录层支持并发 dispatch；`MqttClient` 不用 |
| `publish` | ✅ | ✅ | paho 自身线程安全；未连接时抛 `mqtt::exception` |
| `Topic::open/close/set_qos` | ✅ | ✅ | 幂等 |
| `Topic` 析构 | ✅ | ⚠️ 只在自己是当前 handler 时安全（自等豁免） | 见 `05` 的边界说明 |
| `MqttClient` 析构 | ✅ | ❌ 会 join 当前线程 | 不要在 handler 里销毁客户端 |

## 3.3 同步原语清单

| 原语 | 类型 | 保护/表达 | 使用处 |
| --- | --- | --- | --- |
| `mtx_` | `std::shared_mutex` | `dir_` / `wild_filters_` / `by_slot_` | `add` / `remove` / `rebind` / `dispatch` / 查询接口 |
| `Slot::alive` | `atomic<bool>` | "本登记项是否已被摘除" | `remove` 写（release）、`dispatch` 读（acquire） |
| `Slot::obj` | `atomic<void*>` | 目标对象指针（支持 move 后 rebind） | `rebind` 写（release）、`dispatch` 读（acquire） |
| `Slot::inflight` | `atomic<int>` | **在飞调用帧**计数 | `dispatch` ±1（acq_rel）、`remove` 等待 |
| `Slot::async_refs` | `atomic<int>` | **存活保活凭据**计数 | `Lease` 构造/析构 ±1、`remove` 等待 |
| `session_mtx_` | `std::mutex` | `session_subs_` / `connected_once_` | `install` / `uninstall` / `on_connected` / `subscribe_all` |
| `running_` | `atomic<bool>` | 客户端生命周期闸门 | `start`/`stop`（acq_rel exchange）、`is_running`/`consume_loop`（acquire） |
| `next_slot_` | `atomic<uint64_t>` | slot id 发放 | `add`（relaxed fetch_add，只需唯一性） |
| `t_active_slot` | `thread_local const void*` | 当前线程正在执行的 handler（自等豁免） | `dispatch` 设置/恢复，`remove` 查询 |

**为什么等待用 `atomic::wait/notify_all` 而不是条件变量**：
等待者（`remove`）必须在**不持有任何锁**的情况下阻塞（见 3.5），
而 `condition_variable` 需要一个配对的 mutex；`atomic::wait` 直接把
"值变化"当事件，省掉 mutex，也避免了"持锁等待"导致的连锁阻塞。

## 3.4 内存序选择与理由

| 位置 | 序 | 理由 |
| --- | --- | --- |
| `alive.store(false)` / `alive.load()` | release / acquire | 保证"摘除标记"与"容器修改"对派发线程可见 |
| `obj.store()` / `obj.load()` | release / acquire | 保证 `rebind` 写入的新指针与对象初始化对派发线程可见 |
| `inflight` / `async_refs` 的 ±1 | acq_rel | 与 `wait/notify` 配对；release 保证计数归零前的写入对等待者可见 |
| `running_` exchange | acq_rel | 一个线程的 start 与另一个线程的 stop 之间建立顺序 |
| `next_slot_` fetch_add | relaxed | 只要求唯一，不承担同步职责 |

## 3.5 锁序与三条防死锁规则

**锁序**：`mtx_`（目录）与 `session_mtx_`（会话）**从不嵌套持有**。

```
install()    : dir_.add() 返回 ──► 取 session_mtx_ ──► inner_->subscribe ──► 释放
uninstall()  : dir_.remove() 返回（可能长时间阻塞）──► 取 session_mtx_ ──► unsubscribe ──► 释放
on_connected(): { session_mtx_ 清空 } ──► subscribe_all(){ 取 session_mtx_ } ──► { 取 session_mtx_ 置位 }
```

**R1 · 持锁不调用户代码**：`h->fn(...)` 只在 `mtx_` 之外调用。
⇒ handler 里 `open()`/`close()`（会取 `mtx_`）不会死锁。

**R2 · 等待不持锁**：`remove` 的 `wait_until_zero(inflight)` / `wait_until_zero(async_refs)`
都在释放 `mtx_` 之后执行。
⇒ 某个 handler 阻塞时，其它线程仍能注册/摘除/派发别的 topic。

**R3 · 自等豁免**：`remove` 若发现 `t_active_slot == 目标 slot`（即"回调里析构自己"），
跳过两段等待。
⇒ 不会自己等自己；代价是该场景下不提供异步保护（语义上自相矛盾，见 `05`）。

## 3.6 等待协议（两段等待的完整时序）

```
dispatch(T2)                                   remove(T1)
─────────────                                  ───────────
shared_lock(mtx_)
  alive? → inflight++            ← ① 收进批次
unlock
                                                unique_lock(mtx_)
                                                  alive = false          ← ② 先失效
                                                  erase(both maps)
                                                unlock
                                                wait inflight == 0       ← ③ 等调用帧
make_shared<Lease>  async_refs++ ← ④ 建凭据（此刻 inflight 仍 >0）
h->fn(obj, call)                 ← 用户代码
  └─ 若投递线程池：副本持有 Lease ⇒ async_refs 保持 >0
inflight-- (notify)              ← ⑤ 调用帧结束
                                                inflight==0 → 继续
                                                wait async_refs == 0     ← ⑥ 等异步副本
                                                  （无副本则立即返回）
任务结束 → ~Lease → async_refs-- (notify) ──────► ⑦ 醒来，remove 返回
                                                ⇒ 目标对象此刻才可能被析构
```

**顺序不可颠倒的原因**（不变式 I6 + I8）：

1. ② 在锁内完成，因此 ① 之后不可能再有新的批次包含该 slot；
2. ④ 发生在 `inflight > 0` 的窗口内，所以 ③ 归零时"该 slot 的所有凭据都已创建"；
3. 于是 ⑥ 观察到的 `async_refs` 是完整的，不会漏掉任何一份异步副本。

若反过来先等 ⑥，会出现"⑥ 读到 0 → 返回 → 对象析构 → 另一线程才在 ④ 建凭据并调用"的窗口。

## 3.7 生命周期时序（start / stop / 重启）

```
start()                                   stop()
───────                                   ──────
running_.exchange(true)  ── 已 true ⇒ 直接返回   running_.exchange(false) ── 已 false ⇒ 直接返回（幂等）
  try:                                            consumer_.request_stop()
    start_consuming()          ← 先建队列        inner_->stop_consuming()   ← 唤醒等待中的消费线程
    connect(opts) → wait_for(timeout)            if (joinable && 不是自己) join()
    get_connect_response()                       if (is_connected) disconnect()->wait()
    on_connected(session_present)                （全部包在 try/catch 中，noexcept）
    consumer_ = jthread(consume_loop)
  catch: running_=false + disconnect + stop_consuming + rethrow   ← 状态回滚
```

- `start()` 失败不会留下"running_ 为 true 但没有连接也没有消费线程"的半死状态；
- `stop()` 幂等且 `noexcept`；在消费线程内调用时跳过 `join`（避免自 join）；
- **重启**：`stop()` 正常 join 之后可以再次 `start()`。但**不要**在 handler 里
  `stop()` 后立刻 `start()`——旧 `jthread` 尚未 join，重新赋值会触发对当前线程的 join；
- 消费线程退出条件：`st.stop_requested() || !running_`，因此即使收到 paho 的 shutdown 事件
  也不会空转。

## 3.8 与 paho 的线程交接

| 方向 | 机制 | 注意 |
| --- | --- | --- |
| 库 → paho | `inner_->subscribe/unsubscribe/publish/connect/disconnect` | 均为非阻塞入队 + 返回 token；`connect` 后我们用 `wait_for(connect_timeout)` 限时等待 |
| paho → 库 | `start_consuming()` 建立的 consumer queue | 由 T2 用 `try_consume_event_for(&evt, poll_timeout)` 拉取，超时返回 false 以便检查停止标志 |
| 事件类型 | `const_message_ptr` / `connected_event` / `connection_lost_event` / `disconnected_event` | 只处理前两类 + 记录后两类；**不**使用 `event::is_shutdown()`（paho 该实现判的是 `disconnected_event`，语义有误） |
| 会话补齐 | `connected_event` → `on_connected(true)` → `subscribe_all()` | 幂等；`session_subs_` 决定是否真的发送 SUBSCRIBE |

`session_mtx_` 会在调用 `inner_->subscribe()` 期间被持有（3.5 的锁序图）。
前提假设：**paho 的 subscribe/unsubscribe 不会同步回调进本库**（它们只入队并返回 token）。
若将来引入 paho 的同步回调监听器，需要把 paho 调用移出 `session_mtx_` 临界区。

## 3.9 线程安全承诺（对外）

| 你可以假设 | 你不可以假设 |
| --- | --- |
| 任意线程调用 `install/uninstall/rebind/publish/open/close` 都安全 | handler 会被多个线程并发调用（实际只有 T2 调用它） |
| handler 内可以安全地 `open/close` 其它（甚至自己）Topic | handler 内可以安全地析构 `MqttClient` |
| `close()` 返回后，同步 handler 一定不再被调用 | `close()` 返回后，**你派生的异步任务**也一定结束了——只有按值捕获了消息副本（凭据）才会等待，见 `05` |
| 同一个 `Topic` 对象不会被两个线程同时 move（它是普通对象，需自行保证） | `Topic::publish` 在 move 之后仍可用（会抛 `std::logic_error`） |
