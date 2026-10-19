# 04 · 函数调用链

> 目标：跟代码时的"地图"。共 8 条链：启动、注册、派发、摘除、move、重连、发布、异步。
> 引用格式 `文件::符号`；`▸` 表示内部调用，`⇒` 表示结果/副作用。

## ① 启动链

```
应用
└─ MqttClient::MqttClient(cfg)                    [mqtt_client.cpp]
   ├─ broker_url 为空            ⇒ throw std::invalid_argument
   ├─ client_id 为空             ⇒ generate_client_id()（mqttx-<12 hex>）
   ├─ reconnect_min > max        ⇒ 修正为 max = min
   └─ inner_ = make_unique<mqtt::async_client>(url, client_id)

应用
└─ MqttClient::start()
   ├─ running_.exchange(true, acq_rel) 已为 true ⇒ 直接返回（幂等）
   ├─ connect_options_builder::v5()
   │    .clean_start(clean_session)
   │    .keep_alive_interval(keep_alive_sec)
   │    .automatic_reconnect(reconnect_min, reconnect_max)
   │    .connect_timeout(connect_timeout)
   │    .finalize()
   ├─ username/password 非空 ⇒ set_user_name / set_password
   ├─ inner_->start_consuming()                  ← 先建消费队列，避免连接后丢消息
   ├─ tok = inner_->connect(opts)
   │    └─ tok->wait_for(connect_timeout) 超时 ⇒ throw std::runtime_error
   ├─ rsp = tok->get_connect_response()          ← 阻塞至连接结果可用
   ├─ spdlog::info("已连接 … session_present=…")
   ├─ ▸ MqttClient::on_connected(rsp.is_session_present())
   │    ├─ { lock session_mtx_ } session_present==false ⇒ session_subs_.clear()
   │    ├─ ▸ subscribe_all()                     ← trust_session = connected_once_
   │    └─ { lock session_mtx_ } connected_once_ = true
   ├─ consumer_ = std::jthread(consume_loop)
   └─ catch(...) ⇒ running_=false ▸ disconnect ▸ stop_consuming ▸ rethrow（状态回滚）
```

## ② 注册链（应用线程，或 handler 内）

```
Topic<Svc, M>::open()                              [topic.hpp]
├─ slot_ != 0                    ⇒ return true（幂等）
├─ cli_ == nullptr || svc_ == nullptr ⇒ return false（已被 move）
└─ slot_ = cli_->install(topic_, this, &Topic::invoke, qos_)
   │
   └─ MqttClient::install(topic, obj, fn, qos)     [mqtt_client.cpp]
      ├─ eff_qos = (qos < 0) ? cfg_.qos : qos
      ├─ ▸ TopicDirectory::add(topic, obj, fn, eff_qos)   [topic_directory.cpp]
      │  ├─ topic 为空 / fn 为空 ⇒ throw std::invalid_argument
      │  ├─ slot = make_shared<Slot>()；id = next_slot_.fetch_add(1, relaxed)
      │  ├─ slot->obj.store(obj, relaxed)；slot->fn = fn
      │  ├─ unique_lock(mtx_)
      │  ├─ dir_.try_emplace(filter) ⇒ Entry&
      │  │    ├─ inserted            ⇒ entry.qos = qos；res.subscribe = true
      │  │    │                        is_wildcard_filter(filter) ⇒ wild_filters_.push_back
      │  │    └─ qos > entry.qos     ⇒ entry.qos = qos；res.subscribe = true（需重订阅）
      │  ├─ entry.handlers.push_back(slot)        ← 多播：同一 filter 多个登记项
      │  ├─ by_slot_.insert_or_assign(id, {filter, slot})
      │  └─ ⇒ {slot=id, subscribe, qos=entry.qos}
      └─ res.subscribe && is_running() ⇒
         { lock session_mtx_ }
         inner_->subscribe(filter, res.qos)
         session_subs_.insert(filter)               ← 失败只记日志，不插入（重连时会补）
```

**错误路径**：`subscribe` 抛 `mqtt::exception`（如正处在重连窗口）只记录日志；
登记项仍在目录中，`session_subs_` 不含它，下一次 `on_connected → subscribe_all` 会补订阅。

## ③ 派发链（消费线程，主链路）

```
MqttClient::consume_loop(st)                        [mqtt_client.cpp]
└─ while (!st.stop_requested() && running_.load(acquire))
   ├─ inner_->try_consume_event_for(&evt, poll_timeout)
   │    ├─ 抛 mqtt::exception（队列未启动/已关闭）⇒ 记日志、break
   │    └─ 返回 false（超时）⇒ continue（回到循环检查停止标志）
   ├─ evt.get_message_if() ⇒ *pmsg 为空 ⇒ continue（超时占位事件）
   ├─ 组 MessageView{
   │      topic   = src.get_topic(),            ← 借用 paho 消息内部
   │      payload = src.get_payload_str(),
   │      props   = &src.get_properties(),
   │      qos/retain,
   │      keep_alive = KeepAlive{*pmsg}         ← ① 把"消息寿命"放进凭据
   │  }
   └─ ▸ MqttClient::dispatch(topic, mv)
      ├─ try ⇒ dir_.dispatch(...) ；异常 ⇒ 记日志并返回 0
      └─ ▸ TopicDirectory::dispatch(topic, msg)     [topic_directory.cpp]
         ├─ shared_lock(mtx_)
         ├─ collect = [&](const Entry& e){
         │      for h in e.handlers:
         │          h->alive.load(acquire) 为 false ⇒ skip
         │          h->inflight.fetch_add(1, acq_rel)      ← ② 收进批次
         │          batch.push_back(h) }
         ├─ 精确命中：dir_.find(topic) ⇒ collect(entry)
         ├─ 通配命中：for f in wild_filters_:
         │      f == topic ⇒ skip（已由精确路径处理）
         │      topic_matches_filter(topic, f) ⇒ collect(dir_[f])
         ├─ batch 为空 ⇒ return 0
         ├─ unlock                                        ← R1：用户代码不持锁
         ├─ BatchGuard{batch, next}                       ← 异常兜底：归还剩余 inflight
         └─ for i, h in batch:
            ├─ Guard{h, t_active_slot}                    ← RAII：恢复 active + inflight-1 + notify
            ├─ next = i + 1
            ├─ alive / obj / fn 复查（同批内可能已被摘除）⇒ continue
            ├─ call = msg
            ├─ lease = make_shared<Lease>(h, msg.keep_alive)   ← ③ async_refs+1（仍持有 inflight）
            ├─ call.keep_alive = lease                    ← ④ 把"登记项寿命"也放进凭据
            ├─ t_active_slot = h.get()
            ├─ invoked++
            └─ try h->fn(obj, call)                       ← 唯一进入用户代码的位置
               catch std::exception / ... ⇒ 记日志，继续下一个 handler
                  │
                  └─ Topic<Svc, M>::invoke(self, msg)     [topic.hpp]
                     ├─ M 可用 const Svc* 调用 ⇒ (const Svc*->*M)(msg)（const 成员函数）
                     ├─ M 是成员函数指针       ⇒ (svc_->*M)(msg)      （非 const 成员函数）
                     └─ else                   ⇒ (*svc_)(msg)         （拥有型可调用对象）
```

## ④ 摘除链（应用线程 / 析构 / handler 内）

```
~Topic()  ⇒  Topic::close() noexcept                [topic.hpp]
├─ slot_ == 0 || cli_ == nullptr ⇒ return（幂等）
└─ cli_->uninstall(slot_)；slot_ = 0
   │
   └─ MqttClient::uninstall(slot) noexcept           [mqtt_client.cpp]
      ├─ res = dir_.remove(slot)                     ← 可能长时间阻塞（等异步副本）
      ├─ !res.removed ⇒ return
      └─ res.unsubscribe && is_running() ⇒
         { lock session_mtx_ }
         session_subs_.erase(topic)                  ← 先摘记录，避免重连时又订阅回来
         inner_->unsubscribe(topic)

      TopicDirectory::remove(slot)                   [topic_directory.cpp]
      ├─ unique_lock(mtx_)
      ├─ by_slot_.find(slot) 未命中 ⇒ return {removed=false}（幂等）
      ├─ target = move(rec.slot)；topic = move(rec.topic)；by_slot_.erase(rec)
      ├─ target->alive.store(false, release)         ← 同批后续不再进入
      ├─ dir_.find(topic) ⇒ erase_if(handlers, ==target)
      │     handlers 为空 ⇒ dir_.erase(topic)
      │                    unsubscribe = true
      │                    std::erase(wild_filters_, topic)
      ├─ unlock
      ├─ if (target && t_active_slot != target.get())     ← R3：自等豁免
      │     wait_until_zero(target->inflight)             ← ③ 同步调用帧
      │     wait_until_zero(target->async_refs)           ← ④ 异步副本
      └─ ⇒ {removed, unsubscribe, topic}
```

## ⑤ move 链

```
Topic<Svc, M>::Topic(Topic&& o) noexcept            [topic.hpp]
├─ cli_ = exchange(o.cli_, nullptr)
├─ topic_ = move(o.topic_)；svc_ = o.svc_；owned_ = move(o.owned_)
├─ qos_ = o.qos_
├─ slot_ = exchange(o.slot_, 0)                     ← 移动源从此"失活"（I15）
├─ owned_ 非空 ⇒ svc_ = owned_.get()                ← 拥有型：修正自持有指针
└─ slot_ != 0 && cli_ != nullptr ⇒ cli_->rebind(slot_, this)
   │
   └─ MqttClient::rebind(slot, obj) ⇒ TopicDirectory::rebind
      └─ shared_lock(mtx_) ⇒ by_slot_.find(slot) ⇒ slot->obj.store(obj, release)
```

`operator=(Topic&&)` 顺序不同：**先 `close()` 释放自己原有的注册**，再执行与上面相同的搬运。

## ⑥ 重连链

```
consume_loop 收到 evt.is_connected()（自动重连成功时 paho 会推该事件）
└─ ▸ MqttClient::on_connected(true)
   ├─ { lock session_mtx_ } session_present==true ⇒ 保留 session_subs_（broker 侧仍有会话）
   ├─ ▸ subscribe_all()
   │  ├─ subs = dir_.subscriptions()      ← shared_lock 下的快照（(filter, qos) 列表）
   │  ├─ subs 为空 ⇒ return
   │  ├─ lock session_mtx_
   │  ├─ trust_session = connected_once_
   │  └─ for (filter, qos) in subs:
   │        trust_session && session_subs_.contains(filter) ⇒ skip（已订阅）
   │        inner_->subscribe(filter, qos)；session_subs_.insert(filter)
   │        失败 ⇒ 记日志（不插入，下次再补）
   └─ { lock session_mtx_ } connected_once_ = true

另：evt.is_connection_lost() / is_disconnected() ⇒ 仅记日志，重连由 paho automatic_reconnect 负责
```

**首次连接 vs 重连的差别**：`start()` 在 `connected_once_` 置位**之前**调用 `on_connected`，
所以首次连接时 `trust_session == false` ⇒ 全量订阅（宁可多收一次 retained，也不漏订阅）；
之后的连接 `trust_session == true` ⇒ 只补离线期间新增的 filter。

## ⑦ 发布链

```
Topic<Svc, M>::publish(payload, qos, retain)         [topic.hpp]
├─ require_client()  cli_ == nullptr ⇒ throw std::logic_error（已被 move）
└─ MqttClient::publish(topic_, move(payload), qos, retain)
   │
   └─ MqttClient::publish(...)                       [mqtt_client.cpp]
      ├─ eff_qos = (qos < 0) ? cfg_.qos : qos
      └─ inner_->publish(make_message(topic, move(payload), eff_qos, retain))
         └─ 未连接 ⇒ 抛 mqtt::exception（本库不吞、不转成返回值）

MqttClient::publish_binary(topic, data, len, qos, retain)
└─ inner_->publish(make_message(topic, data, len, eff_qos, retain))
```

## ⑧ 异步链（保活握手）

```
[消费线程] h->fn(obj, call)                          ← call 是带凭据的副本
    │
    └─ pool.post([copy = call] { … })                ← 按值捕获整个 MessageView
         │  ⇒ copy.keep_alive 持有 shared_ptr<Lease> ⇒ async_refs 保持 > 0
         │
[消费线程] 循环体结束 ⇒ 局部 call/lease 析构，但任务里那份还在 ⇒ async_refs 仍 > 0
         │
[应用线程] topic.close() / ~Topic / Session 析构
    └─ remove() ⇒ wait inflight（已 0）⇒ wait async_refs（>0）⇒ 阻塞
         │
[线程池] 任务执行：读 copy.topic / copy.payload（仍有效，Lease 持有消息）
    │
    └─ 任务结束 ⇒ 任务对象析构 ⇒ ~Lease ⇒ async_refs-1 ⇒ notify_all
         │
[应用线程] remove() 被唤醒 ⇒ 返回 ⇒ 目标对象此刻才被回收
```

`deep_copy()` 走的是同一条路：`Owned` 里带 `keep_alive`，字符串自成一份，
因此既保护数据又保护登记项。

## ⑨ 错误路径一览

| 链 | 失败点 | 行为 |
| --- | --- | --- |
| 启动 | `broker_url` 为空 | 构造期 `std::invalid_argument` |
| 启动 | 连接超时 / 连接被拒 | `runtime_error` 或 `mqtt::exception` 抛出；`running_` 回滚、已断开、队列已停 |
| 启动 | 订阅失败（首次） | 记日志；登记项保留；后续 `on_connected` 会补 |
| 注册 | filter 为空 / thunk 为空 | `std::invalid_argument` |
| 注册 | `SUBSCRIBE` 抛异常 | 记日志；不写入 `session_subs_`（重连补） |
| 派发 | 事件队列异常 | 记日志并退出消费线程（`running_` 不变，需应用重启或后续 `start`） |
| 派发 | handler 抛异常 | 就地吞掉并记日志；同批其它 handler 继续 |
| 派发 | 凭据分配失败（`bad_alloc`） | `BatchGuard` 归还本批剩余 `inflight` 后异常外抛；`MqttClient::dispatch` 捕获记日志 |
| 摘除 | 未知 / 重复 slot | 幂等返回 `removed=false`，不抛 |
| 摘除 | `UNSUBSCRIBE` 抛异常 | `noexcept` 内吞掉并记日志 |
| 发布 | 未连接 | `mqtt::exception` 直接抛出（由调用方决定策略） |
| 发布 | 在 move 后的 `Topic` 上调用 | `std::logic_error` |
