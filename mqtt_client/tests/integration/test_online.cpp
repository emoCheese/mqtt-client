// ---------------------------------------------------------------------------
// 端到端测试：真正连接 broker（由 tests/integration/mock_broker.py 提供），
// 验证 订阅 → 发布 → 回环 → 按 topic filter 派发 → 退订 的完整链路。
//
//   ./mqttx_online_test tcp://127.0.0.1:18830
// ---------------------------------------------------------------------------

#include "mqtt_client.hpp"
#include "topic.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace mqttx;

namespace {

struct Counters {
    std::mutex              m;
    std::condition_variable cv;
    int                     exact{0};
    int                     plus{0};
    int                     hash{0};

    void bump(int& field) {
        {
            const std::lock_guard lk{m};
            ++field;
        }
        cv.notify_all();
    }

    /// 等到 pred 成立或超时
    template <typename Pred>
    bool wait_for(Pred pred, std::chrono::milliseconds timeout) {
        std::unique_lock lk{m};
        return cv.wait_for(lk, timeout, [&] { return pred(); });
    }

    int get(int Counters::*field) {
        const std::lock_guard lk{m};
        return this->*field;
    }
};

struct Service {
    Counters* c{nullptr};

    void on_exact(const MessageView&) { c->bump(c->exact); }
    void on_plus(const MessageView&) { c->bump(c->plus); }
};

int g_failed = 0;

void check(bool ok, const std::string& what) {
    std::cout << (ok ? "  [ OK ] " : "  [FAIL] ") << what << '\n';
    if (!ok) ++g_failed;
}

/// 轮询等待条件成立（测试里只用于跨线程的最终一致性判断）
template <typename Pred>
bool wait_until(Pred pred, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return pred();
}

}  // namespace

int main(int argc, char** argv) {
    const std::string broker = (argc > 1) ? argv[1] : "tcp://127.0.0.1:18830";

    MqttConfig cfg;
    cfg.broker_url = broker;
    cfg.client_id  = "mqttx-online-test";
    cfg.qos        = 1;
    cfg.connect_timeout = std::chrono::milliseconds(3000);

    MqttClient cli{cfg};
    Counters   counters;
    Service    svc{&counters};

    Topic<Service> exact{cli, "it/1/cmd", svc, &Service::on_exact, 1};
    Topic<Service> plus{cli, "it/+/cmd", svc, &Service::on_plus, 1};
    auto           hash = make_topic(cli, "it/#", [&](const MessageView&) { counters.bump(counters.hash); }, 1);
    Topic<Service> idle{cli, "never/used", svc, &Service::on_exact, 1};

    // 异步 handler：把 MessageView 副本投递到“线程池”（这里用 detached 线程模拟），
    // 调用帧立刻返回，但凭据让消息数据与登记项一直活到任务结束。
    struct AsyncPool {
        std::mutex               m;
        std::vector<std::thread> workers;
        std::atomic<int>         good{0};
        std::atomic<int>         bad{0};

        void post(MessageView copy) {
            std::lock_guard lk{m};
            workers.emplace_back([copy, this] {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                if (copy.payload == "async-payload") good.fetch_add(1);
                else bad.fetch_add(1);
            });
        }

        void join_all() {
            std::lock_guard lk{m};
            for (auto& t : workers) t.join();
            workers.clear();
        }
    } pool;

    auto async_topic = make_topic(cli, "it/async",
                                  [&pool](const MessageView& m) { pool.post(m); }, 1);

    exact.open();
    plus.open();
    hash.open();
    idle.open();
    async_topic.open();

    check(cli.topic_count() == 5, "注册了 5 个 topic filter");

    try {
        cli.start();
    }
    catch (const std::exception& e) {
        std::cout << "  [FAIL] 连接 " << broker << " 失败: " << e.what() << '\n';
        return 1;
    }
    check(cli.is_running(), "start() 后 is_running() == true");
    check(cli.is_connected(), "start() 后已连接 broker");

    try {
        auto tok = cli.publish("it/1/cmd", R"({"n":1})");
        check(tok && tok->wait_for(std::chrono::milliseconds(2000)), "publish(QoS1) 完成");
        cli.publish("it/9/cmd", R"({"n":2})");
        cli.publish("other/x", "ignored");  // 不匹配任何 filter
    }
    catch (const std::exception& e) {
        std::cout << "  [FAIL] publish 异常: " << e.what() << '\n';
        return 1;
    }

    const bool got_all = counters.wait_for(
        [&] { return counters.exact == 1 && counters.plus == 2 && counters.hash == 2; },
        std::chrono::milliseconds(5000));

    check(got_all, "3 条发布按 filter 路由到位");
    std::cout << "         exact=" << counters.get(&Counters::exact)
              << " plus=" << counters.get(&Counters::plus) << " hash=" << counters.get(&Counters::hash)
              << '\n';
    check(counters.get(&Counters::exact) == 1, "精确 filter 'it/1/cmd' 只收到自己的消息");
    check(counters.get(&Counters::plus) == 2, "'it/+/cmd' 收到 2 条");
    check(counters.get(&Counters::hash) == 2, "'it/#' 收到 2 条");

    // 异步 handler：派发帧已返回、消息也可能已被 paho 释放，异步任务仍要读到正确数据
    cli.publish("it/async", "async-payload");
    const bool async_ok = wait_until([&] { return pool.good.load() + pool.bad.load() >= 1; },
                                     std::chrono::milliseconds(3000));
    check(async_ok, "异步 handler 收到消息");
    check(pool.good.load() == 1, "异步任务读到的载荷正确（保活凭据生效）");
    check(pool.bad.load() == 0, "异步任务没有读到损坏数据");
    pool.join_all();

    // 退订：关闭 hash 后该 filter 不再收到消息
    hash.close();
    check(cli.topic_count() == 4, "close() 后目录只剩 4 个 filter");
    const int hash_before  = counters.get(&Counters::hash);
    const int exact_before = counters.get(&Counters::exact);

    cli.publish("it/1/cmd", R"({"n":3})");
    const bool exact_grew = counters.wait_for(
        [&] { return counters.exact > exact_before; }, std::chrono::milliseconds(3000));
    check(exact_grew, "退订后其余 filter 仍可收到消息");
    check(counters.get(&Counters::hash) == hash_before, "已退订的 'it/#' 不再收到消息");

    cli.stop();
    check(!cli.is_running(), "stop() 后 is_running() == false");

    std::cout << (g_failed == 0 ? "端到端测试全部通过\n" : "端到端测试存在失败\n");
    return g_failed == 0 ? 0 : 1;
}
