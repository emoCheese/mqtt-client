#pragma once

// ---------------------------------------------------------------------------
// Handler —— 类型擦除后的消息处理器
//
// 与旧的 “void* obj + InvokeFn” 不同，Handler 直接持有调用目标：
//   - 成员函数模式：{Svc*, 静态 thunk}，零堆分配；
//   - 可调用对象模式：{shared_ptr<State<F>>, 静态 thunk}，一次分配。
//
// 注册后 Handler 不再变化，因此目录不需要在对象移动时 rebind，也不需要
// atomic<void*> 读取目标指针。每次调用仍是一次间接调用（与旧实现相同）。
// ---------------------------------------------------------------------------

#include "message.hpp"

#include <memory>
#include <type_traits>
#include <utility>

namespace mqttx {

/// handler 的统一调用约束（成员绑定请用 on<&Svc::M>() 或 Handler::member）
template <typename F>
concept MessageHandler = std::is_invocable_v<F&, const MessageView&>;

class Handler {
public:
    Handler() noexcept = default;
    Handler(const Handler&) = default;
    Handler& operator=(const Handler&) = default;

    Handler(Handler&& o) noexcept
        : target_{std::exchange(o.target_, nullptr)}
        , fn_{std::exchange(o.fn_, nullptr)}
        , owner_{std::move(o.owner_)} {}

    Handler& operator=(Handler&& o) noexcept {
        if (this != &o) {
            target_ = std::exchange(o.target_, nullptr);
            fn_     = std::exchange(o.fn_, nullptr);
            owner_  = std::move(o.owner_);
        }
        return *this;
    }

    [[nodiscard]] bool valid() const noexcept { return fn_ != nullptr && target_ != nullptr; }
    explicit operator bool() const noexcept { return valid(); }

    void operator()(const MessageView& msg) const {
        if (fn_ != nullptr && target_ != nullptr) fn_(target_, msg);
    }

    /// 绑定到 svc 的成员函数（const / 非 const 均可，M 在编译期已知）
    template <typename Svc, auto M>
        requires std::is_member_function_pointer_v<decltype(M)> &&
                 std::is_invocable_v<decltype(M), Svc*, const MessageView&>
    [[nodiscard]] static Handler member(Svc& svc) noexcept {
        return Handler{&svc, &invoke_member<Svc, M>, nullptr};
    }

    /// 拥有型：绑定到 lambda / 函数对象 / std::function
    template <typename F>
        requires std::is_invocable_v<std::decay_t<F>&, const MessageView&>
    [[nodiscard]] static Handler callable(F&& fn) {
        using Stored = std::decay_t<F>;
        auto  state  = std::make_shared<State<Stored>>(std::forward<F>(fn));
        // 先取出裸指针：std::move(state) 会清空 state，而 C++ 不保证实参求值
        // 顺序（MSVC 从右往左求值），直接在实参里调用 state.get() 会得到空指针。
        auto* target = state.get();
        return Handler{target, &invoke_state<Stored>, std::move(state)};
    }

private:
    template <typename F>
    struct State {
        F fn;

        template <typename G>
        explicit State(G&& g) : fn(std::forward<G>(g)) {}
    };

    template <typename Svc, auto M>
    static void invoke_member(void* target, const MessageView& msg) {
        auto* svc = static_cast<Svc*>(target);
        if constexpr (std::is_invocable_v<decltype(M), const Svc*, const MessageView&>) {
            (static_cast<const Svc*>(svc)->*M)(msg);  // const 成员函数
        } else {
            (svc->*M)(msg);                            // 非 const 成员函数
        }
    }

    template <typename F>
    static void invoke_state(void* target, const MessageView& msg) {
        static_cast<State<F>*>(target)->fn(msg);
    }

    Handler(void* target, void (*fn)(void*, const MessageView&), std::shared_ptr<void> owner) noexcept
        : target_{target}, fn_{fn}, owner_{std::move(owner)} {}

    void*                 target_{nullptr};
    void (*fn_)(void*, const MessageView&){nullptr};
    std::shared_ptr<void> owner_;  ///< 仅可调用对象模式非空
};

/// 语法糖：mqttx::on<&Svc::on_msg>(svc)
template <auto M, typename Svc>
    requires std::is_member_function_pointer_v<decltype(M)> &&
             std::is_invocable_v<decltype(M), Svc*, const MessageView&>
[[nodiscard]] Handler on(Svc& svc) noexcept {
    return Handler::member<Svc, M>(svc);
}

}  // namespace mqttx
