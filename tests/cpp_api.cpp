/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <array>
#include <atomic>
#include <chrono>
#include <thread>
#include <type_traits>

#include "corby/corby.hpp"
#include "corby/runtime.h"

#define CHECK(expr) do { if (!(expr)) return __LINE__; } while (0)

int run() {
    static_assert(!std::is_copy_constructible_v<corby::Lease>);
    static_assert(std::is_nothrow_move_constructible_v<corby::Lease>);
    auto context = corby::Context::create();
    CHECK(context);
    const std::array<int32_t, 2> devices{ 0, 1 };
    auto shared = context.value.register_engine("shared", devices);
    auto owner = context.value.register_engine("owner", devices, 1, COR_TENANCY_DEDICATED);
    CHECK(shared && owner);
    auto work = shared.value.begin();
    CHECK(work);
    auto held = work.value.acquire(0);
    CHECK(held);
    auto demand = owner.value.begin();
    CHECK(demand);
    CHECK(corby::should_yield(held.value, UINT64_MAX).value);
    CHECK(held.value.close() == COR_OK);
    CHECK(work.value.acquire(0).status == COR_TIMEOUT);
    auto owner_held = demand.value.acquire(0);
    CHECK(owner_held);
    CHECK(owner_held.value.close() == COR_OK);
    CHECK(demand.value.close() == COR_OK);
    CHECK(work.value.acquire(0));
    CHECK(shared.value.stats().value.grants == 2);

    auto held_again = work.value.acquire(0);
    CHECK(held_again);
    auto waiter = owner.value.begin();
    CHECK(waiter);
    std::atomic<int32_t> status{ COR_INTERNAL };
    std::thread thread([&] {
        try {
            status.store(waiter.value.acquire().status);
        } catch (...) {
            status.store(COR_INTERNAL);
        }
    });
    // Cancellation is sticky, so it is valid even before acquire queues.
    CHECK(waiter.value.cancel() == COR_OK);
    thread.join();
    CHECK(status.load() == COR_CANCELLED);
    CHECK(held_again.value.close() == COR_OK);
    return 0;
}

int main() {
    cor_runtime_t* runtime = nullptr;
    CHECK(cor_runtime_start(nullptr, &runtime) == COR_OK);
    const auto status = run();
    CHECK(cor_runtime_destroy(runtime) == COR_OK);
    return status;
}
