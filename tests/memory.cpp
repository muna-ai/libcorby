/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <poll.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include "corby/corby.hpp"
#include "corby/runtime.h"

extern char** environ;
#define CHECK(expression) do { if (!(expression)) throw std::runtime_error(    std::string("line ") + std::to_string(__LINE__) + ": " #expression); } while (false)

using namespace std::chrono_literals;

namespace {
struct Events {
    std::mutex mutex;
    std::condition_variable changed;
    cor_reclaim_t* ticket = nullptr;
    uint32_t completions = 0;
    cor_status_t acquisition = COR_PENDING;
    cor_status_t release_in_callback = COR_OK;
    cor_memory_progress_t progress{};
    bool progress_seen = false;

    template <typename F>
    void wait(F&& predicate) {
        std::unique_lock lock(mutex);
        CHECK(changed.wait_for(lock, 3s, std::forward<F>(predicate)));
    }
};
void acquired(
    cor_lease_t* lease,
    void* raw
) {
    auto& events = *static_cast<Events*>(raw);
    const auto status = cor_lease_acquired(lease);
    const auto release = cor_lease_release(lease);
    std::lock_guard lock(events.mutex);
    ++events.completions;
    events.acquisition = status;
    events.release_in_callback = release;
    events.changed.notify_all();
}
void warning(
    cor_reclaim_t* request,
    void* raw
) {
    auto& events = *static_cast<Events*>(raw);
    std::lock_guard lock(events.mutex);
    events.ticket = request;
    events.changed.notify_all();
}
void progress(
    cor_lease_t*,
    const cor_memory_progress_t* value,
    void* raw
) {
    auto& events = *static_cast<Events*>(raw);
    std::lock_guard lock(events.mutex);
    events.progress = *value;
    events.progress_seen = true;
    events.changed.notify_all();
}
// Signals precede callback return. A close is explicitly allowed to report BUSY
// in that narrow window; keep the callback's borrowed context alive until fenced.
template <typename H>
void close(H& handle) {
    const auto deadline = std::chrono::steady_clock::now() + 3s;
    for (;;) {
        const auto status = handle.close();
        if (status == COR_OK)
            return;
        CHECK(status == COR_BUSY && std::chrono::steady_clock::now() < deadline);
        std::this_thread::yield();
    }
}

int worker(const char* endpoint, int output) {
    auto context = corby::Context::create(COR_FEATURE_MEMORY_LEASES | COR_FEATURE_ASYNC_LEASES, endpoint);
    CHECK(context);
    const int32_t devices[]{0};
    auto engine = context.value.register_engine("worker", devices);
    CHECK(engine);
    Events events;
    CHECK(engine.value.set_memory_handler(warning, &events) == COR_OK);
    auto work = engine.value.begin();
    CHECK(work);
    auto pending = work.value.acquire({ .asynchronous = true });
    CHECK(pending && pending.value.acquired() == COR_PENDING);
    CHECK(pending.value.register_handler(acquired, &events) == COR_OK);
    CHECK(write(output, "R", 1) == 1);
    events.wait([&] { return events.ticket != nullptr; });
    corby::Reclaim request(events.ticket);
    auto requirements = request.requirements();
    CHECK(requirements && requirements.value[0].bytes == 4096);
    CHECK(request.complete(COR_OK, 4096) == COR_OK);
    events.wait([&] { return events.completions == 1; });
    CHECK(events.acquisition == COR_OK && events.release_in_callback == COR_BUSY);
    close(pending.value);
    close(work.value);
    CHECK(engine.value.set_memory_handler(nullptr) == COR_OK);
    close(engine.value);
    close(context.value);
    return 0;
}

#ifndef CORBY_MEMORY_WORKER
void run(uint32_t scope) {
    const auto endpoint = "/tmp/corby-memory-" + std::to_string(getpid()) + ".sock";
    const cor_runtime_options_t options{
        .size = sizeof(cor_runtime_options_t),
        .abi_version = COR_ABI_VERSION,
        .scope = scope,
        .reserved = 0,
        .endpoint = scope == COR_SCOPE_SYSTEM ? endpoint.c_str() : nullptr
    };
    cor_runtime_t* runtime = nullptr;
    CHECK(cor_runtime_start(&options, &runtime) == COR_OK);
    const auto* socket = scope == COR_SCOPE_SYSTEM ? endpoint.c_str() : nullptr;
    auto context = corby::Context::create(COR_FEATURE_MEMORY_LEASES | COR_FEATURE_ASYNC_LEASES, socket);
    auto other = corby::Context::create(COR_FEATURE_MEMORY_LEASES, socket);
    CHECK(context && other);
    const int32_t devices[]{0};
    auto a = context.value.register_engine("newcomer", devices);
    auto b = other.value.register_engine("resident", devices, 1, COR_TENANCY_SHARED,
        COR_CAP_COOPERATIVE_YIELD | COR_CAP_MEMORY_RECLAIM);
    CHECK(a && b);
    Events events;
    CHECK(b.value.set_memory_handler(warning, &events) == COR_OK);
    auto wa = a.value.begin();
    auto wb = b.value.begin();
    CHECK(wa && wb);
    auto holder = wb.value.acquire();
    auto pending = wa.value.acquire({ .asynchronous = true });
    CHECK(holder && pending);
    CHECK(pending.value.acquired() == COR_PENDING);
    auto yield = corby::should_yield(pending.value);
    CHECK(yield.status == COR_PENDING && yield.value);
    CHECK(pending.value.register_handler(acquired, &events) == COR_OK);
    close(holder.value);
    events.wait([&] { return events.completions == 1; });
    CHECK(events.acquisition == COR_OK && events.release_in_callback == COR_BUSY);
    CHECK(pending.value.register_handler(acquired, &events) == COR_BUSY);
    close(pending.value);

    // Reclamation can proceed while resident compute is active.
    auto computing = wb.value.acquire();
    auto memory = a.value.begin(corby::Resource::Memory, corby::WorkIntent::Initialization);
    CHECK(computing && memory);
    auto lease = memory.value.acquire();
    CHECK(lease);
    const cor_memory_requirement_t requirements[]{{ .device_id = 0, .reserved = 0, .bytes = 1024 }};
    CHECK(lease.value.reclaim_memory(requirements, progress, &events, 2'000'000'000) == COR_OK);
    events.wait([&] { return events.ticket != nullptr; });
    corby::Reclaim request(events.ticket);
    auto requested = request.requirements();
    CHECK(requested && requested.value.size() == 1 && requested.value[0].bytes == 1024);
    CHECK(request.complete(COR_PENDING) == COR_INVALID_ARGUMENT);
    CHECK(request.complete(COR_OK, 2048) == COR_OK);
    events.wait([&] { return events.progress_seen && events.progress.status != COR_PENDING; });
    CHECK(events.progress.status == COR_OK && events.progress.completed == 1 && events.progress.released_bytes == 2048);
    close(lease.value);

    // Release interest early: the delivered recipient obligation stays valid.
    events.ticket = nullptr;
    auto early = memory.value.acquire();
    CHECK(early && early.value.reclaim_memory(requirements, progress, &events) == COR_OK);
    events.wait([&] { return events.ticket != nullptr; });
    close(early.value);
    corby::Reclaim late(events.ticket);
    CHECK(late.complete() == COR_OK);

    close(computing.value);
    close(memory.value);
    close(wa.value);
    close(wb.value);
    CHECK(b.value.set_memory_handler(nullptr) == COR_OK);
    close(a.value);
    close(b.value);
    close(context.value);
    close(other.value);
    CHECK(cor_runtime_shutdown(runtime) == COR_OK);
    CHECK(cor_runtime_destroy(runtime) == COR_OK);
}

void cross_process(const char* executable) {
    const auto endpoint = "/tmp/corby-memory-worker-" + std::to_string(getpid()) + ".sock";
    const cor_runtime_options_t options{
        .size = sizeof(cor_runtime_options_t),
        .abi_version = COR_ABI_VERSION,
        .scope = COR_SCOPE_SYSTEM,
        .reserved = 0,
        .endpoint = endpoint.c_str()
    };
    cor_runtime_t* runtime = nullptr;
    CHECK(cor_runtime_start(&options, &runtime) == COR_OK);
    auto context = corby::Context::create();
    CHECK(context);
    const int32_t devices[]{0};
    auto engine = context.value.register_engine("host", devices);
    CHECK(engine);
    auto compute = engine.value.begin();
    auto holder = compute.value.acquire();
    CHECK(compute && holder);
    int descriptors[2];
    CHECK(pipe(descriptors) == 0);
    const auto output = std::to_string(descriptors[1]);
    char* args[]{const_cast<char*>(executable), const_cast<char*>("--worker"),
        const_cast<char*>(endpoint.c_str()), const_cast<char*>(output.c_str()), nullptr};
    pid_t process = 0;
    CHECK(posix_spawn(&process, executable, nullptr, nullptr, args, environ) == 0);
    pollfd pipe_event{ .fd = descriptors[0], .events = POLLIN, .revents = 0 };
    CHECK(poll(&pipe_event, 1, 5000) == 1);
    char ready = 0;
    CHECK(read(descriptors[0], &ready, 1) == 1 && ready == 'R');
    auto memory = engine.value.begin(corby::Resource::Memory);
    auto lease = memory.value.acquire();
    CHECK(memory && lease);
    Events events;
    const cor_memory_requirement_t requirements[]{{ .device_id = 0, .reserved = 0, .bytes = 4096 }};
    CHECK(lease.value.reclaim_memory(requirements, progress, &events, 2'000'000'000) == COR_OK);
    events.wait([&] { return events.progress_seen && events.progress.status != COR_PENDING; });
    CHECK(events.progress.status == COR_OK && events.progress.released_bytes == 4096);
    close(lease.value);
    close(memory.value);
    close(holder.value);
    close(compute.value);
    int status = 0;
    CHECK(waitpid(process, &status, 0) == process && WIFEXITED(status) && WEXITSTATUS(status) == 0);
    ::close(descriptors[0]);
    ::close(descriptors[1]);
    close(engine.value);
    close(context.value);
    CHECK(cor_runtime_destroy(runtime) == COR_OK);
}
#endif
} // namespace

int main(int argc, char** argv) {
    try {
        if (argc == 4)
            return worker(argv[2], std::atoi(argv[3]));
#ifdef CORBY_MEMORY_WORKER
        return 1;
#else
        CHECK(argc == 2);
        run(COR_SCOPE_PROCESS);
        run(COR_SCOPE_SYSTEM);
        cross_process(argv[1]);
        return 0;
#endif
    } catch (const std::exception& error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }
}
