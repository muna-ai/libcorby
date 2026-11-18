/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#pragma once

#include <span>
#include <utility>
#include <vector>

#include "corby/corby.h"

namespace corby {

/**
 * Small status-based result; failures never become passthrough leases.
 */
template <typename T>
struct Result {
    cor_status_t status = COR_INTERNAL;
    T value{};
    explicit operator bool() const noexcept { return status == COR_OK; }
};

/** 
 * Move-only owning handle. Parent handles must outlive their children.
 * Drain device work before Lease destruction, including exception paths.
 * Destruction must not race any use; close() exposes BUSY/errors explicitly.
 */
template <typename T, cor_status_t (*Destroy)(T*)>
class Handle {
public:
    Handle() = default;
    explicit Handle(T* raw) noexcept : raw_(raw) {}
    Handle(const Handle&) = delete;
    Handle& operator=(const Handle&) = delete;
    Handle(Handle&& other) noexcept : raw_(std::exchange(other.raw_, nullptr)) {}
    // Assignment could silently lose an existing BUSY handle; keep it explicit.
    Handle& operator=(Handle&&) = delete;
    ~Handle() noexcept { (void)close(); }

    T* get() const noexcept { return raw_; }
    cor_status_t close() noexcept {
        const auto status = Destroy(raw_);
        if (status == COR_OK)
            raw_ = nullptr;
        return status;
    }
protected:
    T* raw_ = nullptr;
};

enum class Resource : uint32_t { Compute = COR_RESOURCE_COMPUTE, Memory = COR_RESOURCE_MEMORY };
enum class WorkIntent : uint32_t {
    Required = COR_WORK_INTENT_REQUIRED,
    Initialization = COR_WORK_INTENT_INITIALIZATION,
    Opportunistic = COR_WORK_INTENT_OPPORTUNISTIC
};
struct AcquisitionOptions {
    uint64_t timeout_ns = COR_WAIT_FOREVER;
    bool asynchronous = false;
};

class Lease : public Handle<cor_lease_t, cor_lease_release> {
public:
    using Handle::Handle;
    cor_status_t acquired() const noexcept { return cor_lease_acquired(get()); }
    cor_status_t register_handler(
        cor_lease_acquisition_fn handler,
        void* context = nullptr
    ) const noexcept {
        return cor_lease_register_handler(get(), handler, context);
    }
    cor_status_t reclaim_memory(
        std::span<const cor_memory_requirement_t> requirements,
        cor_memory_progress_fn handler,
        void* context = nullptr,
        uint64_t timeout_ns = COR_WAIT_FOREVER
    ) const noexcept {
        if (requirements.size() > 4096)
            return COR_INVALID_ARGUMENT;
        const cor_memory_options_t options{
            .size = sizeof(cor_memory_options_t),
            .abi_version = COR_ABI_VERSION,
            .requirements = requirements.data(),
            .requirement_count = static_cast<uint32_t>(requirements.size()),
            .reserved = 0,
            .timeout_ns = timeout_ns
        };
        return cor_lease_reclaim_memory(get(), &options, handler, context);
    }
};

inline cor_status_t cancel_reclaim(cor_reclaim_t* request) noexcept {
    return request ? cor_reclaim_complete(request, COR_CANCELLED, nullptr) : COR_OK;
}
class Reclaim : public Handle<cor_reclaim_t, cancel_reclaim> {
public:
    using Handle::Handle;
    Result<std::vector<cor_memory_requirement_t>> requirements() const {
        uint32_t needed = 0;
        auto status = cor_reclaim_requirements(get(), nullptr, 0, &needed);
        if (status)
            return { .status = status };
        std::vector<cor_memory_requirement_t> values(needed);
        status = cor_reclaim_requirements(get(), values.data(), needed, &needed);
        return { .status = status, .value = std::move(values) };
    }
    cor_status_t complete(
        cor_status_t outcome = COR_OK,
        uint64_t released_bytes = 0
    ) noexcept {
        const cor_memory_release_report_t report{
            .size = sizeof(cor_memory_release_report_t),
            .abi_version = COR_ABI_VERSION,
            .released_bytes = released_bytes
        };
        const auto status = cor_reclaim_complete(get(), outcome, &report);
        if (!status)
            raw_ = nullptr;
        return status;
    }
};

class Work : public Handle<cor_work_t, cor_work_end> {
public:
    using Handle::Handle;
    Result<Lease> acquire(uint64_t timeout_ns = COR_WAIT_FOREVER) const noexcept {
        cor_lease_t* raw = nullptr;
        const auto status = cor_lease_acquire(get(), timeout_ns, &raw);
        return { .status = status, .value = Lease(raw) };
    }
    Result<Lease> acquire(AcquisitionOptions options) const noexcept {
        const cor_acquire_options_t raw_options{
            .size = sizeof(cor_acquire_options_t),
            .abi_version = COR_ABI_VERSION,
            .flags = options.asynchronous ? COR_LEASE_ACQUIRE_FLAGS_ASYNC : 0,
            .reserved = 0,
            .timeout_ns = options.timeout_ns
        };
        cor_lease_t* raw = nullptr;
        const auto status = cor_lease_acquire_ex(get(), &raw_options, &raw);
        return { .status = status, .value = Lease(raw) };
    }
    cor_status_t cancel() const noexcept { return cor_work_cancel(get()); }
};

class Engine : public Handle<cor_engine_t, cor_engine_unregister> {
public:
    using Handle::Handle;
    Result<Work> begin() const noexcept {
        cor_work_t* raw = nullptr;
        const auto status = cor_work_begin(get(), &raw);
        return { .status = status, .value = Work(raw) };
    }
    Result<Work> begin(
        Resource resource,
        WorkIntent intent = WorkIntent::Required
    ) const noexcept {
        const cor_work_options_t options{
            .size = sizeof(cor_work_options_t),
            .abi_version = COR_ABI_VERSION,
            .resource = static_cast<uint32_t>(resource),
            .intent = static_cast<uint32_t>(intent)
        };
        cor_work_t* raw = nullptr;
        const auto status = cor_work_begin_ex(get(), &options, &raw);
        return { .status = status, .value = Work(raw) };
    }
    cor_status_t set_memory_handler(
        cor_memory_handler_fn handler,
        void* context = nullptr
    ) const noexcept {
        return cor_engine_set_memory_handler(get(), handler, context);
    }
    cor_status_t retire() const noexcept { return cor_engine_retire(get()); }
    cor_status_t set_weight(uint32_t weight) const noexcept {
        return cor_engine_set_weight(get(), weight);
    }
    Result<cor_engine_stats_t> stats() const noexcept {
        cor_engine_stats_t stats{};
        stats.size = sizeof(stats);
        stats.abi_version = COR_ABI_VERSION;
        const auto status = cor_engine_stats(get(), &stats);
        return { .status = status, .value = stats };
    }
};

class Context : public Handle<cor_context_t, cor_context_destroy> {
public:
    using Handle::Handle;
    static Result<Context> create(
        uint64_t required_features = 0,
        const char* endpoint = nullptr
    ) noexcept {
        const cor_context_options_t options{
            .size = sizeof(cor_context_options_t),
            .abi_version = COR_ABI_VERSION,
            .required_features = required_features,
            .endpoint = endpoint
        };
        cor_context_t* raw = nullptr;
        const auto status = cor_context_create(&options, &raw);
        return { .status = status, .value = Context(raw) };
    }
    Result<Engine> register_engine(
        const char* label,
        std::span<const int32_t> devices,
        uint32_t weight = 1,
        uint32_t tenancy = COR_TENANCY_SHARED,
        uint64_t capabilities = COR_CAP_COOPERATIVE_YIELD
    ) const noexcept {
        if (devices.size() > UINT32_MAX)
            return { .status = COR_INVALID_ARGUMENT };
        const cor_engine_options_t options{
            .size = sizeof(cor_engine_options_t),
            .abi_version = COR_ABI_VERSION,
            .label = label,
            .device_ids = devices.data(),
            .device_count = static_cast<uint32_t>(devices.size()),
            .weight = weight,
            .tenancy = tenancy,
            .reserved = 0,
            .capabilities = capabilities
        };
        cor_engine_t* raw = nullptr;
        const auto status = cor_engine_register(get(), &options, &raw);
        return { .status = status, .value = Engine(raw) };
    }
};

inline Result<bool> should_yield(
    const Lease& lease,
    uint64_t min_hold_ns = 0
) noexcept {
    uint32_t result = 1;
    const auto status = cor_lease_should_yield(lease.get(), min_hold_ns, &result);
    return { .status = status, .value = result != 0 };
}

} // namespace corby
