/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#pragma once

#include <span>
#include <utility>

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
private:
    T* raw_ = nullptr;
};

using Lease = Handle<cor_lease_t, cor_lease_release>;

class Work : public Handle<cor_work_t, cor_work_end> {
public:
    using Handle::Handle;
    Result<Lease> acquire(uint64_t timeout_ns = COR_WAIT_FOREVER) const noexcept {
        cor_lease_t* raw = nullptr;
        const auto status = cor_lease_acquire(get(), timeout_ns, &raw);
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
