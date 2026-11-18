/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <cstdlib>
#include <utility>

#include "api/handles.hpp"
#include "api/support.hpp"
#include "transport/discovery.hpp"
#include "transport/socket.hpp"

using namespace corby::client;

extern "C" {

uint32_t cor_abi_version(void) {
    return COR_ABI_VERSION;
}

uint64_t cor_features(void) {
    return 63;
}

cor_status_t cor_context_create(
    const cor_context_options_t* options,
    cor_context_t** result
) {
    return protect([&]() -> cor_status_t {
        if (!result)
            return COR_INVALID_ARGUMENT;
        *result = nullptr;
        uint64_t required = 0;
        const char* endpoint = nullptr;
        if (options) {
            uint32_t size;
            const auto status = prefix(options, 8, size);
            if (status)
                return status;
            if (size >= 16)
                required = options->required_features;
            if (size >= offsetof(cor_context_options_t, endpoint) + sizeof(endpoint))
                endpoint = options->endpoint;
        }
        if (!endpoint) {
            const auto* env = std::getenv("CORBY_ENDPOINT");
            if (env && *env)
                endpoint = env;
        }
        auto client = std::make_shared<Connection>();
        if (endpoint) {
            client->endpoint = endpoint;
            client->control = connect_socket(endpoint);
            if (client->control < 0)
                return COR_UNAVAILABLE;
            Buffer request, response;
            request.u64(required);
            if (!send_frame(client->control, 0, request.bytes) ||
                !read_frame(client->control, 0, response) || response.bytes.size() < 4)
                return COR_UNAVAILABLE;
            const auto status = static_cast<cor_status_t>(response.get(4));
            if (status)
                return status;
            if (response.bytes.size() != 28)
                return COR_UNSUPPORTED;
            client->features = response.get(8);
            if (required & ~client->features)
                return COR_UNSUPPORTED;
            client->token.assign(response.bytes.begin() + 12, response.bytes.end());
            client->reader = std::thread([raw = client.get()] {
                raw->notifications();
            });
        } else {
            client->api = discover_dispatch();
            if (!client->api)
                return COR_UNAVAILABLE;
            client->features = client->api->features;
            const cor_context_options_t core{
                .size = 16,
                .abi_version = COR_ABI_VERSION,
                .required_features = required,
                .endpoint = nullptr
            };
            const auto status = client->api->context_create(&core, &client->native);
            if (status)
                return status;
        }
        *result = new cor_context{client};
        return COR_OK;
    });
}

cor_status_t cor_context_destroy(cor_context_t* context) {
    return protect([&]() -> cor_status_t {
        delete context;
        return COR_OK;
    });
}

} // extern "C"
