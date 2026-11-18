/*
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#pragma once

#include "api/handles.hpp"

// Never read an appended pointer until its entire slot is covered. A new
// predictor client can still perform legacy compute work on an old runtime.
#define COR_HAS(client, entry, feature) \
    (((client)->features & (feature)) && (!(client)->api || \
    ((client)->api->size >= offsetof(cor_dispatch_v1_t, entry) + sizeof((client)->api->entry) && \
    (client)->api->entry)))
