/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#pragma once

#include "corby/dispatch.h"

namespace corby::client {

// The provider must remain loaded for the lifetime of clients using this table.
const cor_dispatch_v1_t* discover_dispatch();

} // namespace corby::client
