/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <cstdlib>
#include "corby/corby.hpp"
int main() {
    unsetenv("CORBY_ENDPOINT");
    auto context = corby::Context::create();
    return context.status == COR_UNAVAILABLE ? 0 : 1;
}
