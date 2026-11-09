/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <dlfcn.h>
#include <stdio.h>
#include "corby/corby.h"
#include "corby/runtime.h"
#define CHECK(expr) do { if (!(expr)) { fprintf(stderr, "line %d: %s\n", __LINE__, #expr); return __LINE__; } } while (0)

typedef cor_status_t (*register_fn)(cor_engine_t**);
typedef cor_status_t (*begin_fn)(cor_engine_t*, cor_work_t**);
typedef cor_status_t (*acquire_fn)(cor_work_t*, cor_lease_t**);
typedef cor_status_t (*release_fn)(cor_lease_t*);
typedef cor_status_t (*end_fn)(cor_work_t*);
typedef cor_status_t (*unregister_fn)(cor_engine_t*);

int main(int argc, char** argv) {
    cor_runtime_t* runtime = NULL;
    CHECK(cor_runtime_start(NULL, &runtime) == COR_OK);

    CHECK(argc == 3);
    void* a = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
    void* b = dlopen(argv[2], RTLD_NOW | RTLD_LOCAL);
    CHECK(a && b);
    register_fn reg_a = (register_fn)dlsym(a, "plugin_register");
    register_fn reg_b = (register_fn)dlsym(b, "plugin_register");
    begin_fn begin_a = (begin_fn)dlsym(a, "plugin_begin");
    begin_fn begin_b = (begin_fn)dlsym(b, "plugin_begin");
    acquire_fn acquire_a = (acquire_fn)dlsym(a, "plugin_acquire");
    acquire_fn acquire_b = (acquire_fn)dlsym(b, "plugin_acquire");
    release_fn release = (release_fn)dlsym(a, "plugin_release");
    end_fn end = (end_fn)dlsym(a, "plugin_end");
    unregister_fn unreg = (unregister_fn)dlsym(a, "plugin_unregister");
    CHECK(reg_a && reg_b && begin_a && begin_b && acquire_a && acquire_b && release && end && unreg);
    cor_engine_t *ea = 0, *eb = 0;
    cor_work_t *wa = 0, *wb = 0;
    cor_lease_t *la = 0, *lb = 0;
    CHECK(reg_a(&ea) == COR_OK && reg_b(&eb) == COR_OK);
    CHECK(begin_a(ea, &wa) == COR_OK && begin_b(eb, &wb) == COR_OK);
    CHECK(acquire_a(wa, &la) == COR_OK);
    CHECK(acquire_b(wb, &lb) == COR_TIMEOUT && lb == 0);
    CHECK(release(la) == COR_OK);
    CHECK(acquire_b(wb, &lb) == COR_OK);
    CHECK(release(lb) == COR_OK);
    CHECK(end(wa) == COR_OK && end(wb) == COR_OK);
    CHECK(unreg(ea) == COR_OK && unreg(eb) == COR_OK);
    CHECK(dlclose(b) == 0 && dlclose(a) == 0);
    CHECK(cor_runtime_destroy(runtime) == COR_OK);
    return 0;
}
