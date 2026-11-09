/* 
*   Corby
*   Copyright © 2026 NatML Inc. All rights reserved.
*/

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>
#include <poll.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
#include "corby/corby.h"
#include "corby/runtime.h"

extern char** environ;
#define CHECK(x) do { if (!(x)) { std::fprintf(stderr,"line %d: %s\n",__LINE__,#x); return __LINE__ % 250 + 1; } } while (0)

static cor_engine_options_t options(
    const char* label,
    const int32_t* device,
    uint32_t tenancy = 0
) {
    return {
        .size=sizeof(cor_engine_options_t),
        .abi_version=1,
        .label=label,
        .device_ids=device,
        .device_count=1,
        .weight=1,
        .tenancy=tenancy,
        .reserved=0,
        .capabilities=COR_CAP_COOPERATIVE_YIELD
    };
}

static int child(const char* endpoint, int output, bool crash) {
    const cor_context_options_t config{
        .size=sizeof(config),
        .abi_version=1,
        .required_features=15,
        .endpoint=endpoint
    };
    auto unsupported=config; unsupported.required_features=UINT64_C(1)<<63;
    cor_context_t* rejected=nullptr;
    CHECK(cor_context_create(&unsupported,&rejected)==COR_UNSUPPORTED && !rejected);
    cor_context_t* context=nullptr; CHECK(cor_context_create(&config,&context)==COR_OK);
    const int32_t device=crash ? 2 : 0;
    const auto config_engine=options(crash ? "crash" : "worker",&device);
    cor_engine_t* engine=nullptr; CHECK(cor_engine_register(context,&config_engine,&engine)==COR_OK);
    if (!crash) {
        uint32_t needed=0;
        CHECK(cor_context_engine_ids(context,nullptr,0,&needed)==COR_OK);
        std::vector<uint64_t> ids(needed);
        CHECK(cor_context_engine_ids(context,ids.data(),ids.size(),&needed)==COR_OK);
        bool found=false;
        for (auto id:ids) {
            char label[128];
            CHECK(cor_context_engine_label(context,id,label,sizeof(label),&needed)==COR_OK);
            if (std::strcmp(label,"worker")) continue;
            found=true;
            int32_t devices[2];
            CHECK(cor_context_engine_devices(context,id,devices,2,&needed)==COR_OK && needed==1 && devices[0]==0);
            CHECK(cor_context_engine_set_weight(context,id,3)==COR_OK);
            cor_engine_stats_t stats{ .size=sizeof(stats), .abi_version=1 };
            CHECK(cor_context_engine_stats(context,id,&stats)==COR_OK && stats.weight==3);
        }
        CHECK(found);
        CHECK(cor_engine_set_weight(engine,1)==COR_OK);
        char label[128];
        CHECK(cor_engine_label(engine,label,sizeof(label),&needed)==COR_OK && !std::strcmp(label,"worker"));
    }
    cor_work_t* work=nullptr; CHECK(cor_work_begin(engine,&work)==COR_OK);
    if (!crash) {
        std::atomic<cor_status_t> status{COR_INTERNAL};
        std::thread waiter([&] {
            try { cor_lease_t* lease=nullptr; status.store(cor_lease_acquire(work,COR_WAIT_FOREVER,&lease)); }
            catch (...) { status.store(COR_INTERNAL); }
        });
        const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(3);
        cor_engine_stats_t stats{};
        do {
            stats={ .size=sizeof(stats), .abi_version=1 };
            CHECK(cor_engine_stats(engine,&stats)==COR_OK);
            if (stats.waiting) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } while (std::chrono::steady_clock::now()<deadline);
        CHECK(stats.waiting==1);
        CHECK(cor_work_cancel(work)==COR_OK);
        waiter.join(); CHECK(status.load()==COR_CANCELLED);
        CHECK(cor_work_end(work)==COR_OK);
        CHECK(cor_work_begin(engine,&work)==COR_OK);
        cor_lease_t* timed=nullptr;
        CHECK(cor_lease_acquire(work,1'000'000,&timed)==COR_TIMEOUT && !timed);
        CHECK(write(output,"R",1)==1);
    }
    cor_lease_t* lease=nullptr; CHECK(cor_lease_acquire(work,5'000'000'000,&lease)==COR_OK);
    CHECK(write(output,"A",1)==1);
    if (crash) _exit(0); // Deliberately omit every Corby destructor.
    const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(5);
    uint32_t yield=0;
    while (!yield && std::chrono::steady_clock::now()<deadline) {
        CHECK(cor_lease_should_yield(lease,UINT64_MAX,&yield)==COR_OK);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    CHECK(yield==1); CHECK(write(output,"Y",1)==1);
    CHECK(cor_lease_release(lease)==COR_OK);
    CHECK(cor_work_end(work)==COR_OK);
    CHECK(cor_engine_unregister(engine)==COR_OK);
    const int32_t free_device=3;
    const auto extra_config=options("release-order",&free_device);
    CHECK(cor_engine_register(context,&extra_config,&engine)==COR_OK);
    for (int32_t i=0;i<32;++i) {
        CHECK(cor_work_begin(engine,&work)==COR_OK);
        CHECK(cor_lease_acquire(work,0,&lease)==COR_OK);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        CHECK(cor_lease_release(lease)==COR_OK);
        // A yield observer must not extend ownership past a release reply.
        CHECK(cor_work_end(work)==COR_OK);
    }
    CHECK(cor_engine_unregister(engine)==COR_OK);
    CHECK(cor_context_destroy(context)==COR_OK);
    return 0;
}

static bool event(int input, char expected) {
    pollfd fd{ .fd=input, .events=POLLIN, .revents=0 };
    if (poll(&fd,1,7000)!=1) return false;
    char value=0; return read(input,&value,1)==1 && value==expected;
}

static pid_t spawn(
    const char* executable,
    const char* endpoint,
    int output,
    bool crash
) {
    const auto descriptor=std::to_string(output);
    char* args[]={const_cast<char*>(executable),const_cast<char*>(crash ? "--crash" : "--worker"),const_cast<char*>(endpoint),const_cast<char*>(descriptor.c_str()),nullptr};
    pid_t pid=0;
    if (posix_spawn(&pid,executable,nullptr,nullptr,args,environ)!=0)
        return -1;
    return pid;
}

int main(int argc, char** argv) {
    if (argc==4)
        return child(argv[2],std::atoi(argv[3]),std::strcmp(argv[1],"--crash")==0);
#ifdef CORBY_IPC_WORKER
    return 1;
#else
    CHECK(argc==2);
    const auto* worker_executable=argv[1];
    const auto path="/tmp/corby-ipc-test-"+std::to_string(getpid())+".sock";
    const cor_runtime_options_t config{ .size=sizeof(config), .abi_version=1, .scope=COR_SCOPE_SYSTEM, .reserved=0, .endpoint=path.c_str() };
    cor_runtime_t* runtime=nullptr; CHECK(cor_runtime_start(&config,&runtime)==COR_OK);
    cor_runtime_t* duplicate=nullptr; CHECK(cor_runtime_start(nullptr,&duplicate)==COR_CONFLICT && !duplicate);
    cor_context_t* context=nullptr; CHECK(cor_context_create(nullptr,&context)==COR_OK);
    const int32_t device=0;
    const auto shared_config=options("host-shared",&device);
    cor_engine_t* shared=nullptr; CHECK(cor_engine_register(context,&shared_config,&shared)==COR_OK);
    cor_work_t* work=nullptr; CHECK(cor_work_begin(shared,&work)==COR_OK);
    cor_lease_t* held=nullptr; CHECK(cor_lease_acquire(work,0,&held)==COR_OK);
    int pipefd[2]; CHECK(pipe(pipefd)==0);
    const auto worker=spawn(worker_executable,path.c_str(),pipefd[1],false); CHECK(worker>0);
    CHECK(event(pipefd[0],'R'));
    CHECK(cor_lease_release(held)==COR_OK);
    CHECK(event(pipefd[0],'A'));
    const auto owner_config=options("owner",&device,COR_TENANCY_DEDICATED);
    cor_engine_t* owner=nullptr; CHECK(cor_engine_register(context,&owner_config,&owner)==COR_OK);
    cor_work_t* demand=nullptr; CHECK(cor_work_begin(owner,&demand)==COR_OK);
    CHECK(event(pipefd[0],'Y'));
    int status=0; CHECK(waitpid(worker,&status,0)==worker && WIFEXITED(status) && WEXITSTATUS(status)==0);
    CHECK(cor_work_end(demand)==COR_OK); CHECK(cor_engine_unregister(owner)==COR_OK);
    CHECK(cor_work_end(work)==COR_OK); CHECK(cor_engine_unregister(shared)==COR_OK);

    // A dead/disconnected client must never cause an uncertain lease to be
    // automatically regranted. CPU simulation deliberately leaves it pinned.
    const auto crashed=spawn(worker_executable,path.c_str(),pipefd[1],true); CHECK(crashed>0);
    CHECK(event(pipefd[0],'A')); CHECK(waitpid(crashed,&status,0)==crashed && WEXITSTATUS(status)==0);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    const int32_t quarantined_device=2;
    const auto peer_config=options("peer",&quarantined_device);
    cor_engine_t* peer=nullptr; CHECK(cor_engine_register(context,&peer_config,&peer)==COR_OK);
    CHECK(cor_work_begin(peer,&work)==COR_OK);
    held=nullptr; CHECK(cor_lease_acquire(work,0,&held)==COR_TIMEOUT && !held);
    CHECK(cor_work_end(work)==COR_OK); CHECK(cor_engine_unregister(peer)==COR_OK);
    CHECK(cor_runtime_shutdown(runtime)==COR_BUSY);
    CHECK(cor_context_destroy(context)==COR_OK);
    close(pipefd[0]); close(pipefd[1]);
    // No recovery/fencing is faked: the process ends with the quarantined
    // runtime still pinned, just as a supervisor must restart its worker group.
    return 0;
#endif
}
