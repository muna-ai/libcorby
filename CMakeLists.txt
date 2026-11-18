cmake_minimum_required(VERSION 3.20)
project(Corby VERSION 0.1.0 LANGUAGES C CXX)

# Config
option(CORBY_BUILD_RUNTIME "Build the optional Rust host runtime" OFF)
option(CORBY_BUILD_TESTS "Build client and interoperability tests" ON)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
find_package(Threads REQUIRED)
if(NOT UNIX)
    message(FATAL_ERROR "Corby currently supports Linux and macOS")
endif()

# Define the client library
add_library(corby_client STATIC
    client/api/callbacks.cpp
    client/api/extensions.cpp
    client/api/memory.cpp
    client/api/context.cpp
    client/api/engine.cpp
    client/api/lease.cpp
    client/api/supervisor.cpp
    client/api/support.cpp
    client/api/work.cpp
    client/transport/connection.cpp
    client/transport/discovery.cpp
    client/transport/protocol.cpp
    client/transport/socket.cpp
)
add_library(Corby::client ALIAS corby_client)

# Configure client
set_target_properties(
    corby_client PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN YES
    EXPORT_NAME client
)
target_compile_features(corby_client PUBLIC cxx_std_20)
target_include_directories(
    corby_client PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/client"
    PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(
    corby_client PUBLIC 
    Threads::Threads
)

# Install rules
install(
    TARGETS corby_client
    EXPORT CorbyTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(
    DIRECTORY include/corby
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
install(
    EXPORT CorbyTargets
    NAMESPACE Corby:: DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/Corby
)
configure_package_config_file(
    cmake/CorbyConfig.cmake.in "${CMAKE_CURRENT_BINARY_DIR}/CorbyConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/Corby
    PATH_VARS CMAKE_INSTALL_LIBDIR
)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/CorbyConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)
install(
    FILES
        "${CMAKE_CURRENT_BINARY_DIR}/CorbyConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/CorbyConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/Corby
)

# Runtime
if(CORBY_BUILD_RUNTIME)
    find_program(CARGO_EXECUTABLE cargo REQUIRED)
    set(CORBY_LIBRARY "${CMAKE_CURRENT_BINARY_DIR}/cargo/release/${CMAKE_SHARED_LIBRARY_PREFIX}corby${CMAKE_SHARED_LIBRARY_SUFFIX}")
    file(GLOB_RECURSE CORBY_RUST_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/*.rs")
    add_custom_command(
        OUTPUT "${CORBY_LIBRARY}"
        COMMAND "${CARGO_EXECUTABLE}" build --release --offline --locked
            --manifest-path "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml"
            --target-dir "${CMAKE_CURRENT_BINARY_DIR}/cargo"
        DEPENDS ${CORBY_RUST_SOURCES} Cargo.toml Cargo.lock build.rs VERBATIM
    )
    add_custom_target(corby_runtime_build ALL DEPENDS "${CORBY_LIBRARY}")
    add_library(corby_runtime SHARED IMPORTED GLOBAL)
    add_library(Corby::runtime ALIAS corby_runtime)
    set_target_properties(
        corby_runtime PROPERTIES
        IMPORTED_LOCATION "${CORBY_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_SOURCE_DIR}/include"
    )
    if(APPLE)
        set_target_properties(
            corby_runtime PROPERTIES
            IMPORTED_SONAME "@rpath/libcorby.dylib"
        )
    else()
        set_target_properties(
            corby_runtime PROPERTIES
            IMPORTED_SONAME "libcorby.so"
        )
    endif()
    add_dependencies(corby_runtime corby_runtime_build)
    install(FILES "${CORBY_LIBRARY}" DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

# Tests
if(CORBY_BUILD_TESTS)
    enable_testing()
    add_executable(corby_client_only_test tests/client_only.cpp)
    target_link_libraries(corby_client_only_test PRIVATE Corby::client)
    add_test(NAME client_without_runtime COMMAND corby_client_only_test)
    if(CORBY_BUILD_RUNTIME)
        foreach(test IN ITEMS c_api cpp_api)
            if(test STREQUAL "c_api")
                set(source tests/c_api.c)
            else()
                set(source tests/cpp_api.cpp)
            endif()
            add_executable(corby_${test}_test ${source})
            target_link_libraries(
                corby_${test}_test PRIVATE
                Corby::client
                Corby::runtime
            )
            add_test(NAME ${test} COMMAND corby_${test}_test)
        endforeach()
        add_executable(corby_memory_test tests/memory.cpp)
        target_link_libraries(corby_memory_test PRIVATE Corby::client Corby::runtime)
        add_executable(corby_memory_worker tests/memory.cpp)
        target_compile_definitions(corby_memory_worker PRIVATE CORBY_MEMORY_WORKER=1)
        target_link_libraries(corby_memory_worker PRIVATE Corby::client)
        add_dependencies(corby_memory_test corby_memory_worker)
        add_test(NAME memory_leases COMMAND corby_memory_test "$<TARGET_FILE:corby_memory_worker>")
        set_tests_properties(memory_leases PROPERTIES TIMEOUT 30 ENVIRONMENT "CORBY_ENDPOINT=")
        foreach(plugin IN ITEMS a b)
            add_library(corby_plugin_${plugin} MODULE tests/plugin.c)
            target_link_libraries(corby_plugin_${plugin} PRIVATE Corby::client)
        endforeach()
        add_executable(corby_dlopen_test tests/dlopen.c)
        target_include_directories(corby_dlopen_test PRIVATE include)
        target_link_libraries(corby_dlopen_test PRIVATE Corby::runtime ${CMAKE_DL_LIBS})
        add_dependencies(corby_dlopen_test corby_plugin_a corby_plugin_b)
        add_test(
            NAME independent_static_clients COMMAND corby_dlopen_test
            "$<TARGET_FILE:corby_plugin_a>" "$<TARGET_FILE:corby_plugin_b>"
        )
        add_executable(corby_ipc_test tests/ipc.cpp)
        target_link_libraries(
            corby_ipc_test PRIVATE
            Corby::client
            Corby::runtime
        )
        add_executable(corby_ipc_worker tests/ipc.cpp)
        target_compile_definitions(corby_ipc_worker PRIVATE CORBY_IPC_WORKER=1)
        target_link_libraries(corby_ipc_worker PRIVATE Corby::client)
        add_dependencies(corby_ipc_test corby_ipc_worker)
        add_test(
            NAME system_scope
            COMMAND corby_ipc_test "$<TARGET_FILE:corby_ipc_worker>"
        )
        set_tests_properties(
            c_api cpp_api independent_static_clients system_scope
            PROPERTIES TIMEOUT 30
        )
        set_tests_properties(
            c_api cpp_api independent_static_clients system_scope
            PROPERTIES ENVIRONMENT "CORBY_ENDPOINT="
        )
    endif()
endif()
