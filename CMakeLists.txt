cmake_minimum_required(VERSION 3.16)
project(kv_store_stage2 CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

# --- Protobuf/gRPC discovery ---
# macOS (Homebrew) ships proper CMake Config-mode packages, so we use those
# directly. Ubuntu's apt packages (used by our CI runner) only ship
# pkg-config .pc files for these -- no CMake config -- so on non-Apple
# platforms we go through pkg-config instead. Same libraries, different
# packaging conventions per platform; this branch just adapts to that.
if(APPLE)
    list(APPEND CMAKE_PREFIX_PATH /opt/homebrew/opt/grpc /opt/homebrew/opt/protobuf)
    find_package(Protobuf CONFIG REQUIRED)
    find_package(gRPC CONFIG REQUIRED)

    set(PROTOBUF_LIB protobuf::libprotobuf)
    set(GRPC_LIB gRPC::grpc++)
    set(PROTOC_CMD protobuf::protoc)
else()
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(PROTOBUF_PC REQUIRED IMPORTED_TARGET protobuf)
    pkg_check_modules(GRPCPP_PC REQUIRED IMPORTED_TARGET grpc++)

    set(PROTOBUF_LIB PkgConfig::PROTOBUF_PC)
    set(GRPC_LIB PkgConfig::GRPCPP_PC)

    find_program(PROTOC_EXECUTABLE protoc REQUIRED)
    set(PROTOC_CMD ${PROTOC_EXECUTABLE})
endif()

find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

# --- Generate C++ code from kvstore.proto at build time ---
set(PROTO_FILE "${CMAKE_SOURCE_DIR}/proto/kvstore.proto")
set(GENERATED_DIR "${CMAKE_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GENERATED_DIR})

set(PROTO_SRCS "${GENERATED_DIR}/kvstore.pb.cc")
set(PROTO_HDRS "${GENERATED_DIR}/kvstore.pb.h")
set(GRPC_SRCS  "${GENERATED_DIR}/kvstore.grpc.pb.cc")
set(GRPC_HDRS  "${GENERATED_DIR}/kvstore.grpc.pb.h")

add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND ${PROTOC_CMD}
    ARGS
        --cpp_out=${GENERATED_DIR}
        --grpc_out=${GENERATED_DIR}
        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
        -I "${CMAKE_SOURCE_DIR}/proto"
        "${PROTO_FILE}"
    DEPENDS "${PROTO_FILE}"
    COMMENT "Generating gRPC/protobuf code from kvstore.proto"
)

add_library(kvstore_proto ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS})
target_include_directories(kvstore_proto PUBLIC ${GENERATED_DIR})
target_link_libraries(kvstore_proto PUBLIC ${GRPC_LIB} ${PROTOBUF_LIB})

include_directories(include)

# --- Stage 1 core store (still used, now wrapped by the server) ---
add_library(kv_core src/kv_store.cpp)

# --- Server ---
add_executable(kv_server src/server.cpp)
target_link_libraries(kv_server PRIVATE kv_core kvstore_proto ${GRPC_LIB} ${PROTOBUF_LIB} Threads::Threads)

# --- Client ---
add_executable(kv_client src/client.cpp)
target_link_libraries(kv_client PRIVATE kvstore_proto ${GRPC_LIB} ${PROTOBUF_LIB} Threads::Threads)

# --- Router (Stage 3/4: hash(key) % N and consistent hashing across nodes) ---
add_executable(kv_router src/router.cpp)
target_link_libraries(kv_router PRIVATE kvstore_proto ${GRPC_LIB} ${PROTOBUF_LIB} Threads::Threads)


# --- Failure detector demo (Stage 7) ---
add_executable(fd_demo src/fd_demo.cpp)
target_link_libraries(fd_demo PRIVATE kvstore_proto ${GRPC_LIB} ${PROTOBUF_LIB} Threads::Threads)

# --- Unit tests (GoogleTest) ---
enable_testing()
find_package(GTest REQUIRED)

add_executable(kv_tests
    tests/test_kv_store.cpp
    tests/test_consistent_hash_ring.cpp
    src/kv_store.cpp
)
target_link_libraries(kv_tests PRIVATE GTest::gtest GTest::gtest_main Threads::Threads)

include(GoogleTest)
gtest_discover_tests(kv_tests)