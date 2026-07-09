cmake_minimum_required(VERSION 3.16)
project(kv_store_stage2 CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

list(APPEND CMAKE_PREFIX_PATH /opt/homebrew/opt/grpc /opt/homebrew/opt/protobuf)

find_package(Threads REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

set(PROTO_FILE "${CMAKE_SOURCE_DIR}/proto/kvstore.proto")
set(GENERATED_DIR "${CMAKE_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GENERATED_DIR})

set(PROTO_SRCS "${GENERATED_DIR}/kvstore.pb.cc")
set(PROTO_HDRS "${GENERATED_DIR}/kvstore.pb.h")
set(GRPC_SRCS  "${GENERATED_DIR}/kvstore.grpc.pb.cc")
set(GRPC_HDRS  "${GENERATED_DIR}/kvstore.grpc.pb.h")

add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND protobuf::protoc
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
target_link_libraries(kvstore_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)

include_directories(include)

add_library(kv_core src/kv_store.cpp)

add_executable(kv_server src/server.cpp)
target_link_libraries(kv_server PRIVATE kv_core kvstore_proto gRPC::grpc++ protobuf::libprotobuf Threads::Threads)

add_executable(kv_client src/client.cpp)
target_link_libraries(kv_client PRIVATE kvstore_proto gRPC::grpc++ protobuf::libprotobuf Threads::Threads)


# --- Router (Stage 3: hash(key) % N across multiple nodes) ---
add_executable(kv_router src/router.cpp)
target_link_libraries(kv_router PRIVATE kvstore_proto gRPC::grpc++ protobuf::libprotobuf Threads::Threads)

# --- Ring demo (Stage 4: proves consistent hashing moves far fewer keys) ---
add_executable(ring_demo src/ring_demo.cpp)