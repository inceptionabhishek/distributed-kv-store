cmake_minimum_required(VERSION 3.16)
project(kv_store_stage1 CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(include)

add_executable(kv_store_main
    src/main.cpp
    src/kv_store.cpp
)