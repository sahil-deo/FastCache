cmake_minimum_required(VERSION 3.10)

set(CMAKE_CXX_STANDARD 20)

project(MyRedis)

add_executable(server src/server.cpp)

target_include_directories(server PRIVATE ${CMAKE_SOURCE_DIR}/include)




