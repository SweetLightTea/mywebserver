cmake_minimum_required(VERSION 3.10)
project(mywebserver)
set(CMAKE_CXX_STANDARD 17)
add_executable(mywebserver main.cpp web_server.cpp config/config.cpp)
target_include_directories(mywebserver PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/config)   # 【L29】让 config.h 可以裸名 include
target_link_libraries(mywebserver pthread mysqlclient)    # L24: 多链一个 mysqlclient 库