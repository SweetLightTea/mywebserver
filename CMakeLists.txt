cmake_minimum_required(VERSION 3.10)
project(mywebserver)
set(CMAKE_CXX_STANDARD 17)
add_executable(mywebserver main.cpp)
target_link_libraries(mywebserver pthread)    # 新增：链 pthread库