cmake_minimum_required(VERSION 3.10)
project(mywebserver)
set(CMAKE_CXX_STANDARD 17)
add_executable(mywebserver main.cpp)

add_executable(thread_test thread_test.cpp) # 新增：线程池实验
target_link_libraries(thread_test pthread)    # 新增：链 pthread库