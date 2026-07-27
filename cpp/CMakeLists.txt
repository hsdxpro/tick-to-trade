cmake_minimum_required(VERSION 3.24)
project(tick_to_trade_cpp VERSION 0.1.0 LANGUAGES CXX)

option(T2T_TSAN "Compile the tests under ThreadSanitizer" OFF)
option(T2T_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" ON)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(t2t INTERFACE)
target_include_directories(t2t INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")

if(MSVC)
    # C4324: "structure was padded due to alignment specifier". The padding
    # is the point of the alignas on the queue's fields, so the warning is
    # noise here.
    target_compile_options(t2t INTERFACE /W4 /permissive- /EHsc /wd4324)
    if(T2T_WARNINGS_AS_ERRORS)
        target_compile_options(t2t INTERFACE /WX)
    endif()
else()
    target_compile_options(t2t INTERFACE
        -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion -Wshadow
        $<$<CONFIG:Release>:-O3>)
    if(T2T_WARNINGS_AS_ERRORS)
        target_compile_options(t2t INTERFACE -Werror)
    endif()
    if(T2T_TSAN)
        # TSAN is how the memory orderings are checked on this side: it
        # instruments the acquire/release edges, so a weakened ordering is
        # reported as a data race on the slot bytes instead of passing quietly.
        target_compile_options(t2t INTERFACE -fsanitize=thread -O1 -g -fno-omit-frame-pointer)
        target_link_options(t2t INTERFACE -fsanitize=thread)
    endif()
endif()

enable_testing()
add_executable(t2t_test_spsc tests/test_spsc.cpp)
target_link_libraries(t2t_test_spsc PRIVATE t2t)
add_test(NAME t2t_test_spsc COMMAND t2t_test_spsc)

add_executable(t2t_bench_spsc bench/bench_spsc.cpp)
target_link_libraries(t2t_bench_spsc PRIVATE t2t)

add_executable(t2t_test_feed tests/test_feed.cpp)
target_link_libraries(t2t_test_feed PRIVATE t2t)
add_test(NAME t2t_test_feed COMMAND t2t_test_feed)

add_executable(t2t_bench_feed bench/bench_feed.cpp)
target_link_libraries(t2t_bench_feed PRIVATE t2t)

find_package(Threads REQUIRED)
target_link_libraries(t2t_test_spsc PRIVATE Threads::Threads)
target_link_libraries(t2t_bench_spsc PRIVATE Threads::Threads)
