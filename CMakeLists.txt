cmake_minimum_required(VERSION 3.10)
project(can_tools C OBJC)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Compiler flags
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -O2)
endif()

# Common include path
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/common)

# Common source files
set(COMMON_SOURCES
    common/elm327.c
    common/stn2255.c
    common/can_discover.c
)

# --- can_reader ---
add_executable(can_reader
    can_reader/main.m
    ${COMMON_SOURCES}
)
target_link_libraries(can_reader
    "-framework CoreBluetooth"
    "-framework IOBluetooth"
    "-framework Foundation"
    m
)

# --- can_discover ---
add_executable(can_discover
    can_discover/main.m
    ${COMMON_SOURCES}
)
target_link_libraries(can_discover
    "-framework CoreBluetooth"
    "-framework IOBluetooth"
    "-framework Foundation"
    m
)

message(STATUS "CAN Tools Build Configuration")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  C Compiler: ${CMAKE_C_COMPILER}")
message(STATUS "  Targets: can_reader, can_discover")
