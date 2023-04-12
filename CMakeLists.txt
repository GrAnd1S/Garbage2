cmake_minimum_required(VERSION 3.0)

project(hw03)

# -------------------------------------------------------------------
# DO NOT EDIT LINES BELOW lest you break it

# Search for source files.
# If you add a new source to your project, just run ‹cmake -B build -S .›
# or rerun CMake configuration from your IDE.
file(GLOB SOURCES "*.c")

# Test files
file(GLOB TESTS_GLOB "tests/*.c")

# Test utility files
file(GLOB TESTS_LIBS_GLOB "tests/libs/*.c")

set(SOURCES_LIB ${SOURCES})
list(FILTER SOURCES_LIB EXCLUDE REGEX "/?main\\.c$")

foreach (file_path ${TESTS_GLOB})
    message("Found test: " ${file_path})
endforeach ()
set(TEST_SOURCES
    ${TESTS_LIBS_GLOB}
    ${TESTS_GLOB}
    ${SOURCES_LIB}
)

# Targets
set(EXECUTABLE container-explorer)
set(EXECUTABLE_TESTS container-explorer-tests)

# Executable
add_executable(${EXECUTABLE} ${SOURCES} main.c container.h container.c parse_args.h parse_args.c)
target_link_libraries(${EXECUTABLE} m)

# Tests
add_definitions(-DCUT -DWRAP_INDIRECT)
add_executable(${EXECUTABLE_TESTS} ${TEST_SOURCES})
target_link_libraries(${EXECUTABLE_TESTS} m)

# Configure compiler warnings
if (CMAKE_C_COMPILER_ID MATCHES Clang OR ${CMAKE_C_COMPILER_ID} STREQUAL GNU)
    # using regular Clang, AppleClang or GCC
    # Strongly suggested: neable -Werror
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99 -Wall -Wextra -pedantic -g -O0")
elseif (${CMAKE_C_COMPILER_ID} STREQUAL MSVC)
    # using Visual Studio C++
    target_compile_definitions(${EXECUTABLE} PRIVATE _CRT_SECURE_NO_DEPRECATE)
    target_compile_definitions(${EXECUTABLE_TESTS} PRIVATE _CRT_SECURE_NO_DEPRECATE)
    set(CMAKE_CXX_FLAGS "/permissive- /W4 /EHsc")
endif ()

if (MINGW)
    target_compile_definitions(${EXECUTABLE} PRIVATE __USE_MINGW_ANSI_STDIO=1)
    target_compile_definitions(${EXECUTABLE_TESTS} PRIVATE _CRT_SECURE_NO_DEPRECATE)
endif ()
