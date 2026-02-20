cmake_minimum_required(VERSION 3.16)
project(leveldb_kvtool LANGUAGES CXX)

if(NOT DEFINED LEVELDB_SRC_DIR)
  message(FATAL_ERROR "LEVELDB_SRC_DIR is required")
endif()

get_filename_component(LEVELDB_SRC_DIR "${LEVELDB_SRC_DIR}" ABSOLUTE)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(LEVELDB_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(LEVELDB_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(LEVELDB_INSTALL OFF CACHE BOOL "" FORCE)

add_subdirectory("${LEVELDB_SRC_DIR}" "${CMAKE_BINARY_DIR}/leveldb-build")

add_executable(leveldb_kvtool
  leveldb_kvtool.cc
)

target_link_libraries(leveldb_kvtool PRIVATE leveldb)
