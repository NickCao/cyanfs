
cmake_minimum_required(VERSION 3.9)
project(fs)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_BUILD_TYPE "Release")
set(CMAKE_SOURCE_DIR src)
include_directories(include)
aux_source_directory(${CMAKE_SOURCE_DIR} DIR)

add_executable(fs ${DIR})
add_library(kv STATIC ${DIR})
set_property(TARGET kv PROPERTY POSITION_INDEPENDENT_CODE TRUE)
install(TARGETS kv)
