cmake_minimum_required(VERSION 3.18...3.23)

# Enable CMake policy from latest current version
if(${CMAKE_VERSION} VERSION_LESS 3.12)
    cmake_policy(VERSION ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION})
endif()

include(cmake/StandardProjectSettings.cmake)
include(cmake/PreventInSourceBuilds.cmake)

project(
    Compaction
    VERSION 1.0
    DESCRIPTION "GPU compaction"
    HOMEPAGE_URL "https://github.com/gchq/sleeper"
    LANGUAGES CXX CUDA)

# Enable later C++ standard and disable GNU extensions
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable the same for CUDA compiled code
set(CMAKE_CUDA_STANDARD 17)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)

# Set CUDA architecture for Tesla T4
set(CMAKE_CUDA_ARCHITECTURES 75)

# Link this 'library' to set the c++ standard / compile-time options requested
add_library(project_options INTERFACE)
target_compile_features(project_options INTERFACE cxx_std_17)

# Link this 'library' to use the warnings specified in CompilerWarnings.cmake
add_library(project_warnings INTERFACE)

# enable cache system
include(cmake/Cache.cmake)

# standard compiler warnings
include(cmake/CompilerWarnings.cmake)
set_project_warnings(project_warnings)

# sanitizer options if supported by compiler
include(cmake/Sanitizers.cmake)
enable_sanitizers(project_options)

# allow for static analysis options
include(cmake/StaticAnalyzers.cmake)

option(ENABLE_PCH "Enable precompiled headers" OFF)
if(ENABLE_PCH)
    target_precompile_headers(
        project_options
        INTERFACE
        <vector>
        <string>
        <iostream>
        <memory>
        <utility>)
endif()

# Find some libraries
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
find_package(Arrow REQUIRED)

# We might need to set Parquet_DIR manually!
find_package(Parquet REQUIRED)
find_package(cudf REQUIRED)
find_package(gmp REQUIRED)

add_library(histogram STATIC src/main/c/src/histo.cpp src/main/c/include/histo.hpp)
target_include_directories(histogram PUBLIC src/main/c/include)
target_link_libraries(histogram PRIVATE gmp::gmp project_warnings project_options)

find_package(
    AWSSDK CONFIG
    COMPONENTS s3
    REQUIRED)

# S3 Interaction code
add_library(s3_utils STATIC src/main/c/src/s3_utils.cpp src/main/c/include/s3_utils.hpp
                            src/main/c/src/temp_file.cpp src/main/c/include/temp_file.hpp)
target_include_directories(s3_utils PUBLIC src/main/c/include)
target_include_directories(s3_utils INTERFACE ${AWSSDK_INCLUDE_DIRS})
target_link_libraries(s3_utils PRIVATE ${AWSSDK_LIBRARIES} project_warnings project_options)

# Build the filters library
add_library(
    filters STATIC
    src/main/c/src/range_filter_int32.cu src/main/c/src/range_filter_int64.cu
    src/main/c/src/range_filter_string.cu src/main/c/include/range_filter.cuh
    src/main/c/include/filters.h)
target_include_directories(filters PUBLIC src/main/c/include)
target_compile_options(filters PRIVATE --expt-relaxed-constexpr --expt-extended-lambda -Xfatbin
                                       -compress-all -Xcompiler -pthread)
target_link_libraries(filters PRIVATE cudf::cudf project_options)

add_executable(
    cukeydist
    src/main/c/src/cukeydist.cpp
    src/main/c/src/retrieve_first_last.cpp
    src/main/c/src/cukeydist_utils.cpp
    src/main/c/include/cukeydist_utils.hpp
    src/main/c/src/output_split.cpp
    src/main/c/src/s3_waiting_source.cpp
    src/main/c/src/s3_writer.cpp
    src/main/c/include/output_split.hpp
    src/main/c/include/cukeydist.hpp
    src/main/c/include/s3_waiting_source.hpp
    src/main/c/include/s3_writer.hpp)
target_link_libraries(cukeydist PUBLIC cudf::cudf filters parquet_shared histogram s3_utils)
target_link_libraries(cukeydist PRIVATE project_warnings project_options)