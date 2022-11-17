cmake_minimum_required(VERSION 3.18...3.23)

# Enable CMake policy from latest current version
if(${CMAKE_VERSION} VERSION_LESS 3.12)
  cmake_policy(VERSION ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION})
endif()

# Do this before project() to skip the compiler config process
if("${CMAKE_BINARY_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}")
  message(FATAL_ERROR "In-source builds are disabled.
    Please create a subfolder and use `cmake ..` inside it.
    NOTE: cmake will now create CMakeCache.txt and CMakeFiles/*.
          You must delete them, or cmake will refuse to work.")
endif()

project(Compaction VERSION 1.0
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

# We should probably set this based on each target or whether it is Release or Debug instead.
add_compile_options(-O3 -march=native)

# Find some libraries
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
find_package(Arrow REQUIRED)

# We might need to set Parquet_DIR manually!
find_package(Parquet REQUIRED)
find_package(cudf REQUIRED)
find_package(gmp REQUIRED)

add_library(histogram STATIC src/main/c/src/histo.cpp src/main/c/include/histo.hpp)
target_include_directories(histogram PUBLIC src/main/c/include)
target_link_libraries(histogram PRIVATE gmp::gmp)

find_package(AWSSDK CONFIG COMPONENTS s3 REQUIRED)

# S3 Interaction code
add_library(s3_utils STATIC src/main/c/src/s3_utils.cpp src/main/c/include/s3_utils.hpp src/main/c/src/temp_file.cpp src/main/c/include/temp_file.hpp)
target_include_directories(s3_utils PUBLIC src/main/c/include)
target_include_directories(s3_utils INTERFACE ${AWSSDK_INCLUDE_DIRS})
target_link_libraries(s3_utils PRIVATE ${AWSSDK_LIBRARIES})

# Build the filters library
add_library(filters STATIC src/main/c/src/range_filter_int32.cu src/main/c/src/range_filter_int64.cu src/main/c/src/range_filter_string.cu src/main/c/include/range_filter.cuh src/main/c/include/filters.h)
target_include_directories(filters PUBLIC src/main/c/include)
target_compile_options(filters PRIVATE --expt-relaxed-constexpr --expt-extended-lambda -Xfatbin -compress-all -Xcompiler -pthread)
target_link_libraries(filters PRIVATE cudf::cudf)

add_executable(cukeydist src/main/c/src/cukeydist.cpp src/main/c/src/cukeydist_utils.cpp src/main/c/include/cukeydist_utils.hpp src/main/c/src/output_split.cpp src/main/c/src/s3_waiting_source.cpp src/main/c/src/s3_writer.cpp src/main/c/include/output_split.hpp src/c/main/c/include/cukeydist.hpp src/main/c/include/s3_waiting_source.hpp src/main/c/include/s3_writer.hpp)
target_link_libraries(cukeydist PUBLIC cudf::cudf filters parquet_shared histogram s3_utils)
