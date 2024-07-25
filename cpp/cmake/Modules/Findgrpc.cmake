# Copyright 2024 Crown Copyright
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Try to find the gRPC++ libraries
# See http://gmplib.org/
if(grpc_INCLUDES AND grpc_LIBRARIES)
  set(grpc_FIND_QUIETLY TRUE)
endif(grpc_INCLUDES AND grpc_LIBRARIES)

find_package(PkgConfig REQUIRED)
pkg_check_modules(PC_grpc REQUIRED grpc++>=1.30.2)

find_path(
  grpc_INCLUDE_DIR
  NAMES grpcpp.h
  PATHS ${PC_grpc_INCLUDE_DIRS}
  PATH_SUFFIXES grpcpp REQUIRED)
find_library(
  grpc_LIBRARY
  NAMES grpc++
  PATHS ${PC_grpc_LIBRARY_DIRS} REQUIRED)

set(grpc_VERSION ${PC_grpc_VERSION})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  grpc
  FOUND_VAR grpc_FOUND
  REQUIRED_VARS grpc_LIBRARY grpc_INCLUDE_DIR
  VERSION_VAR grpc_VERSION)

if(grpc_FOUND)
  set(grpc_LIBRARIES ${grpc_LIBRARY})
  set(grpc_INCLUDE_DIRS ${grpc_INCLUDE_DIR})
  set(grpc_DEFINITIONS ${PC_grpc_CFLAGS_OTHER})
endif()

if(grpc_FOUND AND NOT TARGET grpc::grpc)
  add_library(grpc::grpc UNKNOWN IMPORTED)
  set_target_properties(
    grpc::grpc
    PROPERTIES IMPORTED_LOCATION "${grpc_LIBRARY}"
               INTERFACE_COMPILE_OPTIONS "${PC_grpc_CFLAGS_OTHER}"
               INTERFACE_INCLUDE_DIRECTORIES "${grpc_INCLUDE_DIR}")
endif()

mark_as_advanced(grpc_INCLUDE_DIR grpc_LIBRARY)
