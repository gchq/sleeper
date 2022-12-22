# Copyright 2022 Crown Copyright
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

# Try to find the GNU Multiple Precision Arithmetic Library (GMP)
# See http://gmplib.org/
if (gmp_INCLUDES AND gmp_LIBRARIES)
  set(gmp_FIND_QUIETLY TRUE)
endif (gmp_INCLUDES AND gmp_LIBRARIES)

find_package(PkgConfig REQUIRED)
pkg_check_modules(PC_gmp REQUIRED gmp>=6.2.0)

find_path(gmp_INCLUDE_DIR
  NAMES gmp.h
  PATHS ${PC_gmp_INCLUDE_DIRS}
  PATH_SUFFIXES gmp
)
find_library(gmp_LIBRARY
  NAMES gmp
  PATHS ${PC_gmp_LIBRARY_DIRS}
)

set(gmp_VERSION ${PC_gmp_VERSION})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(gmp
  FOUND_VAR gmp_FOUND
  REQUIRED_VARS
    gmp_LIBRARY
    gmp_INCLUDE_DIR
  VERSION_VAR gmp_VERSION
)

if(gmp_FOUND)
  set(gmp_LIBRARIES ${gmp_LIBRARY})
  set(gmp_INCLUDE_DIRS ${gmp_INCLUDE_DIR})
  set(gmp_DEFINITIONS ${PC_gmp_CFLAGS_OTHER})
endif()

if(gmp_FOUND AND NOT TARGET gmp::gmp)
  add_library(gmp::gmp UNKNOWN IMPORTED)
  set_target_properties(gmp::gmp PROPERTIES
    IMPORTED_LOCATION "${gmp_LIBRARY}"
    INTERFACE_COMPILE_OPTIONS "${PC_gmp_CFLAGS_OTHER}"
    INTERFACE_INCLUDE_DIRECTORIES "${gmp_INCLUDE_DIR}"
  )
endif()

mark_as_advanced(
  gmp_INCLUDE_DIR
  gmp_LIBRARY
)