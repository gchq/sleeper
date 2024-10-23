include(GenerateExportHeader)

add_library(
  io
  s3_sink.cpp
  s3_utils.cpp
  prefetch_source.cpp
  prefetch.cpp
  ../format_helper/format_helper.cpp
  ../../include/io/s3_sink.hpp
  ../../include/io/s3_utils.hpp
  ../../include/io/prefetch_source.hpp
  ../../include/io/prefetch.hpp)

add_library(gpu_compact::io ALIAS io)
target_link_libraries(io PRIVATE fmt::fmt spdlog::spdlog aws-cpp-sdk-s3)
target_link_libraries(io PUBLIC cudf::cudf)
target_link_libraries(io PRIVATE gpu_compact_options gpu_compact_warnings)
target_include_directories(io ${WARNING_GUARD} PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
                                                           $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/include>)
target_compile_features(io PUBLIC cxx_std_20)
target_link_options(
    io
  BEFORE
  INTERFACE
  "LINKER:--copy-dt-needed-entries")

set_target_properties(
    io
  PROPERTIES VERSION ${PROJECT_VERSION}
             CXX_VISIBILITY_PRESET hidden
             VISIBILITY_INLINES_HIDDEN YES)

generate_export_header(io EXPORT_FILE_NAME ${PROJECT_BINARY_DIR}/include/gpu_compact/s3_export.hpp)

if(NOT BUILD_SHARED_LIBS)
  target_compile_definitions(io PUBLIC CUDF_COMPACT_STATIC_DEFINE)
endif()
