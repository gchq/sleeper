include(GenerateExportHeader)

add_library(
  s3utils
  s3_sink.cpp
  s3_utils.cpp
  ../../include/s3/s3_sink.hpp
  ../../include/s3/s3_utils.hpp)

add_library(gpu_compact::s3 ALIAS s3utils)
target_link_libraries(s3utils PRIVATE fmt::fmt spdlog::spdlog aws-cpp-sdk-s3)
target_link_libraries(s3utils PUBLIC cudf::cudf)
target_link_libraries(s3utils PRIVATE gpu_compact_options gpu_compact_warnings)
target_include_directories(s3utils ${WARNING_GUARD} PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
                                                           $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/include>)
target_compile_features(s3utils PUBLIC cxx_std_20)
target_link_options(
  s3utils
  BEFORE
  INTERFACE
  "LINKER:--copy-dt-needed-entries")

set_target_properties(
  s3utils
  PROPERTIES VERSION ${PROJECT_VERSION}
             CXX_VISIBILITY_PRESET hidden
             VISIBILITY_INLINES_HIDDEN YES)

generate_export_header(s3utils EXPORT_FILE_NAME ${PROJECT_BINARY_DIR}/include/gpu_compact/s3_export.hpp)

if(NOT BUILD_SHARED_LIBS)
  target_compile_definitions(s3utils PUBLIC CUDF_COMPACT_STATIC_DEFINE)
endif()
