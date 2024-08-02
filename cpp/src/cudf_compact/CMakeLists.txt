include(GenerateExportHeader)

add_library(cudf_compact cudf_compact.cpp)

add_library(gpu_compact::cudf_compact ALIAS cudf_compact)

target_link_libraries(cudf_compact PRIVATE gpu_compact_options gpu_compact_warnings)
target_include_directories(cudf_compact ${WARNING_GUARD} PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
                                                                $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/include>)
target_compile_features(cudf_compact PUBLIC cxx_std_20)
target_link_libraries(cudf_compact PUBLIC cudf::cudf)

set_target_properties(
  cudf_compact
  PROPERTIES VERSION ${PROJECT_VERSION}
             CXX_VISIBILITY_PRESET hidden
             VISIBILITY_INLINES_HIDDEN YES)

generate_export_header(cudf_compact EXPORT_FILE_NAME ${PROJECT_BINARY_DIR}/include/gpu_compact/cudf_compact_export.hpp)

if(NOT BUILD_SHARED_LIBS)
  target_compile_definitions(cudf_compact PUBLIC CUDF_COMPACT_STATIC_DEFINE)
endif()
