include(GenerateExportHeader)

add_library(format_helper format_helper.cpp ../../include/format_helper/format_helper.hpp)

add_library(gpu_compact::format_helper ALIAS format_helper)
target_link_libraries(format_helper PRIVATE fmt::fmt)
target_link_libraries(format_helper PRIVATE gpu_compact_options gpu_compact_warnings)
target_include_directories(format_helper ${WARNING_GUARD} PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
                                                                 $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/include>)
target_compile_features(format_helper PUBLIC cxx_std_20)
# target_link_options(
#     format_helper
#   BEFORE
#   INTERFACE
#   "LINKER:--copy-dt-needed-entries")

set_target_properties(
  format_helper
  PROPERTIES VERSION ${PROJECT_VERSION}
             CXX_VISIBILITY_PRESET hidden
             VISIBILITY_INLINES_HIDDEN YES)

generate_export_header(format_helper EXPORT_FILE_NAME
                       ${PROJECT_BINARY_DIR}/include/gpu_compact/format_helper_export.hpp)

if(NOT BUILD_SHARED_LIBS)
  target_compile_definitions(format_helper PUBLIC CUDF_COMPACT_STATIC_DEFINE)
endif()
