include(GenerateExportHeader)

add_library(
  cudf_compact
  #   cudf_compact.cpp
  filters.cpp
  parquet_types.cpp
  ranges.cu
  ../../include/cudf_compact/common_types.hpp
  ../../include/cudf_compact/cudf_compact.hpp
  ../../include/cudf_compact/filters.hpp
  ../../include/cudf_compact/metadata.hpp
  ../../include/cudf_compact/parquet_types.h
  ../../include/cudf_compact/ranges.hpp)

add_library(gpu_compact::cudf_compact ALIAS cudf_compact)
target_link_libraries(cudf_compact PRIVATE fmt::fmt spdlog::spdlog cudf::cudf)
target_link_libraries(cudf_compact PRIVATE gpu_compact_options gpu_compact_warnings)
target_include_directories(cudf_compact ${WARNING_GUARD} PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
                                                                $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/include>)
target_compile_features(cudf_compact PUBLIC cxx_std_20 cuda_std_20)
# target_compile_options(
#   cudf_compact
#   PRIVATE --expt-relaxed-constexpr
#           --expt-extended-lambda
#           -Xfatbin
#           -compress-all)
# set_target_properties(cudf_compact PROPERTIES CUDA_ARCHITECTURES "75")

set_target_properties(
  cudf_compact
  PROPERTIES VERSION ${PROJECT_VERSION}
             CXX_VISIBILITY_PRESET hidden
             VISIBILITY_INLINES_HIDDEN YES)

generate_export_header(cudf_compact EXPORT_FILE_NAME ${PROJECT_BINARY_DIR}/include/gpu_compact/cudf_compact_export.hpp)

if(NOT BUILD_SHARED_LIBS)
  target_compile_definitions(cudf_compact PUBLIC CUDF_COMPACT_STATIC_DEFINE)
endif()
