#include(GenerateExportHeader)

# target_include_directories(gpu_proto ${WARNING_GUARD} PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
#                                                              $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/include>)
# target_compile_features(gpu_proto PUBLIC cxx_std_20)

# set_target_properties(
#   gpu_proto
#   PROPERTIES VERSION ${PROJECT_VERSION}
#              CXX_VISIBILITY_PRESET hidden
#              VISIBILITY_INLINES_HIDDEN YES)

# generate_export_header(gpu_proto EXPORT_FILE_NAME ${PROJECT_BINARY_DIR}/include/gpu_compact/gpu_proto_export.hpp)

# if(NOT BUILD_SHARED_LIBS)
#   target_compile_definitions(gpu_proto PUBLIC GPU_PROTO_STATIC_DEFINE)
# endif()
