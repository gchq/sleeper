include(GenerateExportHeader)

add_library(gpu_proto ${CMAKE_SOURCE_DIR}/../java/compaction/compaction-gpu/proto/ProtoCompaction.proto)
add_library(gpu_compact::compact_grpc ALIAS gpu_proto)
set_target_properties(gpu_proto PROPERTIES CXX_CLANG_TIDY "") # Disable clang-tidy
protobuf_generate(
  TARGET
  gpu_proto
  LANGUAGE
  cpp
  APPEND_PATH)

protobuf_generate(
  TARGET
  gpu_proto
  LANGUAGE
  grpc
  GENERATE_EXTENSIONS
  .grpc.pb.cc
  .grpc.pb.h
  PLUGIN
  "protoc-gen-grpc=${grpc_CPP_PLUGIN_LOCATION}"
  OUT_VAR
  gpu_proto_gen_srcs
  APPEND_PATH)

list(
  GET
  gpu_proto_gen_srcs
  0
  gpu_proto_gen_file)
cmake_path(
  GET
  gpu_proto_gen_file
  PARENT_PATH
  GPU_PROTO_INCLUDE)
target_include_directories(gpu_proto SYSTEM INTERFACE ${GPU_PROTO_INCLUDE})
target_link_libraries(gpu_proto PRIVATE gpu_compact::gpu_compact_options
)# Don't enable compiler warnings on Protobuf files
target_link_libraries(gpu_proto PUBLIC grpc::grpc protobuf::libprotobuf)
target_link_options(
  gpu_proto
  BEFORE
  INTERFACE
  "LINKER:--copy-dt-needed-entries")

target_include_directories(gpu_proto ${WARNING_GUARD} PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
                                                             $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/include>)
target_compile_features(gpu_proto PUBLIC cxx_std_20)

set_target_properties(
  gpu_proto
  PROPERTIES VERSION ${PROJECT_VERSION}
             CXX_VISIBILITY_PRESET hidden
             VISIBILITY_INLINES_HIDDEN YES)

generate_export_header(gpu_proto EXPORT_FILE_NAME ${PROJECT_BINARY_DIR}/include/gpu_compact/gpu_proto_export.hpp)

if(NOT BUILD_SHARED_LIBS)
  target_compile_definitions(gpu_proto PUBLIC GPU_PROTO_STATIC_DEFINE)
endif()
