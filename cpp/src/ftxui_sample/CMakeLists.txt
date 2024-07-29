add_library(gpu_proto ${CMAKE_SOURCE_DIR}/../java/compaction/compaction-gpu/proto/ProtoCompaction.proto)
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
target_include_directories(gpu_proto PUBLIC ${GPU_PROTO_INCLUDE})
target_link_libraries(gpu_proto PRIVATE gpu_compact::gpu_compact_options
)# Don't enable compiler warnings on Protobuf files

add_executable(intro main.cpp)
target_link_system_libraries(
  intro
  PRIVATE
  CLI11::CLI11
  fmt::fmt
  spdlog::spdlog
  lefticus::tools
  gpu_compact::sample_library
  gpu_proto)

target_link_libraries(intro PRIVATE gpu_compact::gpu_compact_options gpu_compact::gpu_compact_warnings)
target_include_directories(intro PRIVATE "${CMAKE_BINARY_DIR}/configured_files/include")
