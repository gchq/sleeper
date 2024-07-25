protobuf_generate_cpp(GPU_PROTO_SRC GPU_PROTO_HDRS
                      ${CMAKE_SOURCE_DIR}/../../../java/compaction/compaction-gpu/proto/ProtoCompaction.proto)

message(STATUS "${GPU_PROTO_SRC} found")

add_executable(intro main.cpp ${GPU_PROTO_SRC} ${GPU_PROTO_HDRS})

target_link_libraries(intro PRIVATE gpu_compact::gpu_compact_options gpu_compact::gpu_compact_warnings)

target_link_system_libraries(
  intro
  PRIVATE
  CLI11::CLI11
  fmt::fmt
  spdlog::spdlog
  lefticus::tools
  gpu_compact::sample_library)
target_link_libraries(intro PRIVATE grpc::grpc)

target_include_directories(intro PRIVATE "${CMAKE_BINARY_DIR}/configured_files/include")
target_link_libraries(intro ${Protobuf_LIBRARIES})
