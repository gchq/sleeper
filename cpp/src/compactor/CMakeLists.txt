add_executable(compactor main.cpp)
target_link_system_libraries(
  compactor
  PRIVATE
  CLI11::CLI11
  fmt::fmt
  spdlog::spdlog
  lefticus::tools
  gpu_compact::sample_library
  gpu_compact::compact_grpc
  gpu_compact::cudf_compact)
target_link_libraries(compactor PRIVATE gpu_compact::gpu_compact_options gpu_compact::gpu_compact_warnings)
target_include_directories(compactor PRIVATE "${CMAKE_BINARY_DIR}/configured_files/include")
