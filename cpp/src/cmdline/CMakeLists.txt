add_executable(main main.cpp configure_logging.cpp ../../include/cmdline/configure_logging.hpp ../format_helper/format_helper.cpp)
target_link_system_libraries(
  main
  PRIVATE
  CLI11::CLI11
  fmt::fmt
  spdlog::spdlog
  lefticus::tools)
target_link_libraries(main PRIVATE gpu_compact::cudf_compact)
target_link_libraries(main PRIVATE gpu_compact::gpu_compact_options gpu_compact::gpu_compact_warnings)
target_include_directories(main PRIVATE "${CMAKE_BINARY_DIR}/configured_files/include")
target_include_directories(main PRIVATE "${CMAKE_SOURCE_DIR}/include/cmdline")

add_executable(chunk_reader chunk_reader.cpp configure_logging.cpp ../../include/cmdline/configure_logging.hpp)
target_link_system_libraries(chunk_reader
  PRIVATE
  CLI11::CLI11
  fmt::fmt
  spdlog::spdlog
  cudf::cudf
)
target_link_libraries(chunk_reader PRIVATE gpu_compact::s3)
target_include_directories(chunk_reader PRIVATE "${CMAKE_BINARY_DIR}/configured_files/include")
target_include_directories(chunk_reader PRIVATE "${CMAKE_SOURCE_DIR}/include/cmdline")
