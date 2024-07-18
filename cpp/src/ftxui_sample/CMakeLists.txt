add_executable(intro main.cpp)

target_link_libraries(
  intro
  PRIVATE gpu_compact::gpu_compact_options
          gpu_compact::gpu_compact_warnings)

target_link_system_libraries(
  intro
  PRIVATE
          CLI11::CLI11
          fmt::fmt
          spdlog::spdlog
          lefticus::tools
          gpu_compact::sample_library
        )

target_include_directories(intro PRIVATE "${CMAKE_BINARY_DIR}/configured_files/include")
