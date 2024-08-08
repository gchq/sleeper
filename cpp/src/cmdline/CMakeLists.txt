add_executable(
  main
  main.cpp
  locale_set.cpp
  ../../include/cmdline/locale_set.hpp
  configure_logging.cpp
  ../../include/cmdline/configure_logging.hpp)
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
