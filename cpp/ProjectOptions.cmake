include(cmake/SystemLink.cmake)
include(cmake/LibFuzzer.cmake)
include(CMakeDependentOption)
include(CheckCXXCompilerFlag)

macro(gpu_compact_supports_sanitizers)
  if((CMAKE_CXX_COMPILER_ID MATCHES ".*Clang.*" OR CMAKE_CXX_COMPILER_ID MATCHES ".*GNU.*") AND NOT WIN32)
    set(SUPPORTS_UBSAN ON)
  else()
    set(SUPPORTS_UBSAN OFF)
  endif()

  if((CMAKE_CXX_COMPILER_ID MATCHES ".*Clang.*" OR CMAKE_CXX_COMPILER_ID MATCHES ".*GNU.*") AND WIN32)
    set(SUPPORTS_ASAN OFF)
  else()
    set(SUPPORTS_ASAN ON)
  endif()
endmacro()

macro(gpu_compact_setup_options)
  option(gpu_compact_ENABLE_HARDENING "Enable hardening" ON)
  option(gpu_compact_ENABLE_COVERAGE "Enable coverage reporting" OFF)
  cmake_dependent_option(
    gpu_compact_ENABLE_GLOBAL_HARDENING
    "Attempt to push hardening options to built dependencies"
    ON
    gpu_compact_ENABLE_HARDENING
    OFF)

  gpu_compact_supports_sanitizers()

  if(NOT PROJECT_IS_TOP_LEVEL OR gpu_compact_PACKAGING_MAINTAINER_MODE)
    option(gpu_compact_ENABLE_IPO "Enable IPO/LTO" OFF)
    option(gpu_compact_WARNINGS_AS_ERRORS "Treat Warnings As Errors" OFF)
    option(gpu_compact_ENABLE_USER_LINKER "Enable user-selected linker" OFF)
    option(gpu_compact_ENABLE_SANITIZER_ADDRESS "Enable address sanitizer" OFF)
    option(gpu_compact_ENABLE_SANITIZER_LEAK "Enable leak sanitizer" OFF)
    option(gpu_compact_ENABLE_SANITIZER_UNDEFINED "Enable undefined sanitizer" OFF)
    option(gpu_compact_ENABLE_SANITIZER_THREAD "Enable thread sanitizer" OFF)
    option(gpu_compact_ENABLE_SANITIZER_MEMORY "Enable memory sanitizer" OFF)
    option(gpu_compact_ENABLE_UNITY_BUILD "Enable unity builds" OFF)
    option(gpu_compact_ENABLE_CLANG_TIDY "Enable clang-tidy" OFF)
    option(gpu_compact_ENABLE_CPPCHECK "Enable cpp-check analysis" OFF)
    option(gpu_compact_ENABLE_PCH "Enable precompiled headers" OFF)
    option(gpu_compact_ENABLE_CACHE "Enable ccache" OFF)
    option(gpu_compact_ENABLE_IWYU "Enable include-what-you-use" OFF)
  else()
    option(gpu_compact_ENABLE_IPO "Enable IPO/LTO" OFF)
    option(gpu_compact_WARNINGS_AS_ERRORS "Treat Warnings As Errors" ON)
    option(gpu_compact_ENABLE_USER_LINKER "Enable user-selected linker" OFF)
    # option(gpu_compact_ENABLE_SANITIZER_ADDRESS "Enable address sanitizer" ${SUPPORTS_ASAN})
    option(gpu_compact_ENABLE_SANITIZER_ADDRESS "Enable address sanitizer" OFF)
    option(gpu_compact_ENABLE_SANITIZER_LEAK "Enable leak sanitizer" OFF)
    option(gpu_compact_ENABLE_SANITIZER_UNDEFINED "Enable undefined sanitizer" ${SUPPORTS_UBSAN})
    option(gpu_compact_ENABLE_SANITIZER_THREAD "Enable thread sanitizer" OFF)
    option(gpu_compact_ENABLE_SANITIZER_MEMORY "Enable memory sanitizer" OFF)
    option(gpu_compact_ENABLE_UNITY_BUILD "Enable unity builds" ON)
    option(gpu_compact_ENABLE_CLANG_TIDY "Enable clang-tidy" OFF)
    option(gpu_compact_ENABLE_CPPCHECK "Enable cpp-check analysis" OFF)
    option(gpu_compact_ENABLE_PCH "Enable precompiled headers" OFF)
    option(gpu_compact_ENABLE_CACHE "Enable ccache" ON)
    option(gpu_compact_ENABLE_IWYU "Enable include-what-you-use" ON)
  endif()

  if(NOT PROJECT_IS_TOP_LEVEL)
    mark_as_advanced(
      gpu_compact_ENABLE_IPO
      gpu_compact_WARNINGS_AS_ERRORS
      gpu_compact_ENABLE_USER_LINKER
      gpu_compact_ENABLE_SANITIZER_ADDRESS
      gpu_compact_ENABLE_SANITIZER_LEAK
      gpu_compact_ENABLE_SANITIZER_UNDEFINED
      gpu_compact_ENABLE_SANITIZER_THREAD
      gpu_compact_ENABLE_SANITIZER_MEMORY
      gpu_compact_ENABLE_UNITY_BUILD
      gpu_compact_ENABLE_CLANG_TIDY
      gpu_compact_ENABLE_CPPCHECK
      gpu_compact_ENABLE_COVERAGE
      gpu_compact_ENABLE_PCH
      gpu_compact_ENABLE_CACHE
      gpu_compact_ENABLE_IWYU)
  endif()

  gpu_compact_check_libfuzzer_support(LIBFUZZER_SUPPORTED)
  if(LIBFUZZER_SUPPORTED
     AND (gpu_compact_ENABLE_SANITIZER_ADDRESS
          OR gpu_compact_ENABLE_SANITIZER_THREAD
          OR gpu_compact_ENABLE_SANITIZER_UNDEFINED))
    set(DEFAULT_FUZZER ON)
  else()
    set(DEFAULT_FUZZER OFF)
  endif()

  option(gpu_compact_BUILD_FUZZ_TESTS "Enable fuzz testing executable" ${DEFAULT_FUZZER})

endmacro()

macro(gpu_compact_global_options)
  if(gpu_compact_ENABLE_IPO)
    include(cmake/InterproceduralOptimization.cmake)
    gpu_compact_enable_ipo()
  endif()

  gpu_compact_supports_sanitizers()

  if(gpu_compact_ENABLE_HARDENING AND gpu_compact_ENABLE_GLOBAL_HARDENING)
    include(cmake/Hardening.cmake)
    if(NOT SUPPORTS_UBSAN
       OR gpu_compact_ENABLE_SANITIZER_UNDEFINED
       OR gpu_compact_ENABLE_SANITIZER_ADDRESS
       OR gpu_compact_ENABLE_SANITIZER_THREAD
       OR gpu_compact_ENABLE_SANITIZER_LEAK)
      set(ENABLE_UBSAN_MINIMAL_RUNTIME FALSE)
    else()
      set(ENABLE_UBSAN_MINIMAL_RUNTIME TRUE)
    endif()
    message("${gpu_compact_ENABLE_HARDENING} ${ENABLE_UBSAN_MINIMAL_RUNTIME} ${gpu_compact_ENABLE_SANITIZER_UNDEFINED}")
    gpu_compact_enable_hardening(gpu_compact_options ON ${ENABLE_UBSAN_MINIMAL_RUNTIME})
  endif()
endmacro()

macro(gpu_compact_local_options)
  if(PROJECT_IS_TOP_LEVEL)
    include(cmake/StandardProjectSettings.cmake)
  endif()

  add_library(gpu_compact_warnings INTERFACE)
  add_library(gpu_compact_options INTERFACE)

  include(cmake/CompilerWarnings.cmake)
  gpu_compact_set_project_warnings(
    gpu_compact_warnings
    ${gpu_compact_WARNINGS_AS_ERRORS}
    ""
    ""
    ""
    "")

  if(gpu_compact_ENABLE_USER_LINKER)
    include(cmake/Linker.cmake)
    gpu_compact_configure_linker(gpu_compact_options)
  endif()

  include(cmake/Sanitizers.cmake)
  gpu_compact_enable_sanitizers(
    gpu_compact_options
    ${gpu_compact_ENABLE_SANITIZER_ADDRESS}
    ${gpu_compact_ENABLE_SANITIZER_LEAK}
    ${gpu_compact_ENABLE_SANITIZER_UNDEFINED}
    ${gpu_compact_ENABLE_SANITIZER_THREAD}
    ${gpu_compact_ENABLE_SANITIZER_MEMORY})

  set_target_properties(gpu_compact_options PROPERTIES UNITY_BUILD ${gpu_compact_ENABLE_UNITY_BUILD})

  if(gpu_compact_ENABLE_PCH)
    target_precompile_headers(
      gpu_compact_options
      INTERFACE
      <vector>
      <string>
      <utility>)
  endif()

  if(gpu_compact_ENABLE_CACHE)
    include(cmake/Cache.cmake)
    gpu_compact_enable_cache()
  endif()

  include(cmake/StaticAnalyzers.cmake)
  if(gpu_compact_ENABLE_CLANG_TIDY)
    gpu_compact_enable_clang_tidy(gpu_compact_options ${gpu_compact_WARNINGS_AS_ERRORS})
  endif()

  if(gpu_compact_ENABLE_IWYU)
    gpu_compact_enable_include_what_you_use()
  endif()

  if(gpu_compact_ENABLE_CPPCHECK)
    gpu_compact_enable_cppcheck(${gpu_compact_WARNINGS_AS_ERRORS} "" # override cppcheck options
    )
  endif()

  if(gpu_compact_ENABLE_COVERAGE)
    include(cmake/Tests.cmake)
    gpu_compact_enable_coverage(gpu_compact_options)
  endif()

  if(gpu_compact_WARNINGS_AS_ERRORS)
    check_cxx_compiler_flag("-Wl,--fatal-warnings" LINKER_FATAL_WARNINGS)
    if(LINKER_FATAL_WARNINGS)
      # This is not working consistently, so disabling for now
      # target_link_options(gpu_compact_options INTERFACE -Wl,--fatal-warnings)
    endif()
  endif()

  if(gpu_compact_ENABLE_HARDENING AND NOT gpu_compact_ENABLE_GLOBAL_HARDENING)
    include(cmake/Hardening.cmake)
    if(NOT SUPPORTS_UBSAN
       OR gpu_compact_ENABLE_SANITIZER_UNDEFINED
       OR gpu_compact_ENABLE_SANITIZER_ADDRESS
       OR gpu_compact_ENABLE_SANITIZER_THREAD
       OR gpu_compact_ENABLE_SANITIZER_LEAK)
      set(ENABLE_UBSAN_MINIMAL_RUNTIME FALSE)
    else()
      set(ENABLE_UBSAN_MINIMAL_RUNTIME TRUE)
    endif()
    gpu_compact_enable_hardening(gpu_compact_options OFF ${ENABLE_UBSAN_MINIMAL_RUNTIME})
  endif()

endmacro()
