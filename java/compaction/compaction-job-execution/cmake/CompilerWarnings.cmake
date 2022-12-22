# from here:
#
# https://github.com/lefticus/cppbestpractices/blob/master/02-Use_the_Tools_Available.md

function(set_project_warnings project_name)
  option(ENABLE_EXTRA_WARNINGS "Enable extra compiler warnings" OFF)
  option(WARNINGS_AS_ERRORS "Treat compiler warnings as errors" TRUE)

  set(MSVC_WARNINGS
      /W4 # Baseline reasonable warnings
      /w14242 # 'identifier': conversion from 'type1' to 'type1', possible loss of data
      /w14254 # 'operator': conversion from 'type1:field_bits' to 'type2:field_bits', possible loss of data
      /w14263 # 'function': member function does not override any base class virtual member function
      /w14265 # 'classname': class has virtual functions, but destructor is not virtual instances of this class may not
              # be destructed correctly
      /w14287 # 'operator': unsigned/negative constant mismatch
      /we4289 # nonstandard extension used: 'variable': loop control variable declared in the for-loop is used outside
              # the for-loop scope
      /w14296 # 'operator': expression is always 'boolean_value'
      /w14311 # 'variable': pointer truncation from 'type1' to 'type2'
      /w14545 # expression before comma evaluates to a function which is missing an argument list
      /w14546 # function call before comma missing argument list
      /w14547 # 'operator': operator before comma has no effect; expected operator with side-effect
      /w14549 # 'operator': operator before comma has no effect; did you intend 'operator'?
      /w14555 # expression has no effect; expected expression with side- effect
      /w14619 # pragma warning: there is no warning number 'number'
      /w14640 # Enable warning on thread un-safe static member initialization
      /w14826 # Conversion from 'type1' to 'type_2' is sign-extended. This may cause unexpected runtime behavior.
      /w14905 # wide string literal cast to 'LPSTR'
      /w14906 # string literal cast to 'LPWSTR'
      /w14928 # illegal copy-initialization; more than one user-defined conversion has been implicitly applied
      /permissive- # standards conformance mode for MSVC compiler.
  )

  set(CLANG_WARNINGS
      -Wall
      -Wextra # reasonable and standard
      -Wshadow # warn the user if a variable declaration shadows one from a parent context
      -Wnon-virtual-dtor # warn the user if a class with virtual functions has a non-virtual destructor. This helps
                         # catch hard to track down memory errors
      -Wold-style-cast # warn for c-style casts
      -Wcast-align # warn for potential performance problem casts
      -Wunused # warn on anything being unused
      -Woverloaded-virtual # warn if you overload (not override) a virtual function
      -Wpedantic # warn if non-standard C++ is used
      -Wconversion # warn on type conversions that may lose data
      -Wsign-conversion # warn on sign conversions
      -Wnull-dereference # warn if a null dereference is detected
      -Wdouble-promotion # warn if float is implicit promoted to double
      -Wformat=2 # warn on security issues around functions that format output (ie printf)
  )

  if(WARNINGS_AS_ERRORS)
    set(CLANG_WARNINGS ${CLANG_WARNINGS} -Werror)
    set(MSVC_WARNINGS ${MSVC_WARNINGS} /WX)
  endif()

  set(GCC_WARNINGS
      ${CLANG_WARNINGS}
      -Wall
      -Wextra
      -pedantic
      -pedantic-errors
      -Warray-bounds=2
      -Wpedantic
      -Wmissing-include-dirs
      -Wtrampolines
      -Wsync-nand
      -Wundef
      -Wshadow
      -Wpointer-arith
      -Wconversion
      -Wcast-align=strict
      -Wcast-qual
      -Wmissing-declarations
      -Wwrite-strings
      -Wredundant-decls
      -Wlogical-op
      -Wdisabled-optimization
      -Wfloat-equal
      -Wformat=2
      -Wmissing-format-attribute
      -Wformat-overflow=2
      -Wformat-truncation=2
      -Winvalid-pch
      -Wpacked
      -Wstack-protector
      -Wswitch-default
      -Wswitch-enum
      -Wstrict-overflow=2
      -Wstrict-aliasing=3
      -Wunused
      -Wuninitialized
      -Wnormalized=nfc
      -Wvarargs
      -Wuseless-cast
      -Wnoexcept
      -Wsign-promo
      -Weffc++
      -Wold-style-cast
      -Woverloaded-virtual
      -Wzero-as-null-pointer-constant
      -Wctor-dtor-privacy
      -Wsign-conversion
      -Wstrict-null-sentinel
      -Wconditionally-supported
      -Wsized-deallocation
      -Wshift-overflow=2
      -Wnull-dereference
      -Wduplicated-cond
      -Wduplicated-branches
      -Wrestrict
      -Wregister
      -Walloc-zero
      -Walloca
      -Wsuggest-attribute=noreturn
      -Wsuggest-override
      -Wsuggest-final-types
      -Wsuggest-final-methods
      -Wdouble-promotion
      -Wformat-signedness
      -Wmissing-noreturn
      -Wattribute-alias=2
      -Wunsafe-loop-optimizations
      -Wunused-macros
      -Wdate-time
      -Wvector-operation-performance
      -Wvla
  )

  if (ENABLE_EXTRA_WARNINGS)
    if(MSVC)
      set(PROJECT_WARNINGS ${MSVC_WARNINGS})
    elseif(CMAKE_CXX_COMPILER_ID MATCHES ".*Clang")
      set(PROJECT_WARNINGS ${CLANG_WARNINGS})
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
      set(PROJECT_WARNINGS ${GCC_WARNINGS})
    else()
      message(AUTHOR_WARNING "No compiler warnings set for '${CMAKE_CXX_COMPILER_ID}' compiler.")
    endif()
  endif()
  target_compile_options(${project_name} INTERFACE ${PROJECT_WARNINGS})

endfunction()