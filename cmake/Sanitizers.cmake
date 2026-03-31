function(atlasdb_enable_sanitizers target)
  if(MSVC)
    message(STATUS "Sanitizers are not enabled for MSVC by default.")
    return()
  endif()

  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(${target} PRIVATE
      $<$<CONFIG:Debug>:-fsanitize=address,undefined>
      $<$<CONFIG:Debug>:-fno-omit-frame-pointer>
    )
    target_link_options(${target} PRIVATE
      $<$<CONFIG:Debug>:-fsanitize=address,undefined>
    )
  endif()
endfunction()
