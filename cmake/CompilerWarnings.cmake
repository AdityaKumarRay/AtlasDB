function(atlasdb_set_project_warnings target warnings_as_errors)
  if(MSVC)
    target_compile_options(${target} PRIVATE
      /W4
      /permissive-
      /Zc:preprocessor
      /Zc:__cplusplus
      /EHsc
    )
    if(warnings_as_errors)
      target_compile_options(${target} PRIVATE /WX)
    endif()
  else()
    target_compile_options(${target} PRIVATE
      -Wall
      -Wextra
      -Wpedantic
      -Wconversion
      -Wsign-conversion
      -Wshadow
      -Wformat=2
      -Wundef
      -Wold-style-cast
      -Woverloaded-virtual
      -Wnon-virtual-dtor
      -Wdouble-promotion
      -Wnull-dereference
    )
    if(warnings_as_errors)
      target_compile_options(${target} PRIVATE -Werror)
    endif()
  endif()
endfunction()
