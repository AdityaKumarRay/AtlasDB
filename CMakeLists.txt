cmake_minimum_required(VERSION 3.24)

project(AtlasDB
  VERSION 0.1.0
  DESCRIPTION "AtlasDB embedded database engine"
  LANGUAGES CXX
)

option(ATLASDB_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" ON)
option(ATLASDB_ENABLE_SANITIZERS "Enable sanitizers in Debug builds for GCC/Clang" OFF)
option(ATLASDB_BUILD_TESTS "Build AtlasDB test suite" ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(CompilerWarnings)

if(ATLASDB_ENABLE_SANITIZERS)
  include(Sanitizers)
endif()

add_library(atlasdb STATIC
  src/catalog/memory_catalog.cpp
  src/core/database.cpp
  src/parser/parser.cpp
  src/storage/file_header.cpp
  src/storage/pager.cpp
  src/storage/page.cpp
)
add_library(AtlasDB::atlasdb ALIAS atlasdb)

target_include_directories(atlasdb
  PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_compile_features(atlasdb PUBLIC cxx_std_20)
target_compile_definitions(atlasdb
  PUBLIC
    ATLASDB_VERSION_STRING="${PROJECT_VERSION}"
)

atlasdb_set_project_warnings(atlasdb ${ATLASDB_WARNINGS_AS_ERRORS})

if(ATLASDB_ENABLE_SANITIZERS)
  atlasdb_enable_sanitizers(atlasdb)
endif()

add_executable(atlasdb_cli
  src/cli/main.cpp
)

target_link_libraries(atlasdb_cli PRIVATE AtlasDB::atlasdb)
atlasdb_set_project_warnings(atlasdb_cli ${ATLASDB_WARNINGS_AS_ERRORS})

if(ATLASDB_ENABLE_SANITIZERS)
  atlasdb_enable_sanitizers(atlasdb_cli)
endif()

if(ATLASDB_BUILD_TESTS)
  include(CTest)
  if(BUILD_TESTING)
    add_subdirectory(tests)
  endif()
endif()
