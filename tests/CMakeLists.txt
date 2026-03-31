include(FetchContent)

set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
)

FetchContent_MakeAvailable(googletest)

add_executable(atlasdb_unit_smoke
  unit/btree_internal_node_test.cpp
  unit/btree_leaf_node_test.cpp
  unit/memory_catalog_test.cpp
  unit/parser_test.cpp
  unit/smoke_test.cpp
  unit/storage_file_header_test.cpp
  unit/storage_pager_test.cpp
  unit/storage_row_codec_test.cpp
  unit/storage_row_page_test.cpp
  unit/storage_table_store_test.cpp
)

target_link_libraries(atlasdb_unit_smoke
  PRIVATE
    AtlasDB::atlasdb
    GTest::gtest_main
)

atlasdb_set_project_warnings(atlasdb_unit_smoke ${ATLASDB_WARNINGS_AS_ERRORS})

if(ATLASDB_ENABLE_SANITIZERS)
  atlasdb_enable_sanitizers(atlasdb_unit_smoke)
endif()

include(GoogleTest)
gtest_discover_tests(atlasdb_unit_smoke)
