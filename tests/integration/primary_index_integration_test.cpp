#include "atlasdb/btree/cursor.hpp"
#include "atlasdb/btree/index.hpp"
#include "atlasdb/parser/ast.hpp"
#include "atlasdb/storage/page.hpp"
#include "atlasdb/storage/pager.hpp"
#include "atlasdb/storage/row_codec.hpp"
#include "atlasdb/storage/table_store.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <random>
#include <string>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

namespace {

std::filesystem::path UniqueDbPath() {
  static std::uint64_t sequence = 0U;
  ++sequence;

  const auto tick_count = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  return std::filesystem::temp_directory_path() /
         ("atlasdb_primary_index_integration_" + std::to_string(tick_count) + "_" +
          std::to_string(sequence) + ".db");
}

void RemoveIfExists(const std::filesystem::path& path) {
  std::error_code remove_error;
  std::filesystem::remove(path, remove_error);
}

std::vector<atlasdb::parser::ColumnDefinition> UserColumns() {
  return {
      {"id", atlasdb::parser::ColumnType::Integer, true},
      {"name", atlasdb::parser::ColumnType::Text, false},
  };
}

std::vector<atlasdb::parser::ValueLiteral> UserRow(std::int64_t id, std::string name) {
  return {
      atlasdb::parser::ValueLiteral{id},
      atlasdb::parser::ValueLiteral{std::move(name)},
  };
}

TEST(PrimaryIndexIntegration, KeyLookupReturnsRowFromTableStore) {
  const std::filesystem::path path = UniqueDbPath();
  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  atlasdb::storage::TableStore table_store(&pager);
  std::uint32_t table_root_page_id = 0U;
  ASSERT_TRUE(table_store.Initialize(&table_root_page_id).ok);

  atlasdb::btree::BtreeIndex index(&pager);
  std::uint32_t index_root_page_id = 0U;
  ASSERT_TRUE(index.Initialize(&index_root_page_id).ok);

  const std::vector<atlasdb::parser::ColumnDefinition> columns = UserColumns();
  const std::vector<std::pair<std::int64_t, std::string>> rows = {
      {20, "twenty"},
      {5, "five"},
      {15, "fifteen"},
      {30, "thirty"},
  };

  for (const auto& row : rows) {
    std::vector<std::uint8_t> encoded_row;
    ASSERT_TRUE(atlasdb::storage::SerializeRow(columns, UserRow(row.first, row.second), &encoded_row).ok);

    atlasdb::storage::TableRowLocation location;
    ASSERT_TRUE(table_store.AppendRow(table_root_page_id, encoded_row, &location).ok);

    const atlasdb::btree::LeafEntry index_entry{row.first, location.page_id, location.slot_index};
    ASSERT_TRUE(index.Insert(index_entry).ok);
  }

  atlasdb::btree::LeafEntry found;
  ASSERT_TRUE(index.Find(15, &found).ok);

  std::vector<std::uint8_t> found_row_bytes;
  ASSERT_TRUE(
      table_store.ReadRow(table_root_page_id,
                          atlasdb::storage::TableRowLocation{found.row_page_id, found.row_slot_index},
                          &found_row_bytes)
          .ok);

  std::vector<atlasdb::parser::ValueLiteral> found_row;
  ASSERT_TRUE(atlasdb::storage::DeserializeRow(columns, found_row_bytes, &found_row).ok);
  ASSERT_EQ(found_row.size(), 2U);
  EXPECT_EQ(std::get<std::int64_t>(found_row[0].value), 15);
  EXPECT_EQ(std::get<std::string>(found_row[1].value), "fifteen");

  const atlasdb::btree::BtreeIndexStatus missing_status = index.Find(999, &found);
  ASSERT_FALSE(missing_status.ok);
  EXPECT_EQ(missing_status.code, "E5105");

  pager.Close();
  RemoveIfExists(path);
}

TEST(PrimaryIndexIntegration, OrderedTraversalYieldsPrimaryKeyOrderAcrossRows) {
  const std::filesystem::path path = UniqueDbPath();
  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  atlasdb::storage::TableStore table_store(&pager);
  std::uint32_t table_root_page_id = 0U;
  ASSERT_TRUE(table_store.Initialize(&table_root_page_id).ok);

  atlasdb::btree::BtreeIndex index(&pager);
  std::uint32_t index_root_page_id = 0U;
  ASSERT_TRUE(index.Initialize(&index_root_page_id).ok);

  const std::vector<atlasdb::parser::ColumnDefinition> columns = UserColumns();
  constexpr std::int64_t kRowCount = 750;

  std::vector<std::int64_t> shuffled_keys;
  shuffled_keys.reserve(static_cast<std::size_t>(kRowCount));
  for (std::int64_t key = 1; key <= kRowCount; ++key) {
    shuffled_keys.push_back(key);
  }

  std::mt19937_64 random_engine(0xFACE1234BEEFULL);
  std::shuffle(shuffled_keys.begin(), shuffled_keys.end(), random_engine);

  for (const std::int64_t key : shuffled_keys) {
    std::vector<std::uint8_t> encoded_row;
    ASSERT_TRUE(atlasdb::storage::SerializeRow(
                    columns,
                    UserRow(key, "user_" + std::to_string(static_cast<long long>(key))),
                    &encoded_row)
                    .ok);

    atlasdb::storage::TableRowLocation location;
    ASSERT_TRUE(table_store.AppendRow(table_root_page_id, encoded_row, &location).ok);

    const atlasdb::btree::LeafEntry index_entry{key, location.page_id, location.slot_index};
    ASSERT_TRUE(index.Insert(index_entry).ok);
  }

  std::uint32_t first_leaf_page_id = 0U;
  ASSERT_TRUE(index.ResolveFirstLeaf(&first_leaf_page_id).ok);

  atlasdb::btree::LeafCursor cursor(&pager);
  ASSERT_TRUE(cursor.SeekFirst(first_leaf_page_id).ok);

  std::int64_t expected_key = 1;
  while (cursor.IsValid()) {
    atlasdb::btree::LeafEntry index_entry;
    ASSERT_TRUE(cursor.Current(&index_entry).ok);

    std::vector<std::uint8_t> row_bytes;
    ASSERT_TRUE(
        table_store.ReadRow(table_root_page_id,
                            atlasdb::storage::TableRowLocation{index_entry.row_page_id,
                                                               index_entry.row_slot_index},
                            &row_bytes)
            .ok);

    std::vector<atlasdb::parser::ValueLiteral> decoded_row;
    ASSERT_TRUE(atlasdb::storage::DeserializeRow(columns, row_bytes, &decoded_row).ok);
    ASSERT_EQ(decoded_row.size(), 2U);

    EXPECT_EQ(std::get<std::int64_t>(decoded_row[0].value), expected_key);
    EXPECT_EQ(std::get<std::string>(decoded_row[1].value),
              "user_" + std::to_string(static_cast<long long>(expected_key)));

    ASSERT_TRUE(cursor.Next().ok);
    ++expected_key;
  }

  EXPECT_EQ(expected_key, kRowCount + 1);

  pager.Close();
  RemoveIfExists(path);
}

}  // namespace
