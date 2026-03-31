#include "atlasdb/storage/table_store.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include "atlasdb/storage/row_codec.hpp"

namespace {

std::filesystem::path UniqueDbPath() {
  static std::uint64_t sequence = 0U;
  ++sequence;

  const auto tick_count = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  return std::filesystem::temp_directory_path() /
         ("atlasdb_table_store_test_" + std::to_string(tick_count) + "_" + std::to_string(sequence) + ".db");
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

std::vector<std::uint8_t> EncodeUserRow(std::int64_t id, std::string name) {
  std::vector<std::uint8_t> row_bytes;
  const std::vector<atlasdb::parser::ValueLiteral> values = {
      atlasdb::parser::ValueLiteral{id},
      atlasdb::parser::ValueLiteral{std::move(name)},
  };

  const atlasdb::storage::RowCodecStatus status =
      atlasdb::storage::SerializeRow(UserColumns(), values, &row_bytes);
  EXPECT_TRUE(status.ok);
  return row_bytes;
}

std::int64_t DecodeUserId(const std::vector<std::uint8_t>& row_bytes) {
  std::vector<atlasdb::parser::ValueLiteral> decoded;
  const atlasdb::storage::RowCodecStatus status =
      atlasdb::storage::DeserializeRow(UserColumns(), row_bytes, &decoded);
  EXPECT_TRUE(status.ok);
  EXPECT_EQ(decoded.size(), 2U);
  return std::get<std::int64_t>(decoded[0].value);
}

TEST(StorageTableStore, InitializesDirectoryAndFirstDataPage) {
  const std::filesystem::path path = UniqueDbPath();

  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  atlasdb::storage::TableStore store(&pager);
  std::uint32_t root_page_id = 0U;
  const atlasdb::storage::TableStoreStatus init_status = store.Initialize(&root_page_id);

  ASSERT_TRUE(init_status.ok);
  EXPECT_GT(root_page_id, 0U);
  EXPECT_EQ(pager.Header().page_count, 3U);

  std::uint32_t row_count = 999U;
  ASSERT_TRUE(store.RowCount(root_page_id, &row_count).ok);
  EXPECT_EQ(row_count, 0U);

  pager.Close();
  RemoveIfExists(path);
}

TEST(StorageTableStore, AppendsAndReadsEncodedRows) {
  const std::filesystem::path path = UniqueDbPath();

  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  atlasdb::storage::TableStore store(&pager);
  std::uint32_t root_page_id = 0U;
  ASSERT_TRUE(store.Initialize(&root_page_id).ok);

  const std::vector<std::uint8_t> encoded = EncodeUserRow(7, "alice");

  atlasdb::storage::TableRowLocation location;
  ASSERT_TRUE(store.AppendRow(root_page_id, encoded, &location).ok);

  std::vector<std::uint8_t> read_back;
  ASSERT_TRUE(store.ReadRow(root_page_id, location, &read_back).ok);

  std::vector<atlasdb::parser::ValueLiteral> decoded;
  ASSERT_TRUE(atlasdb::storage::DeserializeRow(UserColumns(), read_back, &decoded).ok);
  ASSERT_EQ(decoded.size(), 2U);
  EXPECT_EQ(std::get<std::int64_t>(decoded[0].value), 7);
  EXPECT_EQ(std::get<std::string>(decoded[1].value), "alice");

  std::uint32_t row_count = 0U;
  ASSERT_TRUE(store.RowCount(root_page_id, &row_count).ok);
  EXPECT_EQ(row_count, 1U);

  pager.Close();
  RemoveIfExists(path);
}

TEST(StorageTableStore, AllocatesNewDataPageWhenTailPageIsFull) {
  const std::filesystem::path path = UniqueDbPath();

  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  atlasdb::storage::TableStore store(&pager);
  std::uint32_t root_page_id = 0U;
  ASSERT_TRUE(store.Initialize(&root_page_id).ok);

  std::vector<atlasdb::storage::TableRowLocation> locations;
  locations.reserve(4U);

  const std::string large_text(1700U, 'x');
  for (std::int64_t id = 1; id <= 4; ++id) {
    const std::vector<std::uint8_t> encoded = EncodeUserRow(id, large_text);
    atlasdb::storage::TableRowLocation location;
    ASSERT_TRUE(store.AppendRow(root_page_id, encoded, &location).ok);
    locations.push_back(location);
  }

  EXPECT_NE(locations.front().page_id, locations.back().page_id);

  std::uint32_t row_count = 0U;
  ASSERT_TRUE(store.RowCount(root_page_id, &row_count).ok);
  EXPECT_EQ(row_count, 4U);

  pager.Close();
  RemoveIfExists(path);
}

TEST(StorageTableStore, RejectsReadForPageOutsideTableDirectory) {
  const std::filesystem::path path = UniqueDbPath();

  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  atlasdb::storage::TableStore store(&pager);
  std::uint32_t root_page_id = 0U;
  ASSERT_TRUE(store.Initialize(&root_page_id).ok);

  std::vector<std::uint8_t> row_bytes;
  const atlasdb::storage::TableStoreStatus status =
      store.ReadRow(root_page_id, atlasdb::storage::TableRowLocation{9999U, 0U}, &row_bytes);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3405");

  pager.Close();
  RemoveIfExists(path);
}

TEST(StorageTableStore, PersistsRowsAcrossPagerReopen) {
  const std::filesystem::path path = UniqueDbPath();

  std::uint32_t root_page_id = 0U;
  atlasdb::storage::TableRowLocation location;

  {
    atlasdb::storage::Pager pager;
    ASSERT_TRUE(pager.Open(path.string()).ok);

    atlasdb::storage::TableStore store(&pager);
    ASSERT_TRUE(store.Initialize(&root_page_id).ok);

    const std::vector<std::uint8_t> encoded = EncodeUserRow(42, "persisted");
    ASSERT_TRUE(store.AppendRow(root_page_id, encoded, &location).ok);

    pager.Close();
  }

  {
    atlasdb::storage::Pager pager;
    ASSERT_TRUE(pager.Open(path.string()).ok);

    atlasdb::storage::TableStore store(&pager);
    std::vector<std::uint8_t> read_back;
    ASSERT_TRUE(store.ReadRow(root_page_id, location, &read_back).ok);

    std::vector<atlasdb::parser::ValueLiteral> decoded;
    ASSERT_TRUE(atlasdb::storage::DeserializeRow(UserColumns(), read_back, &decoded).ok);
    ASSERT_EQ(decoded.size(), 2U);
    EXPECT_EQ(std::get<std::int64_t>(decoded[0].value), 42);
    EXPECT_EQ(std::get<std::string>(decoded[1].value), "persisted");

    std::uint32_t row_count = 0U;
    ASSERT_TRUE(store.RowCount(root_page_id, &row_count).ok);
    EXPECT_EQ(row_count, 1U);

    pager.Close();
  }

  RemoveIfExists(path);
}

TEST(StorageTableStore, ScansRowsInAppendOrderAcrossPages) {
  const std::filesystem::path path = UniqueDbPath();

  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  atlasdb::storage::TableStore store(&pager);
  std::uint32_t root_page_id = 0U;
  ASSERT_TRUE(store.Initialize(&root_page_id).ok);

  const std::string large_text(1700U, 'x');
  for (std::int64_t id = 1; id <= 4; ++id) {
    atlasdb::storage::TableRowLocation location;
    ASSERT_TRUE(store.AppendRow(root_page_id, EncodeUserRow(id, large_text), &location).ok);
  }

  std::vector<atlasdb::storage::StoredTableRow> rows;
  ASSERT_TRUE(store.ScanRows(root_page_id, &rows).ok);
  ASSERT_EQ(rows.size(), 4U);

  EXPECT_EQ(DecodeUserId(rows[0].row_bytes), 1);
  EXPECT_EQ(DecodeUserId(rows[1].row_bytes), 2);
  EXPECT_EQ(DecodeUserId(rows[2].row_bytes), 3);
  EXPECT_EQ(DecodeUserId(rows[3].row_bytes), 4);

  pager.Close();
  RemoveIfExists(path);
}

TEST(StorageTableStore, ScanRowsRejectsNullOutputBuffer) {
  const std::filesystem::path path = UniqueDbPath();

  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  atlasdb::storage::TableStore store(&pager);
  std::uint32_t root_page_id = 0U;
  ASSERT_TRUE(store.Initialize(&root_page_id).ok);

  const atlasdb::storage::TableStoreStatus status = store.ScanRows(root_page_id, nullptr);
  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3400");
  EXPECT_EQ(status.message, "output rows pointer is null");

  pager.Close();
  RemoveIfExists(path);
}

TEST(StorageTableStore, ScanRowsDetectsDirectoryRowCountMismatch) {
  const std::filesystem::path path = UniqueDbPath();

  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  atlasdb::storage::TableStore store(&pager);
  std::uint32_t root_page_id = 0U;
  ASSERT_TRUE(store.Initialize(&root_page_id).ok);

  atlasdb::storage::TableRowLocation location;
  ASSERT_TRUE(store.AppendRow(root_page_id, EncodeUserRow(1, "alice"), &location).ok);

  atlasdb::storage::Page root_page;
  ASSERT_TRUE(pager.ReadPage(root_page_id, &root_page).ok);

  root_page.bytes[12U] = 7U;
  root_page.bytes[13U] = 0U;
  root_page.bytes[14U] = 0U;
  root_page.bytes[15U] = 0U;
  ASSERT_TRUE(pager.WritePage(root_page).ok);

  std::vector<atlasdb::storage::StoredTableRow> rows;
  const atlasdb::storage::TableStoreStatus status = store.ScanRows(root_page_id, &rows);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3404");
  EXPECT_EQ(status.message, "table directory row count does not match scanned rows");

  pager.Close();
  RemoveIfExists(path);
}

}  // namespace
