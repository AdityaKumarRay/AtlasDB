#include "atlasdb/storage/row_page.hpp"

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include "atlasdb/storage/row_codec.hpp"

namespace {

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

TEST(StorageRowPage, InitializesPageAndReportsZeroRows) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(7U);
  ASSERT_TRUE(atlasdb::storage::InitializeRowPage(&page).ok);

  std::uint16_t row_count = 1U;
  const atlasdb::storage::RowPageStatus count_status =
      atlasdb::storage::GetRowCountFromPage(page, &row_count);

  ASSERT_TRUE(count_status.ok);
  EXPECT_EQ(row_count, 0U);
}

TEST(StorageRowPage, AppendsAndReadsRowBytesRoundTrip) {
  const std::vector<atlasdb::parser::ColumnDefinition> columns = UserColumns();
  std::vector<std::uint8_t> encoded_row;
  ASSERT_TRUE(atlasdb::storage::SerializeRow(columns, UserRow(1, "alice"), &encoded_row).ok);

  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(11U);
  ASSERT_TRUE(atlasdb::storage::InitializeRowPage(&page).ok);

  std::uint16_t slot_index = 99U;
  ASSERT_TRUE(atlasdb::storage::AppendRowToPage(&page, encoded_row, &slot_index).ok);
  EXPECT_EQ(slot_index, 0U);

  std::vector<std::uint8_t> row_bytes;
  ASSERT_TRUE(atlasdb::storage::ReadRowFromPage(page, slot_index, &row_bytes).ok);

  std::vector<atlasdb::parser::ValueLiteral> decoded;
  ASSERT_TRUE(atlasdb::storage::DeserializeRow(columns, row_bytes, &decoded).ok);
  ASSERT_EQ(decoded.size(), 2U);
  EXPECT_EQ(std::get<std::int64_t>(decoded[0].value), 1);
  EXPECT_EQ(std::get<std::string>(decoded[1].value), "alice");
}

TEST(StorageRowPage, AppendsMultipleRowsWithStableSlotOrder) {
  const std::vector<atlasdb::parser::ColumnDefinition> columns = UserColumns();
  std::vector<std::uint8_t> first;
  std::vector<std::uint8_t> second;
  ASSERT_TRUE(atlasdb::storage::SerializeRow(columns, UserRow(1, "alice"), &first).ok);
  ASSERT_TRUE(atlasdb::storage::SerializeRow(columns, UserRow(2, "bob"), &second).ok);

  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(12U);
  ASSERT_TRUE(atlasdb::storage::InitializeRowPage(&page).ok);

  std::uint16_t first_slot = 0U;
  std::uint16_t second_slot = 0U;
  ASSERT_TRUE(atlasdb::storage::AppendRowToPage(&page, first, &first_slot).ok);
  ASSERT_TRUE(atlasdb::storage::AppendRowToPage(&page, second, &second_slot).ok);
  EXPECT_EQ(first_slot, 0U);
  EXPECT_EQ(second_slot, 1U);

  std::uint16_t row_count = 0U;
  ASSERT_TRUE(atlasdb::storage::GetRowCountFromPage(page, &row_count).ok);
  EXPECT_EQ(row_count, 2U);

  std::vector<std::uint8_t> first_read;
  std::vector<std::uint8_t> second_read;
  ASSERT_TRUE(atlasdb::storage::ReadRowFromPage(page, first_slot, &first_read).ok);
  ASSERT_TRUE(atlasdb::storage::ReadRowFromPage(page, second_slot, &second_read).ok);

  std::vector<atlasdb::parser::ValueLiteral> first_decoded;
  std::vector<atlasdb::parser::ValueLiteral> second_decoded;
  ASSERT_TRUE(atlasdb::storage::DeserializeRow(columns, first_read, &first_decoded).ok);
  ASSERT_TRUE(atlasdb::storage::DeserializeRow(columns, second_read, &second_decoded).ok);

  EXPECT_EQ(std::get<std::int64_t>(first_decoded[0].value), 1);
  EXPECT_EQ(std::get<std::string>(first_decoded[1].value), "alice");
  EXPECT_EQ(std::get<std::int64_t>(second_decoded[0].value), 2);
  EXPECT_EQ(std::get<std::string>(second_decoded[1].value), "bob");
}

TEST(StorageRowPage, RejectsAppendOnUninitializedPage) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(13U);

  std::uint16_t slot_index = 0U;
  std::vector<std::uint8_t> payload{1U, 2U, 3U};
  const atlasdb::storage::RowPageStatus status =
      atlasdb::storage::AppendRowToPage(&page, payload, &slot_index);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3301");
}

TEST(StorageRowPage, RejectsAppendWhenNoSpaceRemains) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(14U);
  ASSERT_TRUE(atlasdb::storage::InitializeRowPage(&page).ok);

  const std::vector<std::uint8_t> oversized(atlasdb::storage::kPageSize, 0xAAU);
  std::uint16_t slot_index = 0U;
  const atlasdb::storage::RowPageStatus status =
      atlasdb::storage::AppendRowToPage(&page, oversized, &slot_index);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3303");
  EXPECT_EQ(status.message, "insufficient free space on row page");
}

TEST(StorageRowPage, RejectsReadSlotOutsideRowCount) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(15U);
  ASSERT_TRUE(atlasdb::storage::InitializeRowPage(&page).ok);

  std::vector<std::uint8_t> row_bytes;
  const atlasdb::storage::RowPageStatus status =
      atlasdb::storage::ReadRowFromPage(page, 0U, &row_bytes);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3304");
}

TEST(StorageRowPage, RejectsCorruptSlotPayloadRange) {
  const std::vector<atlasdb::parser::ColumnDefinition> columns = UserColumns();
  std::vector<std::uint8_t> encoded_row;
  ASSERT_TRUE(atlasdb::storage::SerializeRow(columns, UserRow(1, "alice"), &encoded_row).ok);

  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(16U);
  ASSERT_TRUE(atlasdb::storage::InitializeRowPage(&page).ok);
  std::uint16_t slot_index = 0U;
  ASSERT_TRUE(atlasdb::storage::AppendRowToPage(&page, encoded_row, &slot_index).ok);

  const std::size_t slot_offset = atlasdb::storage::kRowPageHeaderSize;
  page.bytes[slot_offset + 0U] = 0xFFU;
  page.bytes[slot_offset + 1U] = 0x0FU;
  page.bytes[slot_offset + 2U] = 0x0AU;
  page.bytes[slot_offset + 3U] = 0x00U;

  std::vector<std::uint8_t> row_bytes;
  const atlasdb::storage::RowPageStatus status =
      atlasdb::storage::ReadRowFromPage(page, slot_index, &row_bytes);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3305");
  EXPECT_EQ(status.message, "row slot payload range is invalid");
}

}  // namespace
