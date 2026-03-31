#include "atlasdb/storage/row_codec.hpp"

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

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

TEST(StorageRowCodec, RoundTripPreservesTypedValues) {
  const std::vector<atlasdb::parser::ColumnDefinition> columns = UserColumns();
  const std::vector<atlasdb::parser::ValueLiteral> row = UserRow(7, "alice");

  std::vector<std::uint8_t> bytes;
  const atlasdb::storage::RowCodecStatus encode = atlasdb::storage::SerializeRow(columns, row, &bytes);
  ASSERT_TRUE(encode.ok);

  std::vector<atlasdb::parser::ValueLiteral> decoded;
  const atlasdb::storage::RowCodecStatus decode = atlasdb::storage::DeserializeRow(columns, bytes, &decoded);
  ASSERT_TRUE(decode.ok);

  ASSERT_EQ(decoded.size(), 2U);
  EXPECT_EQ(std::get<std::int64_t>(decoded[0].value), 7);
  EXPECT_EQ(std::get<std::string>(decoded[1].value), "alice");
}

TEST(StorageRowCodec, RejectsSerializeNullOutputBuffer) {
  const atlasdb::storage::RowCodecStatus status =
      atlasdb::storage::SerializeRow(UserColumns(), UserRow(1, "alice"), nullptr);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3200");
  EXPECT_EQ(status.message, "output row buffer pointer is null");
}

TEST(StorageRowCodec, RejectsSerializeCountMismatch) {
  std::vector<atlasdb::parser::ValueLiteral> one_value;
  one_value.emplace_back(1LL);

  std::vector<std::uint8_t> bytes;
  const atlasdb::storage::RowCodecStatus status = atlasdb::storage::SerializeRow(UserColumns(), one_value, &bytes);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3201");
  EXPECT_EQ(status.message, "column/value count mismatch");
}

TEST(StorageRowCodec, RejectsSerializeTypeMismatch) {
  std::vector<atlasdb::parser::ValueLiteral> wrong_types;
  wrong_types.emplace_back(std::string{"not-an-int"});
  wrong_types.emplace_back(std::string{"alice"});

  std::vector<std::uint8_t> bytes;
  const atlasdb::storage::RowCodecStatus status = atlasdb::storage::SerializeRow(UserColumns(), wrong_types, &bytes);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3202");
  EXPECT_EQ(status.message, "type mismatch at column 'id': expected INTEGER");
}

TEST(StorageRowCodec, RejectsDeserializeWhenSchemaColumnCountDiffers) {
  std::vector<std::uint8_t> bytes;
  ASSERT_TRUE(atlasdb::storage::SerializeRow(UserColumns(), UserRow(1, "alice"), &bytes).ok);

  const std::vector<atlasdb::parser::ColumnDefinition> short_schema = {
      {"id", atlasdb::parser::ColumnType::Integer, true},
  };

  std::vector<atlasdb::parser::ValueLiteral> decoded;
  const atlasdb::storage::RowCodecStatus status = atlasdb::storage::DeserializeRow(short_schema, bytes, &decoded);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3205");
  EXPECT_EQ(status.message, "row column count does not match schema");
}

TEST(StorageRowCodec, RejectsDeserializeTruncatedPayload) {
  std::vector<std::uint8_t> bytes;
  ASSERT_TRUE(atlasdb::storage::SerializeRow(UserColumns(), UserRow(1, "alice"), &bytes).ok);
  ASSERT_FALSE(bytes.empty());
  bytes.pop_back();

  std::vector<atlasdb::parser::ValueLiteral> decoded;
  const atlasdb::storage::RowCodecStatus status = atlasdb::storage::DeserializeRow(UserColumns(), bytes, &decoded);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3204");
}

TEST(StorageRowCodec, RejectsDeserializeTrailingBytes) {
  std::vector<std::uint8_t> bytes;
  ASSERT_TRUE(atlasdb::storage::SerializeRow(UserColumns(), UserRow(1, "alice"), &bytes).ok);
  bytes.push_back(0xABU);

  std::vector<atlasdb::parser::ValueLiteral> decoded;
  const atlasdb::storage::RowCodecStatus status = atlasdb::storage::DeserializeRow(UserColumns(), bytes, &decoded);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3206");
  EXPECT_EQ(status.message, "row payload contains trailing bytes");
}

}  // namespace
