#include "atlasdb/storage/file_header.hpp"

#include <cstdint>

#include <gtest/gtest.h>

namespace {

TEST(StorageFileHeader, RoundTripSerialization) {
  atlasdb::storage::DatabaseFileHeader original;
  original.format_version = atlasdb::storage::kFileFormatVersion;
  original.page_size = static_cast<std::uint32_t>(atlasdb::storage::kPageSize);
  original.page_count = 128U;
  original.catalog_root_page = 3U;
  original.schema_epoch = 42U;
  original.checkpoint_lsn = 9001U;

  const atlasdb::storage::Page page = atlasdb::storage::SerializeFileHeaderPage(original);

  atlasdb::storage::DatabaseFileHeader parsed;
  const atlasdb::storage::HeaderStatus status = atlasdb::storage::DeserializeFileHeaderPage(page, &parsed);

  ASSERT_TRUE(status.ok);
  EXPECT_EQ(parsed.format_version, original.format_version);
  EXPECT_EQ(parsed.page_size, original.page_size);
  EXPECT_EQ(parsed.page_count, original.page_count);
  EXPECT_EQ(parsed.catalog_root_page, original.catalog_root_page);
  EXPECT_EQ(parsed.schema_epoch, original.schema_epoch);
  EXPECT_EQ(parsed.checkpoint_lsn, original.checkpoint_lsn);
}

TEST(StorageFileHeader, RejectsInvalidMagic) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(0U);
  atlasdb::storage::DatabaseFileHeader parsed;

  const atlasdb::storage::HeaderStatus status = atlasdb::storage::DeserializeFileHeaderPage(page, &parsed);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3001");
}

TEST(StorageFileHeader, RejectsUnsupportedVersion) {
  atlasdb::storage::DatabaseFileHeader header;
  header.format_version = 99U;

  const atlasdb::storage::Page page = atlasdb::storage::SerializeFileHeaderPage(header);
  atlasdb::storage::DatabaseFileHeader parsed;

  const atlasdb::storage::HeaderStatus status = atlasdb::storage::DeserializeFileHeaderPage(page, &parsed);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3002");
}

TEST(StorageFileHeader, RejectsUnsupportedPageSize) {
  atlasdb::storage::DatabaseFileHeader header;
  header.page_size = 8192U;

  const atlasdb::storage::Page page = atlasdb::storage::SerializeFileHeaderPage(header);
  atlasdb::storage::DatabaseFileHeader parsed;

  const atlasdb::storage::HeaderStatus status = atlasdb::storage::DeserializeFileHeaderPage(page, &parsed);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3003");
}

TEST(StorageFileHeader, RejectsZeroPageCount) {
  atlasdb::storage::DatabaseFileHeader header;
  header.page_count = 0U;

  const atlasdb::storage::Page page = atlasdb::storage::SerializeFileHeaderPage(header);
  atlasdb::storage::DatabaseFileHeader parsed;

  const atlasdb::storage::HeaderStatus status = atlasdb::storage::DeserializeFileHeaderPage(page, &parsed);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3004");
}

TEST(StorageFileHeader, RejectsCatalogRootOutsidePageCount) {
  atlasdb::storage::DatabaseFileHeader header;
  header.page_count = 4U;
  header.catalog_root_page = 8U;

  const atlasdb::storage::Page page = atlasdb::storage::SerializeFileHeaderPage(header);
  atlasdb::storage::DatabaseFileHeader parsed;

  const atlasdb::storage::HeaderStatus status = atlasdb::storage::DeserializeFileHeaderPage(page, &parsed);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3005");
}

}  // namespace
