#include "atlasdb/storage/pager.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

namespace {

std::filesystem::path UniqueDbPath() {
  static std::uint64_t sequence = 0U;
  ++sequence;

  const auto tick_count = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  return std::filesystem::temp_directory_path() /
         ("atlasdb_pager_test_" + std::to_string(tick_count) + "_" + std::to_string(sequence) + ".db");
}

void RemoveIfExists(const std::filesystem::path& path) {
  std::error_code remove_error;
  std::filesystem::remove(path, remove_error);
}

TEST(StoragePager, CreatesNewDatabaseFileAndHeader) {
  const std::filesystem::path path = UniqueDbPath();

  atlasdb::storage::Pager pager;
  const atlasdb::storage::PagerStatus status = pager.Open(path.string());

  ASSERT_TRUE(status.ok);
  EXPECT_TRUE(std::filesystem::exists(path));
  EXPECT_EQ(pager.Header().page_count, 1U);
  EXPECT_EQ(pager.Header().format_version, atlasdb::storage::kFileFormatVersion);
  EXPECT_EQ(pager.Header().page_size, static_cast<std::uint32_t>(atlasdb::storage::kPageSize));

  pager.Close();
  RemoveIfExists(path);
}

TEST(StoragePager, AllocatesPagesAndPersistsPageCountAcrossReopen) {
  const std::filesystem::path path = UniqueDbPath();

  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  std::uint32_t first_page = 0U;
  std::uint32_t second_page = 0U;
  ASSERT_TRUE(pager.AllocatePage(&first_page).ok);
  ASSERT_TRUE(pager.AllocatePage(&second_page).ok);
  EXPECT_EQ(first_page, 1U);
  EXPECT_EQ(second_page, 2U);
  EXPECT_EQ(pager.Header().page_count, 3U);
  pager.Close();

  atlasdb::storage::Pager reopened;
  const atlasdb::storage::PagerStatus reopen_status = reopened.Open(path.string());
  ASSERT_TRUE(reopen_status.ok);
  EXPECT_EQ(reopened.Header().page_count, 3U);

  reopened.Close();
  RemoveIfExists(path);
}

TEST(StoragePager, WritesAndReadsAllocatedPage) {
  const std::filesystem::path path = UniqueDbPath();

  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  std::uint32_t page_id = 0U;
  ASSERT_TRUE(pager.AllocatePage(&page_id).ok);

  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(page_id);
  page.bytes[0] = 0xABU;
  page.bytes[1] = 0xCDU;
  page.bytes[2] = 0xEFU;

  ASSERT_TRUE(pager.WritePage(page).ok);

  atlasdb::storage::Page read_page;
  const atlasdb::storage::PagerStatus read_status = pager.ReadPage(page_id, &read_page);
  ASSERT_TRUE(read_status.ok);
  EXPECT_EQ(read_page.id, page_id);
  EXPECT_EQ(read_page.bytes[0], 0xABU);
  EXPECT_EQ(read_page.bytes[1], 0xCDU);
  EXPECT_EQ(read_page.bytes[2], 0xEFU);

  pager.Close();
  RemoveIfExists(path);
}

TEST(StoragePager, RejectsReadOutsideDeclaredPageCount) {
  const std::filesystem::path path = UniqueDbPath();

  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  atlasdb::storage::Page page;
  const atlasdb::storage::PagerStatus status = pager.ReadPage(1U, &page);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3104");

  pager.Close();
  RemoveIfExists(path);
}

TEST(StoragePager, RejectsInvalidHeaderOnOpen) {
  const std::filesystem::path path = UniqueDbPath();

  {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.is_open());

    std::array<std::uint8_t, atlasdb::storage::kPageSize> bytes{};
    output.write(reinterpret_cast<const char*>(bytes.data()),
                 static_cast<std::streamsize>(bytes.size()));
    ASSERT_TRUE(output.good());
  }

  atlasdb::storage::Pager pager;
  const atlasdb::storage::PagerStatus status = pager.Open(path.string());

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E3001");

  pager.Close();
  RemoveIfExists(path);
}

}  // namespace
