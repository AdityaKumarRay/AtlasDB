#include "atlasdb/btree/leaf_node.hpp"

#include <cstddef>
#include <cstdint>

#include <gtest/gtest.h>

#include "atlasdb/storage/page.hpp"

namespace {

atlasdb::btree::LeafEntry Entry(std::int64_t key, std::uint32_t row_page_id, std::uint16_t slot) {
  return atlasdb::btree::LeafEntry{key, row_page_id, slot};
}

TEST(BtreeLeafNode, InitializesAndReportsZeroEntries) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(301U);
  ASSERT_TRUE(atlasdb::btree::InitializeLeafNode(&page).ok);

  std::uint16_t count = 42U;
  ASSERT_TRUE(atlasdb::btree::GetLeafEntryCount(page, &count).ok);
  EXPECT_EQ(count, 0U);

  std::uint32_t next_page = 99U;
  ASSERT_TRUE(atlasdb::btree::GetLeafNextPage(page, &next_page).ok);
  EXPECT_EQ(next_page, 0U);
}

TEST(BtreeLeafNode, AppendsAndReadsEntriesInOrder) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(302U);
  ASSERT_TRUE(atlasdb::btree::InitializeLeafNode(&page).ok);

  std::uint16_t first_index = 0U;
  std::uint16_t second_index = 0U;
  std::uint16_t third_index = 0U;

  ASSERT_TRUE(atlasdb::btree::AppendLeafEntry(&page, Entry(10, 410, 1), &first_index).ok);
  ASSERT_TRUE(atlasdb::btree::AppendLeafEntry(&page, Entry(20, 420, 2), &second_index).ok);
  ASSERT_TRUE(atlasdb::btree::AppendLeafEntry(&page, Entry(30, 430, 3), &third_index).ok);

  EXPECT_EQ(first_index, 0U);
  EXPECT_EQ(second_index, 1U);
  EXPECT_EQ(third_index, 2U);

  atlasdb::btree::LeafEntry read_entry;
  ASSERT_TRUE(atlasdb::btree::ReadLeafEntry(page, 1U, &read_entry).ok);
  EXPECT_EQ(read_entry.key, 20);
  EXPECT_EQ(read_entry.row_page_id, 420U);
  EXPECT_EQ(read_entry.row_slot_index, 2U);
}

TEST(BtreeLeafNode, FindsEntryByKeyAndReturnsIndex) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(303U);
  ASSERT_TRUE(atlasdb::btree::InitializeLeafNode(&page).ok);

  std::uint16_t index = 0U;
  ASSERT_TRUE(atlasdb::btree::AppendLeafEntry(&page, Entry(5, 501, 1), &index).ok);
  ASSERT_TRUE(atlasdb::btree::AppendLeafEntry(&page, Entry(15, 515, 2), &index).ok);
  ASSERT_TRUE(atlasdb::btree::AppendLeafEntry(&page, Entry(25, 525, 3), &index).ok);

  atlasdb::btree::LeafEntry found;
  std::uint16_t found_index = 99U;
  ASSERT_TRUE(atlasdb::btree::FindLeafEntryByKey(page, 15, &found, &found_index).ok);

  EXPECT_EQ(found_index, 1U);
  EXPECT_EQ(found.key, 15);
  EXPECT_EQ(found.row_page_id, 515U);
  EXPECT_EQ(found.row_slot_index, 2U);
}

TEST(BtreeLeafNode, RejectsAppendOnUninitializedPage) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(304U);

  std::uint16_t index = 0U;
  const atlasdb::btree::LeafNodeStatus status =
      atlasdb::btree::AppendLeafEntry(&page, Entry(1, 100, 0), &index);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E5101");
}

TEST(BtreeLeafNode, RejectsAppendWhenLeafIsFull) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(305U);
  ASSERT_TRUE(atlasdb::btree::InitializeLeafNode(&page).ok);

  std::uint16_t index = 0U;
  for (std::size_t key = 0U; key < atlasdb::btree::kLeafNodeMaxEntries; ++key) {
    const atlasdb::btree::LeafNodeStatus status =
        atlasdb::btree::AppendLeafEntry(&page,
                                        Entry(static_cast<std::int64_t>(key + 1U), 1000U + static_cast<std::uint32_t>(key),
                                              static_cast<std::uint16_t>(key % 4096U)),
                                        &index);
    ASSERT_TRUE(status.ok);
  }

  const atlasdb::btree::LeafNodeStatus overflow_status =
      atlasdb::btree::AppendLeafEntry(&page, Entry(999999, 1, 1), &index);

  ASSERT_FALSE(overflow_status.ok);
  EXPECT_EQ(overflow_status.code, "E5103");
}

TEST(BtreeLeafNode, RejectsAppendWhenKeyIsNotStrictlyIncreasing) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(306U);
  ASSERT_TRUE(atlasdb::btree::InitializeLeafNode(&page).ok);

  std::uint16_t index = 0U;
  ASSERT_TRUE(atlasdb::btree::AppendLeafEntry(&page, Entry(20, 200, 1), &index).ok);

  const atlasdb::btree::LeafNodeStatus status =
      atlasdb::btree::AppendLeafEntry(&page, Entry(20, 201, 2), &index);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E5104");
}

TEST(BtreeLeafNode, ReturnsNotFoundForMissingKey) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(307U);
  ASSERT_TRUE(atlasdb::btree::InitializeLeafNode(&page).ok);

  std::uint16_t index = 0U;
  ASSERT_TRUE(atlasdb::btree::AppendLeafEntry(&page, Entry(11, 111, 1), &index).ok);
  ASSERT_TRUE(atlasdb::btree::AppendLeafEntry(&page, Entry(22, 122, 2), &index).ok);

  atlasdb::btree::LeafEntry found;
  const atlasdb::btree::LeafNodeStatus status =
      atlasdb::btree::FindLeafEntryByKey(page, 13, &found, &index);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E5105");
}

TEST(BtreeLeafNode, DetectsCorruptKeyOrder) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(308U);
  ASSERT_TRUE(atlasdb::btree::InitializeLeafNode(&page).ok);

  std::uint16_t index = 0U;
  ASSERT_TRUE(atlasdb::btree::AppendLeafEntry(&page, Entry(5, 105, 1), &index).ok);
  ASSERT_TRUE(atlasdb::btree::AppendLeafEntry(&page, Entry(8, 108, 2), &index).ok);

  const std::size_t second_entry_offset =
      atlasdb::btree::kLeafNodeHeaderSize + atlasdb::btree::kLeafNodeEntrySize;
  page.bytes[second_entry_offset + 0U] = 0x00U;
  page.bytes[second_entry_offset + 1U] = 0x00U;
  page.bytes[second_entry_offset + 2U] = 0x00U;
  page.bytes[second_entry_offset + 3U] = 0x00U;
  page.bytes[second_entry_offset + 4U] = 0x00U;
  page.bytes[second_entry_offset + 5U] = 0x00U;
  page.bytes[second_entry_offset + 6U] = 0x00U;
  page.bytes[second_entry_offset + 7U] = 0x00U;

  std::uint16_t count = 0U;
  const atlasdb::btree::LeafNodeStatus status =
      atlasdb::btree::GetLeafEntryCount(page, &count);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E5101");
}

TEST(BtreeLeafNode, SetsAndGetsNextLeafPage) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(309U);
  ASSERT_TRUE(atlasdb::btree::InitializeLeafNode(&page).ok);
  ASSERT_TRUE(atlasdb::btree::SetLeafNextPage(&page, 902U).ok);

  std::uint32_t next_page = 0U;
  ASSERT_TRUE(atlasdb::btree::GetLeafNextPage(page, &next_page).ok);
  EXPECT_EQ(next_page, 902U);
}

}  // namespace
