#include "atlasdb/btree/internal_node.hpp"

#include <cstddef>
#include <cstdint>

#include <gtest/gtest.h>

#include "atlasdb/storage/page.hpp"

namespace {

atlasdb::btree::InternalEntry Entry(std::int64_t key, std::uint32_t child_page_id) {
  return atlasdb::btree::InternalEntry{key, child_page_id};
}

TEST(BtreeInternalNode, InitializesAndReportsZeroEntries) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(401U);
  ASSERT_TRUE(atlasdb::btree::InitializeInternalNode(&page, 700U).ok);

  std::uint16_t count = 99U;
  ASSERT_TRUE(atlasdb::btree::GetInternalEntryCount(page, &count).ok);
  EXPECT_EQ(count, 0U);

  std::uint32_t left_child = 0U;
  ASSERT_TRUE(atlasdb::btree::GetInternalLeftChild(page, &left_child).ok);
  EXPECT_EQ(left_child, 700U);
}

TEST(BtreeInternalNode, AppendsAndReadsEntriesInOrder) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(402U);
  ASSERT_TRUE(atlasdb::btree::InitializeInternalNode(&page, 800U).ok);

  std::uint16_t first_index = 0U;
  std::uint16_t second_index = 0U;
  ASSERT_TRUE(atlasdb::btree::AppendInternalEntry(&page, Entry(10, 810U), &first_index).ok);
  ASSERT_TRUE(atlasdb::btree::AppendInternalEntry(&page, Entry(20, 820U), &second_index).ok);
  EXPECT_EQ(first_index, 0U);
  EXPECT_EQ(second_index, 1U);

  atlasdb::btree::InternalEntry read_entry;
  ASSERT_TRUE(atlasdb::btree::ReadInternalEntry(page, 1U, &read_entry).ok);
  EXPECT_EQ(read_entry.key, 20);
  EXPECT_EQ(read_entry.child_page_id, 820U);
}

TEST(BtreeInternalNode, InsertsEntryInMiddleAndPreservesOrder) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(411U);
  ASSERT_TRUE(atlasdb::btree::InitializeInternalNode(&page, 100U).ok);

  std::uint16_t index = 0U;
  ASSERT_TRUE(atlasdb::btree::AppendInternalEntry(&page, Entry(10, 200U), &index).ok);
  ASSERT_TRUE(atlasdb::btree::AppendInternalEntry(&page, Entry(30, 400U), &index).ok);

  std::uint16_t insert_index = 99U;
  ASSERT_TRUE(atlasdb::btree::InsertInternalEntry(&page, Entry(20, 300U), &insert_index).ok);
  EXPECT_EQ(insert_index, 1U);

  atlasdb::btree::InternalEntry first;
  atlasdb::btree::InternalEntry second;
  atlasdb::btree::InternalEntry third;
  ASSERT_TRUE(atlasdb::btree::ReadInternalEntry(page, 0U, &first).ok);
  ASSERT_TRUE(atlasdb::btree::ReadInternalEntry(page, 1U, &second).ok);
  ASSERT_TRUE(atlasdb::btree::ReadInternalEntry(page, 2U, &third).ok);

  EXPECT_EQ(first.key, 10);
  EXPECT_EQ(first.child_page_id, 200U);
  EXPECT_EQ(second.key, 20);
  EXPECT_EQ(second.child_page_id, 300U);
  EXPECT_EQ(third.key, 30);
  EXPECT_EQ(third.child_page_id, 400U);
}

TEST(BtreeInternalNode, InsertsEntryAtFrontAndRoutesChildCorrectly) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(412U);
  ASSERT_TRUE(atlasdb::btree::InitializeInternalNode(&page, 900U).ok);

  std::uint16_t index = 0U;
  ASSERT_TRUE(atlasdb::btree::AppendInternalEntry(&page, Entry(10, 910U), &index).ok);
  ASSERT_TRUE(atlasdb::btree::AppendInternalEntry(&page, Entry(20, 920U), &index).ok);

  std::uint16_t insert_index = 99U;
  ASSERT_TRUE(atlasdb::btree::InsertInternalEntry(&page, Entry(5, 905U), &insert_index).ok);
  EXPECT_EQ(insert_index, 0U);

  std::uint32_t child_page = 0U;
  ASSERT_TRUE(atlasdb::btree::FindInternalChildForKey(page, 4, &child_page).ok);
  EXPECT_EQ(child_page, 900U);
  ASSERT_TRUE(atlasdb::btree::FindInternalChildForKey(page, 5, &child_page).ok);
  EXPECT_EQ(child_page, 905U);
  ASSERT_TRUE(atlasdb::btree::FindInternalChildForKey(page, 18, &child_page).ok);
  EXPECT_EQ(child_page, 910U);
}

TEST(BtreeInternalNode, FindsChildForKeyAcrossRanges) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(403U);
  ASSERT_TRUE(atlasdb::btree::InitializeInternalNode(&page, 100U).ok);

  std::uint16_t index = 0U;
  ASSERT_TRUE(atlasdb::btree::AppendInternalEntry(&page, Entry(10, 200U), &index).ok);
  ASSERT_TRUE(atlasdb::btree::AppendInternalEntry(&page, Entry(20, 300U), &index).ok);
  ASSERT_TRUE(atlasdb::btree::AppendInternalEntry(&page, Entry(30, 400U), &index).ok);

  std::uint32_t child_page = 0U;
  ASSERT_TRUE(atlasdb::btree::FindInternalChildForKey(page, 5, &child_page).ok);
  EXPECT_EQ(child_page, 100U);

  ASSERT_TRUE(atlasdb::btree::FindInternalChildForKey(page, 10, &child_page).ok);
  EXPECT_EQ(child_page, 200U);

  ASSERT_TRUE(atlasdb::btree::FindInternalChildForKey(page, 19, &child_page).ok);
  EXPECT_EQ(child_page, 200U);

  ASSERT_TRUE(atlasdb::btree::FindInternalChildForKey(page, 20, &child_page).ok);
  EXPECT_EQ(child_page, 300U);

  ASSERT_TRUE(atlasdb::btree::FindInternalChildForKey(page, 99, &child_page).ok);
  EXPECT_EQ(child_page, 400U);
}

TEST(BtreeInternalNode, RejectsInitializeWithZeroLeftChild) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(404U);

  const atlasdb::btree::InternalNodeStatus status =
      atlasdb::btree::InitializeInternalNode(&page, 0U);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E5202");
}

TEST(BtreeInternalNode, RejectsAppendOnUninitializedPage) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(405U);

  std::uint16_t index = 0U;
  const atlasdb::btree::InternalNodeStatus status =
      atlasdb::btree::AppendInternalEntry(&page, Entry(11, 111U), &index);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E5201");
}

TEST(BtreeInternalNode, RejectsAppendWhenNodeIsFull) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(406U);
  ASSERT_TRUE(atlasdb::btree::InitializeInternalNode(&page, 900U).ok);

  std::uint16_t index = 0U;
  for (std::size_t key = 0U; key < atlasdb::btree::kInternalNodeMaxEntries; ++key) {
    const atlasdb::btree::InternalNodeStatus status =
        atlasdb::btree::AppendInternalEntry(
            &page,
            Entry(static_cast<std::int64_t>(key + 1U), 1000U + static_cast<std::uint32_t>(key)),
            &index);
    ASSERT_TRUE(status.ok);
  }

  const atlasdb::btree::InternalNodeStatus overflow_status =
      atlasdb::btree::AppendInternalEntry(&page, Entry(999999, 1U), &index);

  ASSERT_FALSE(overflow_status.ok);
  EXPECT_EQ(overflow_status.code, "E5203");
}

TEST(BtreeInternalNode, RejectsAppendWhenKeyIsNotStrictlyIncreasing) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(407U);
  ASSERT_TRUE(atlasdb::btree::InitializeInternalNode(&page, 910U).ok);

  std::uint16_t index = 0U;
  ASSERT_TRUE(atlasdb::btree::AppendInternalEntry(&page, Entry(20, 920U), &index).ok);

  const atlasdb::btree::InternalNodeStatus status =
      atlasdb::btree::AppendInternalEntry(&page, Entry(20, 930U), &index);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E5204");
}

TEST(BtreeInternalNode, RejectsInsertWithDuplicateSeparatorKey) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(413U);
  ASSERT_TRUE(atlasdb::btree::InitializeInternalNode(&page, 111U).ok);

  std::uint16_t index = 0U;
  ASSERT_TRUE(atlasdb::btree::AppendInternalEntry(&page, Entry(10, 210U), &index).ok);
  ASSERT_TRUE(atlasdb::btree::AppendInternalEntry(&page, Entry(20, 220U), &index).ok);

  const atlasdb::btree::InternalNodeStatus status =
      atlasdb::btree::InsertInternalEntry(&page, Entry(20, 230U), &index);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E5204");
}

TEST(BtreeInternalNode, RejectsReadIndexOutsideEntryCount) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(408U);
  ASSERT_TRUE(atlasdb::btree::InitializeInternalNode(&page, 920U).ok);

  atlasdb::btree::InternalEntry entry;
  const atlasdb::btree::InternalNodeStatus status =
      atlasdb::btree::ReadInternalEntry(page, 0U, &entry);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E5205");
}

TEST(BtreeInternalNode, DetectsCorruptKeyOrder) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(409U);
  ASSERT_TRUE(atlasdb::btree::InitializeInternalNode(&page, 930U).ok);

  std::uint16_t index = 0U;
  ASSERT_TRUE(atlasdb::btree::AppendInternalEntry(&page, Entry(5, 940U), &index).ok);
  ASSERT_TRUE(atlasdb::btree::AppendInternalEntry(&page, Entry(8, 950U), &index).ok);

  const std::size_t second_entry_offset =
      atlasdb::btree::kInternalNodeHeaderSize + atlasdb::btree::kInternalNodeEntrySize;
  page.bytes[second_entry_offset + 0U] = 0x00U;
  page.bytes[second_entry_offset + 1U] = 0x00U;
  page.bytes[second_entry_offset + 2U] = 0x00U;
  page.bytes[second_entry_offset + 3U] = 0x00U;
  page.bytes[second_entry_offset + 4U] = 0x00U;
  page.bytes[second_entry_offset + 5U] = 0x00U;
  page.bytes[second_entry_offset + 6U] = 0x00U;
  page.bytes[second_entry_offset + 7U] = 0x00U;

  std::uint16_t count = 0U;
  const atlasdb::btree::InternalNodeStatus status =
      atlasdb::btree::GetInternalEntryCount(page, &count);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E5201");
}

TEST(BtreeInternalNode, SetsAndGetsLeftChildPage) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(410U);
  ASSERT_TRUE(atlasdb::btree::InitializeInternalNode(&page, 1001U).ok);
  ASSERT_TRUE(atlasdb::btree::SetInternalLeftChild(&page, 2002U).ok);

  std::uint32_t left_child_page = 0U;
  ASSERT_TRUE(atlasdb::btree::GetInternalLeftChild(page, &left_child_page).ok);
  EXPECT_EQ(left_child_page, 2002U);
}

TEST(BtreeInternalNode, InitializesRootFromSplitMetadata) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(414U);
  ASSERT_TRUE(atlasdb::btree::InitializeInternalRootFromSplit(&page, 3001U, 77, 3002U).ok);

  std::uint32_t left_child = 0U;
  ASSERT_TRUE(atlasdb::btree::GetInternalLeftChild(page, &left_child).ok);
  EXPECT_EQ(left_child, 3001U);

  std::uint16_t count = 0U;
  ASSERT_TRUE(atlasdb::btree::GetInternalEntryCount(page, &count).ok);
  EXPECT_EQ(count, 1U);

  atlasdb::btree::InternalEntry separator;
  ASSERT_TRUE(atlasdb::btree::ReadInternalEntry(page, 0U, &separator).ok);
  EXPECT_EQ(separator.key, 77);
  EXPECT_EQ(separator.child_page_id, 3002U);
}

TEST(BtreeInternalNode, RejectsRootInitializationWithDuplicateChildIds) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(415U);
  const atlasdb::btree::InternalNodeStatus status =
      atlasdb::btree::InitializeInternalRootFromSplit(&page, 444U, 10, 444U);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E5206");
}

}  // namespace
