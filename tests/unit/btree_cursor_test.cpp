#include "atlasdb/btree/cursor.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "atlasdb/btree/leaf_node.hpp"
#include "atlasdb/storage/pager.hpp"
#include "atlasdb/storage/page.hpp"

namespace {

std::filesystem::path UniqueDbPath() {
  static std::uint64_t sequence = 0U;
  ++sequence;

  const auto tick_count = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  return std::filesystem::temp_directory_path() /
         ("atlasdb_btree_cursor_test_" + std::to_string(tick_count) + "_" + std::to_string(sequence) + ".db");
}

void RemoveIfExists(const std::filesystem::path& path) {
  std::error_code remove_error;
  std::filesystem::remove(path, remove_error);
}

std::uint32_t AllocatePage(atlasdb::storage::Pager* pager) {
  std::uint32_t page_id = 0U;
  EXPECT_TRUE(pager->AllocatePage(&page_id).ok);
  return page_id;
}

void WriteLeafPage(atlasdb::storage::Pager* pager,
                   std::uint32_t page_id,
                   const std::vector<atlasdb::btree::LeafEntry>& entries,
                   std::uint32_t next_page_id) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(page_id);
  ASSERT_TRUE(atlasdb::btree::InitializeLeafNode(&page).ok);

  std::uint16_t index = 0U;
  for (const atlasdb::btree::LeafEntry& entry : entries) {
    ASSERT_TRUE(atlasdb::btree::AppendLeafEntry(&page, entry, &index).ok);
  }

  ASSERT_TRUE(atlasdb::btree::SetLeafNextPage(&page, next_page_id).ok);
  ASSERT_TRUE(pager->WritePage(page).ok);
}

TEST(BtreeCursor, SeekFirstSkipsEmptyLeafAndPositionsAtFirstEntry) {
  const std::filesystem::path path = UniqueDbPath();
  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  const std::uint32_t first_leaf = AllocatePage(&pager);
  const std::uint32_t second_leaf = AllocatePage(&pager);
  const std::uint32_t third_leaf = AllocatePage(&pager);

  WriteLeafPage(&pager, first_leaf, {}, second_leaf);
  WriteLeafPage(&pager, second_leaf, {
      atlasdb::btree::LeafEntry{10, 100, 1},
      atlasdb::btree::LeafEntry{20, 200, 2},
  }, third_leaf);
  WriteLeafPage(&pager, third_leaf, {
      atlasdb::btree::LeafEntry{30, 300, 3},
  }, 0U);

  atlasdb::btree::LeafCursor cursor(&pager);
  ASSERT_TRUE(cursor.SeekFirst(first_leaf).ok);
  ASSERT_TRUE(cursor.IsValid());

  atlasdb::btree::LeafEntry entry;
  ASSERT_TRUE(cursor.Current(&entry).ok);
  EXPECT_EQ(entry.key, 10);
  EXPECT_EQ(entry.row_page_id, 100U);
  EXPECT_EQ(entry.row_slot_index, 1U);

  pager.Close();
  RemoveIfExists(path);
}

TEST(BtreeCursor, SeekFindsExactAndLowerBoundAcrossLeaves) {
  const std::filesystem::path path = UniqueDbPath();
  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  const std::uint32_t first_leaf = AllocatePage(&pager);
  const std::uint32_t second_leaf = AllocatePage(&pager);

  WriteLeafPage(&pager, first_leaf, {
      atlasdb::btree::LeafEntry{5, 500, 1},
      atlasdb::btree::LeafEntry{8, 800, 2},
  }, second_leaf);
  WriteLeafPage(&pager, second_leaf, {
      atlasdb::btree::LeafEntry{15, 1500, 3},
      atlasdb::btree::LeafEntry{25, 2500, 4},
  }, 0U);

  atlasdb::btree::LeafCursor cursor(&pager);
  ASSERT_TRUE(cursor.Seek(first_leaf, 15).ok);
  ASSERT_TRUE(cursor.IsValid());

  atlasdb::btree::LeafEntry exact;
  ASSERT_TRUE(cursor.Current(&exact).ok);
  EXPECT_EQ(exact.key, 15);

  ASSERT_TRUE(cursor.Seek(first_leaf, 9).ok);
  ASSERT_TRUE(cursor.IsValid());

  atlasdb::btree::LeafEntry lower_bound;
  ASSERT_TRUE(cursor.Current(&lower_bound).ok);
  EXPECT_EQ(lower_bound.key, 15);

  ASSERT_TRUE(cursor.Seek(first_leaf, 100).ok);
  EXPECT_FALSE(cursor.IsValid());

  pager.Close();
  RemoveIfExists(path);
}

TEST(BtreeCursor, NextAdvancesWithinAndAcrossLinkedLeaves) {
  const std::filesystem::path path = UniqueDbPath();
  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  const std::uint32_t first_leaf = AllocatePage(&pager);
  const std::uint32_t second_leaf = AllocatePage(&pager);

  WriteLeafPage(&pager, first_leaf, {
      atlasdb::btree::LeafEntry{1, 101, 1},
      atlasdb::btree::LeafEntry{2, 102, 2},
  }, second_leaf);
  WriteLeafPage(&pager, second_leaf, {
      atlasdb::btree::LeafEntry{3, 103, 3},
  }, 0U);

  atlasdb::btree::LeafCursor cursor(&pager);
  ASSERT_TRUE(cursor.SeekFirst(first_leaf).ok);

  atlasdb::btree::LeafEntry entry;
  ASSERT_TRUE(cursor.Current(&entry).ok);
  EXPECT_EQ(entry.key, 1);

  ASSERT_TRUE(cursor.Next().ok);
  ASSERT_TRUE(cursor.Current(&entry).ok);
  EXPECT_EQ(entry.key, 2);

  ASSERT_TRUE(cursor.Next().ok);
  ASSERT_TRUE(cursor.Current(&entry).ok);
  EXPECT_EQ(entry.key, 3);

  ASSERT_TRUE(cursor.Next().ok);
  EXPECT_FALSE(cursor.IsValid());

  const atlasdb::btree::BtreeCursorStatus current_status = cursor.Current(&entry);
  ASSERT_FALSE(current_status.ok);
  EXPECT_EQ(current_status.code, "E5304");

  pager.Close();
  RemoveIfExists(path);
}

TEST(BtreeCursor, RejectsOperationsWhenPagerIsNotOpen) {
  atlasdb::storage::Pager pager;
  atlasdb::btree::LeafCursor cursor(&pager);

  const atlasdb::btree::BtreeCursorStatus status = cursor.SeekFirst(1U);
  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E5301");
}

TEST(BtreeCursor, RejectsZeroStartingPageId) {
  const std::filesystem::path path = UniqueDbPath();
  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  atlasdb::btree::LeafCursor cursor(&pager);

  const atlasdb::btree::BtreeCursorStatus first_status = cursor.SeekFirst(0U);
  ASSERT_FALSE(first_status.ok);
  EXPECT_EQ(first_status.code, "E5302");

  const atlasdb::btree::BtreeCursorStatus seek_status = cursor.Seek(0U, 10);
  ASSERT_FALSE(seek_status.ok);
  EXPECT_EQ(seek_status.code, "E5302");

  pager.Close();
  RemoveIfExists(path);
}

TEST(BtreeCursor, PropagatesLeafLayoutValidationErrors) {
  const std::filesystem::path path = UniqueDbPath();
  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  const std::uint32_t corrupt_leaf = AllocatePage(&pager);
  const atlasdb::storage::Page invalid_page = atlasdb::storage::CreateZeroedPage(corrupt_leaf);
  ASSERT_TRUE(pager.WritePage(invalid_page).ok);

  atlasdb::btree::LeafCursor cursor(&pager);
  const atlasdb::btree::BtreeCursorStatus status = cursor.SeekFirst(corrupt_leaf);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E5101");

  pager.Close();
  RemoveIfExists(path);
}

TEST(BtreeCursor, DetectsLeafChainCycleViaTraversalBound) {
  const std::filesystem::path path = UniqueDbPath();
  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  const std::uint32_t first_leaf = AllocatePage(&pager);
  const std::uint32_t second_leaf = AllocatePage(&pager);

  WriteLeafPage(&pager, first_leaf, {}, second_leaf);
  WriteLeafPage(&pager, second_leaf, {}, first_leaf);

  atlasdb::btree::LeafCursor cursor(&pager);
  const atlasdb::btree::BtreeCursorStatus status = cursor.SeekFirst(first_leaf);

  ASSERT_FALSE(status.ok);
  EXPECT_EQ(status.code, "E5305");

  pager.Close();
  RemoveIfExists(path);
}

}  // namespace
