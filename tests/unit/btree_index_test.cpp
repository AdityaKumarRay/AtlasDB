#include "atlasdb/btree/index.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "atlasdb/btree/cursor.hpp"
#include "atlasdb/btree/internal_node.hpp"
#include "atlasdb/btree/leaf_node.hpp"
#include "atlasdb/storage/page.hpp"
#include "atlasdb/storage/pager.hpp"

namespace {

std::filesystem::path UniqueDbPath() {
  static std::uint64_t sequence = 0U;
  ++sequence;

  const auto tick_count = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  return std::filesystem::temp_directory_path() /
         ("atlasdb_btree_index_test_" + std::to_string(tick_count) + "_" +
          std::to_string(sequence) + ".db");
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

void WriteLeaf(atlasdb::storage::Pager* pager,
               std::uint32_t page_id,
               std::int64_t first_key,
               std::size_t entry_count,
               std::uint32_t next_page_id) {
  atlasdb::storage::Page page = atlasdb::storage::CreateZeroedPage(page_id);
  ASSERT_TRUE(atlasdb::btree::InitializeLeafNode(&page).ok);

  std::uint16_t index = 0U;
  for (std::size_t offset = 0U; offset < entry_count; ++offset) {
    const std::int64_t key = first_key + static_cast<std::int64_t>(offset);
    ASSERT_TRUE(atlasdb::btree::AppendLeafEntry(
                    &page,
                    atlasdb::btree::LeafEntry{key,
                                              static_cast<std::uint32_t>(5000U + static_cast<std::uint32_t>(offset)),
                                              static_cast<std::uint16_t>(offset % 4096U)},
                    &index)
                    .ok);
  }

  ASSERT_TRUE(atlasdb::btree::SetLeafNextPage(&page, next_page_id).ok);
  ASSERT_TRUE(pager->WritePage(page).ok);
}

TEST(BtreeIndex, InitializesAndSupportsOutOfOrderInsertFind) {
  const std::filesystem::path path = UniqueDbPath();
  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  atlasdb::btree::BtreeIndex index(&pager);
  std::uint32_t root_page_id = 0U;
  ASSERT_TRUE(index.Initialize(&root_page_id).ok);
  EXPECT_GT(root_page_id, 0U);

  ASSERT_TRUE(index.Insert(atlasdb::btree::LeafEntry{20, 120, 2}).ok);
  ASSERT_TRUE(index.Insert(atlasdb::btree::LeafEntry{10, 110, 1}).ok);
  ASSERT_TRUE(index.Insert(atlasdb::btree::LeafEntry{30, 130, 3}).ok);

  atlasdb::btree::LeafEntry found;
  ASSERT_TRUE(index.Find(10, &found).ok);
  EXPECT_EQ(found.row_page_id, 110U);
  EXPECT_EQ(found.row_slot_index, 1U);

  ASSERT_TRUE(index.Find(20, &found).ok);
  EXPECT_EQ(found.row_page_id, 120U);
  EXPECT_EQ(found.row_slot_index, 2U);

  const atlasdb::btree::BtreeIndexStatus missing_status = index.Find(99, &found);
  ASSERT_FALSE(missing_status.ok);
  EXPECT_EQ(missing_status.code, "E5105");

  pager.Close();
  RemoveIfExists(path);
}

TEST(BtreeIndex, RejectsDuplicateKeyInsert) {
  const std::filesystem::path path = UniqueDbPath();
  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  atlasdb::btree::BtreeIndex index(&pager);
  std::uint32_t root_page_id = 0U;
  ASSERT_TRUE(index.Initialize(&root_page_id).ok);

  ASSERT_TRUE(index.Insert(atlasdb::btree::LeafEntry{42, 2042U, 4U}).ok);

  const atlasdb::btree::BtreeIndexStatus duplicate_status =
      index.Insert(atlasdb::btree::LeafEntry{42, 3042U, 5U});
  ASSERT_FALSE(duplicate_status.ok);
  EXPECT_EQ(duplicate_status.code, "E5104");

  pager.Close();
  RemoveIfExists(path);
}

TEST(BtreeIndex, SplitsLeafRootAndScansInKeyOrder) {
  const std::filesystem::path path = UniqueDbPath();
  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  atlasdb::btree::BtreeIndex index(&pager);
  std::uint32_t initial_root_page_id = 0U;
  ASSERT_TRUE(index.Initialize(&initial_root_page_id).ok);

  const std::int64_t key_count =
      static_cast<std::int64_t>(atlasdb::btree::kLeafNodeMaxEntries + 25U);

  for (std::int64_t key = 1; key <= key_count; ++key) {
    ASSERT_TRUE(index.Insert(atlasdb::btree::LeafEntry{
                    key,
                    static_cast<std::uint32_t>(1000U + static_cast<std::uint32_t>(key)),
                    static_cast<std::uint16_t>(key % 4096)})
                    .ok);
  }

  std::uint32_t current_root_page_id = 0U;
  ASSERT_TRUE(index.GetRootPageId(&current_root_page_id).ok);
  EXPECT_NE(current_root_page_id, initial_root_page_id);

  atlasdb::storage::Page root_page;
  ASSERT_TRUE(pager.ReadPage(current_root_page_id, &root_page).ok);
  std::uint16_t separator_count = 0U;
  ASSERT_TRUE(atlasdb::btree::GetInternalEntryCount(root_page, &separator_count).ok);
  EXPECT_GT(separator_count, 0U);

  std::uint32_t first_leaf_page_id = 0U;
  ASSERT_TRUE(index.ResolveFirstLeaf(&first_leaf_page_id).ok);

  atlasdb::btree::LeafCursor cursor(&pager);
  ASSERT_TRUE(cursor.SeekFirst(first_leaf_page_id).ok);

  std::int64_t expected_key = 1;
  while (cursor.IsValid()) {
    atlasdb::btree::LeafEntry entry;
    ASSERT_TRUE(cursor.Current(&entry).ok);
    EXPECT_EQ(entry.key, expected_key);
    EXPECT_EQ(entry.row_page_id,
              static_cast<std::uint32_t>(1000U + static_cast<std::uint32_t>(expected_key)));

    ASSERT_TRUE(cursor.Next().ok);
    ++expected_key;
  }

  EXPECT_EQ(expected_key, key_count + 1);

  pager.Close();
  RemoveIfExists(path);
}

TEST(BtreeIndex, PropagatesSplitWhenParentInternalNodeIsFull) {
  const std::filesystem::path path = UniqueDbPath();
  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  const std::uint32_t root_page_id = AllocatePage(&pager);

  std::vector<std::uint32_t> child_page_ids;
  child_page_ids.reserve(atlasdb::btree::kInternalNodeMaxEntries + 1U);

  for (std::size_t index = 0U; index < atlasdb::btree::kInternalNodeMaxEntries + 1U; ++index) {
    const std::uint32_t child_page_id = AllocatePage(&pager);
    child_page_ids.push_back(child_page_id);

    WriteLeaf(&pager,
              child_page_id,
              1000000 + static_cast<std::int64_t>(index * 1000U),
              1U,
              0U);
  }

  WriteLeaf(&pager,
            child_page_ids.front(),
            1,
            atlasdb::btree::kLeafNodeMaxEntries,
            0U);

  atlasdb::storage::Page root_page = atlasdb::storage::CreateZeroedPage(root_page_id);
  ASSERT_TRUE(atlasdb::btree::InitializeInternalNode(&root_page, child_page_ids.front()).ok);

  std::uint16_t separator_index = 0U;
  for (std::size_t index = 0U; index < atlasdb::btree::kInternalNodeMaxEntries; ++index) {
    ASSERT_TRUE(atlasdb::btree::AppendInternalEntry(
                    &root_page,
                    atlasdb::btree::InternalEntry{
                        1000000 + static_cast<std::int64_t>(index * 1000U),
                        child_page_ids[index + 1U]},
                    &separator_index)
                    .ok);
  }
  ASSERT_TRUE(pager.WritePage(root_page).ok);

  atlasdb::btree::BtreeIndex index(&pager);
  ASSERT_TRUE(index.Open(root_page_id).ok);

  const atlasdb::btree::LeafEntry inserted{500, 7777U, 42U};
  ASSERT_TRUE(index.Insert(inserted).ok);

  std::uint32_t new_root_page_id = 0U;
  ASSERT_TRUE(index.GetRootPageId(&new_root_page_id).ok);
  EXPECT_NE(new_root_page_id, root_page_id);

  atlasdb::btree::LeafEntry found;
  ASSERT_TRUE(index.Find(inserted.key, &found).ok);
  EXPECT_EQ(found.row_page_id, inserted.row_page_id);
  EXPECT_EQ(found.row_slot_index, inserted.row_slot_index);

  atlasdb::storage::Page new_root_page;
  ASSERT_TRUE(pager.ReadPage(new_root_page_id, &new_root_page).ok);
  std::uint16_t new_root_separator_count = 0U;
  ASSERT_TRUE(atlasdb::btree::GetInternalEntryCount(new_root_page, &new_root_separator_count).ok);
  EXPECT_EQ(new_root_separator_count, 1U);

  pager.Close();
  RemoveIfExists(path);
}

TEST(BtreeIndex, RejectsOpenOnUnknownRootNodeType) {
  const std::filesystem::path path = UniqueDbPath();
  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  const std::uint32_t root_page_id = AllocatePage(&pager);
  const atlasdb::storage::Page unknown_page = atlasdb::storage::CreateZeroedPage(root_page_id);
  ASSERT_TRUE(pager.WritePage(unknown_page).ok);

  atlasdb::btree::BtreeIndex index(&pager);
  const atlasdb::btree::BtreeIndexStatus open_status = index.Open(root_page_id);
  ASSERT_FALSE(open_status.ok);
  EXPECT_EQ(open_status.code, "E5402");

  pager.Close();
  RemoveIfExists(path);
}

}  // namespace
