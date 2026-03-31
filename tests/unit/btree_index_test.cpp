#include "atlasdb/btree/index.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "atlasdb/btree/cursor.hpp"
#include "atlasdb/btree/internal_node.hpp"
#include "atlasdb/btree/leaf_node.hpp"
#include "atlasdb/storage/page.hpp"
#include "atlasdb/storage/pager.hpp"

namespace {

constexpr std::array<std::uint8_t, 8> kLeafNodeMagic = {
    static_cast<std::uint8_t>('A'),
    static_cast<std::uint8_t>('T'),
    static_cast<std::uint8_t>('L'),
    static_cast<std::uint8_t>('B'),
    static_cast<std::uint8_t>('L'),
    static_cast<std::uint8_t>('F'),
    0U,
    0U,
};

constexpr std::array<std::uint8_t, 8> kInternalNodeMagic = {
    static_cast<std::uint8_t>('A'),
    static_cast<std::uint8_t>('T'),
    static_cast<std::uint8_t>('L'),
    static_cast<std::uint8_t>('B'),
    static_cast<std::uint8_t>('I'),
    static_cast<std::uint8_t>('N'),
    0U,
    0U,
};

bool MatchesMagic(const atlasdb::storage::Page& page,
                  const std::array<std::uint8_t, 8>& magic) {
  for (std::size_t index = 0U; index < magic.size(); ++index) {
    if (page.bytes[index] != magic[index]) {
      return false;
    }
  }

  return true;
}

bool KeyWithinBounds(std::int64_t key,
                     const std::optional<std::int64_t>& min_inclusive,
                     const std::optional<std::int64_t>& max_exclusive) {
  if (min_inclusive.has_value() && key < *min_inclusive) {
    return false;
  }

  if (max_exclusive.has_value() && key >= *max_exclusive) {
    return false;
  }

  return true;
}

struct InvariantTraversal {
  std::set<std::uint32_t> visited_pages;
  std::vector<std::uint32_t> leaf_pages_in_order;
  std::vector<std::int64_t> keys_in_order;
};

::testing::AssertionResult ValidateSubtree(
    atlasdb::storage::Pager* pager,
    std::uint32_t page_id,
    const std::optional<std::int64_t>& min_inclusive,
    const std::optional<std::int64_t>& max_exclusive,
    InvariantTraversal* traversal) {
  if (pager == nullptr || traversal == nullptr) {
    return ::testing::AssertionFailure() << "null pager or traversal state";
  }

  if (page_id == 0U) {
    return ::testing::AssertionFailure() << "encountered zero page id during subtree walk";
  }

  if (traversal->visited_pages.find(page_id) != traversal->visited_pages.end()) {
    return ::testing::AssertionFailure() << "detected page cycle at page " << page_id;
  }
  traversal->visited_pages.insert(page_id);

  atlasdb::storage::Page page;
  const atlasdb::storage::PagerStatus read_status = pager->ReadPage(page_id, &page);
  if (!read_status.ok) {
    return ::testing::AssertionFailure()
           << "failed to read page " << page_id << " with code " << read_status.code;
  }

  if (MatchesMagic(page, kLeafNodeMagic)) {
    std::uint16_t entry_count = 0U;
    const atlasdb::btree::LeafNodeStatus count_status =
        atlasdb::btree::GetLeafEntryCount(page, &entry_count);
    if (!count_status.ok) {
      return ::testing::AssertionFailure()
             << "leaf layout invalid at page " << page_id << " code " << count_status.code;
    }

    std::optional<std::int64_t> previous_key;
    for (std::uint16_t index = 0U; index < entry_count; ++index) {
      atlasdb::btree::LeafEntry entry;
      const atlasdb::btree::LeafNodeStatus read_entry_status =
          atlasdb::btree::ReadLeafEntry(page, index, &entry);
      if (!read_entry_status.ok) {
        return ::testing::AssertionFailure()
               << "failed to read leaf entry on page " << page_id << " code "
               << read_entry_status.code;
      }

      if (!KeyWithinBounds(entry.key, min_inclusive, max_exclusive)) {
        return ::testing::AssertionFailure()
               << "leaf key " << entry.key << " violates range bounds on page " << page_id;
      }

      if (previous_key.has_value() && entry.key <= *previous_key) {
        return ::testing::AssertionFailure()
               << "leaf keys are not strictly increasing on page " << page_id;
      }

      previous_key = entry.key;
      traversal->keys_in_order.push_back(entry.key);
    }

    traversal->leaf_pages_in_order.push_back(page_id);
    return ::testing::AssertionSuccess();
  }

  if (MatchesMagic(page, kInternalNodeMagic)) {
    std::uint32_t left_child_page_id = 0U;
    const atlasdb::btree::InternalNodeStatus left_child_status =
        atlasdb::btree::GetInternalLeftChild(page, &left_child_page_id);
    if (!left_child_status.ok) {
      return ::testing::AssertionFailure()
             << "failed to read internal left child on page " << page_id << " code "
             << left_child_status.code;
    }

    std::uint16_t entry_count = 0U;
    const atlasdb::btree::InternalNodeStatus count_status =
        atlasdb::btree::GetInternalEntryCount(page, &entry_count);
    if (!count_status.ok) {
      return ::testing::AssertionFailure()
             << "internal layout invalid at page " << page_id << " code "
             << count_status.code;
    }

    std::vector<atlasdb::btree::InternalEntry> entries;
    entries.reserve(entry_count);
    for (std::uint16_t index = 0U; index < entry_count; ++index) {
      atlasdb::btree::InternalEntry entry;
      const atlasdb::btree::InternalNodeStatus read_entry_status =
          atlasdb::btree::ReadInternalEntry(page, index, &entry);
      if (!read_entry_status.ok) {
        return ::testing::AssertionFailure()
               << "failed to read internal entry on page " << page_id << " code "
               << read_entry_status.code;
      }

      if (!KeyWithinBounds(entry.key, min_inclusive, max_exclusive)) {
        return ::testing::AssertionFailure()
               << "separator key " << entry.key << " violates range bounds on page "
               << page_id;
      }

      entries.push_back(entry);
    }

    {
      const std::optional<std::int64_t> child_max =
          entries.empty() ? max_exclusive : std::optional<std::int64_t>(entries.front().key);
      const ::testing::AssertionResult left_result =
          ValidateSubtree(pager, left_child_page_id, min_inclusive, child_max, traversal);
      if (!left_result) {
        return left_result;
      }
    }

    for (std::size_t index = 0U; index < entries.size(); ++index) {
      const std::optional<std::int64_t> child_min(entries[index].key);
      const std::optional<std::int64_t> child_max =
          (index + 1U < entries.size()) ? std::optional<std::int64_t>(entries[index + 1U].key)
                                        : max_exclusive;

      const ::testing::AssertionResult child_result = ValidateSubtree(
          pager, entries[index].child_page_id, child_min, child_max, traversal);
      if (!child_result) {
        return child_result;
      }
    }

    return ::testing::AssertionSuccess();
  }

  return ::testing::AssertionFailure() << "unknown node magic at page " << page_id;
}

::testing::AssertionResult ValidateLeafChainMatchesTraversal(
    atlasdb::storage::Pager* pager,
    atlasdb::btree::BtreeIndex* index,
    const InvariantTraversal& traversal) {
  if (pager == nullptr || index == nullptr) {
    return ::testing::AssertionFailure() << "null pager or index for leaf-chain validation";
  }

  std::uint32_t first_leaf_page_id = 0U;
  const atlasdb::btree::BtreeIndexStatus first_leaf_status =
      index->ResolveFirstLeaf(&first_leaf_page_id);
  if (!first_leaf_status.ok) {
    return ::testing::AssertionFailure()
           << "failed to resolve first leaf page with code " << first_leaf_status.code;
  }

  std::set<std::uint32_t> seen_chain_pages;
  std::vector<std::uint32_t> chain_pages;
  std::vector<std::int64_t> chain_keys;

  const std::uint32_t hop_limit = pager->Header().page_count;
  std::uint32_t current_page_id = first_leaf_page_id;
  for (std::uint32_t hops = 0U; hops < hop_limit && current_page_id != 0U; ++hops) {
    if (seen_chain_pages.find(current_page_id) != seen_chain_pages.end()) {
      return ::testing::AssertionFailure() << "detected cycle in linked-leaf chain";
    }
    seen_chain_pages.insert(current_page_id);

    atlasdb::storage::Page leaf_page;
    const atlasdb::storage::PagerStatus read_status = pager->ReadPage(current_page_id, &leaf_page);
    if (!read_status.ok) {
      return ::testing::AssertionFailure()
             << "failed to read leaf page from chain code " << read_status.code;
    }

    if (!MatchesMagic(leaf_page, kLeafNodeMagic)) {
      return ::testing::AssertionFailure()
             << "linked-leaf chain reached non-leaf page " << current_page_id;
    }

    chain_pages.push_back(current_page_id);

    std::uint16_t entry_count = 0U;
    const atlasdb::btree::LeafNodeStatus count_status =
        atlasdb::btree::GetLeafEntryCount(leaf_page, &entry_count);
    if (!count_status.ok) {
      return ::testing::AssertionFailure()
             << "invalid leaf layout in chain code " << count_status.code;
    }

    for (std::uint16_t index_in_leaf = 0U; index_in_leaf < entry_count; ++index_in_leaf) {
      atlasdb::btree::LeafEntry entry;
      const atlasdb::btree::LeafNodeStatus read_entry_status =
          atlasdb::btree::ReadLeafEntry(leaf_page, index_in_leaf, &entry);
      if (!read_entry_status.ok) {
        return ::testing::AssertionFailure()
               << "failed reading chain leaf entry code " << read_entry_status.code;
      }
      chain_keys.push_back(entry.key);
    }

    std::uint32_t next_page_id = 0U;
    const atlasdb::btree::LeafNodeStatus next_status =
        atlasdb::btree::GetLeafNextPage(leaf_page, &next_page_id);
    if (!next_status.ok) {
      return ::testing::AssertionFailure()
             << "failed reading chain next-pointer code " << next_status.code;
    }

    current_page_id = next_page_id;
  }

  if (current_page_id != 0U) {
    return ::testing::AssertionFailure() << "linked-leaf traversal exceeded hop limit";
  }

  if (chain_pages != traversal.leaf_pages_in_order) {
    return ::testing::AssertionFailure() << "linked-leaf page order differs from in-order traversal";
  }

  if (chain_keys != traversal.keys_in_order) {
    return ::testing::AssertionFailure() << "linked-leaf key order differs from in-order traversal";
  }

  return ::testing::AssertionSuccess();
}

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

TEST(BtreeIndex, RandomizedInsertMaintainsTreeInvariants) {
  const std::filesystem::path path = UniqueDbPath();
  atlasdb::storage::Pager pager;
  ASSERT_TRUE(pager.Open(path.string()).ok);

  atlasdb::btree::BtreeIndex index(&pager);
  std::uint32_t root_page_id = 0U;
  ASSERT_TRUE(index.Initialize(&root_page_id).ok);

  constexpr std::int64_t kKeyCount = 3000;
  std::vector<std::int64_t> keys;
  keys.reserve(static_cast<std::size_t>(kKeyCount));
  for (std::int64_t key = 1; key <= kKeyCount; ++key) {
    keys.push_back(key);
  }

  std::mt19937_64 random_engine(0xC0FFEE1234ULL);
  std::shuffle(keys.begin(), keys.end(), random_engine);

  for (const std::int64_t key : keys) {
    const atlasdb::btree::BtreeIndexStatus insert_status =
        index.Insert(atlasdb::btree::LeafEntry{
            key,
            static_cast<std::uint32_t>(200000U + static_cast<std::uint32_t>(key)),
            static_cast<std::uint16_t>(key % 4096)});
    ASSERT_TRUE(insert_status.ok);
  }

  for (std::int64_t key = 1; key <= kKeyCount; ++key) {
    atlasdb::btree::LeafEntry found;
    const atlasdb::btree::BtreeIndexStatus find_status = index.Find(key, &found);
    ASSERT_TRUE(find_status.ok);
    EXPECT_EQ(found.key, key);
    EXPECT_EQ(found.row_page_id,
              static_cast<std::uint32_t>(200000U + static_cast<std::uint32_t>(key)));
  }

  std::uint32_t current_root_page_id = 0U;
  ASSERT_TRUE(index.GetRootPageId(&current_root_page_id).ok);

  InvariantTraversal traversal;
  const ::testing::AssertionResult subtree_result =
      ValidateSubtree(&pager, current_root_page_id, std::nullopt, std::nullopt, &traversal);
  ASSERT_TRUE(subtree_result);

  ASSERT_EQ(traversal.keys_in_order.size(), static_cast<std::size_t>(kKeyCount));
  for (std::size_t index_in_order = 0U; index_in_order < traversal.keys_in_order.size(); ++index_in_order) {
    EXPECT_EQ(traversal.keys_in_order[index_in_order],
              static_cast<std::int64_t>(index_in_order + 1U));
  }

  const ::testing::AssertionResult chain_result =
      ValidateLeafChainMatchesTraversal(&pager, &index, traversal);
  ASSERT_TRUE(chain_result);

  pager.Close();
  RemoveIfExists(path);
}

}  // namespace
