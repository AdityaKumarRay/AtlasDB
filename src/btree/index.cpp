#include "atlasdb/btree/index.hpp"

#include <array>
#include <cstdint>
#include <string>
#include <utility>

#include "atlasdb/btree/internal_node.hpp"
#include "atlasdb/storage/page.hpp"

namespace atlasdb::btree {
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

enum class NodeType {
  kLeaf,
  kInternal,
  kUnknown,
};

bool MatchesMagic(const storage::Page& page, const std::array<std::uint8_t, 8>& magic) {
  for (std::size_t index = 0U; index < magic.size(); ++index) {
    if (page.bytes[index] != magic[index]) {
      return false;
    }
  }

  return true;
}

NodeType DetectNodeType(const storage::Page& page) {
  if (MatchesMagic(page, kLeafNodeMagic)) {
    return NodeType::kLeaf;
  }

  if (MatchesMagic(page, kInternalNodeMagic)) {
    return NodeType::kInternal;
  }

  return NodeType::kUnknown;
}

BtreeIndexStatus FromPagerStatus(const storage::PagerStatus& status) {
  return BtreeIndexStatus::Error(status.code, status.message);
}

BtreeIndexStatus FromLeafStatus(const LeafNodeStatus& status) {
  return BtreeIndexStatus::Error(status.code, status.message);
}

BtreeIndexStatus FromInternalStatus(const InternalNodeStatus& status) {
  return BtreeIndexStatus::Error(status.code, status.message);
}

BtreeIndexStatus ContainsInternalSeparatorKey(const storage::Page& page,
                                              std::int64_t key,
                                              bool* out_contains) {
  if (out_contains == nullptr) {
    return BtreeIndexStatus::Error("E5400", "output duplicate-check pointer is null");
  }

  std::uint16_t entry_count = 0U;
  const InternalNodeStatus count_status = GetInternalEntryCount(page, &entry_count);
  if (!count_status.ok) {
    return FromInternalStatus(count_status);
  }

  for (std::uint16_t index = 0U; index < entry_count; ++index) {
    InternalEntry entry;
    const InternalNodeStatus read_status = ReadInternalEntry(page, index, &entry);
    if (!read_status.ok) {
      return FromInternalStatus(read_status);
    }

    if (entry.key == key) {
      *out_contains = true;
      return BtreeIndexStatus::Ok("internal separator key already exists");
    }
  }

  *out_contains = false;
  return BtreeIndexStatus::Ok("internal separator key does not exist");
}

}  // namespace

BtreeIndexStatus BtreeIndexStatus::Ok(std::string message) {
  return BtreeIndexStatus{true, "", std::move(message)};
}

BtreeIndexStatus BtreeIndexStatus::Error(std::string code, std::string message) {
  return BtreeIndexStatus{false, std::move(code), std::move(message)};
}

BtreeIndex::BtreeIndex(storage::Pager* pager) : pager_(pager) {}

BtreeIndexStatus BtreeIndex::EnsurePagerOpen() const {
  if (pager_ == nullptr || !pager_->IsOpen()) {
    return BtreeIndexStatus::Error("E5401", "btree index pager is not open");
  }

  return BtreeIndexStatus::Ok();
}

BtreeIndexStatus BtreeIndex::EnsureRootInitialized() const {
  if (root_page_id_ == 0U) {
    return BtreeIndexStatus::Error("E5403", "btree index root page is not initialized");
  }

  return BtreeIndexStatus::Ok();
}

BtreeIndexStatus BtreeIndex::ReadTreePage(std::uint32_t page_id,
                                          storage::Page* out_page) const {
  if (out_page == nullptr) {
    return BtreeIndexStatus::Error("E5400", "output page pointer is null");
  }

  if (page_id == 0U) {
    return BtreeIndexStatus::Error("E5403", "btree page id must be non-zero");
  }

  const storage::PagerStatus read_status = pager_->ReadPage(page_id, out_page);
  if (!read_status.ok) {
    return FromPagerStatus(read_status);
  }

  return BtreeIndexStatus::Ok();
}

BtreeIndexStatus BtreeIndex::Initialize(std::uint32_t* out_root_page_id) {
  if (out_root_page_id == nullptr) {
    return BtreeIndexStatus::Error("E5400", "output root page id pointer is null");
  }

  const BtreeIndexStatus pager_status = EnsurePagerOpen();
  if (!pager_status.ok) {
    return pager_status;
  }

  std::uint32_t new_root_page_id = 0U;
  const storage::PagerStatus allocate_status = pager_->AllocatePage(&new_root_page_id);
  if (!allocate_status.ok) {
    return FromPagerStatus(allocate_status);
  }

  storage::Page root_page = storage::CreateZeroedPage(new_root_page_id);
  const LeafNodeStatus init_status = InitializeLeafNode(&root_page);
  if (!init_status.ok) {
    return FromLeafStatus(init_status);
  }

  const storage::PagerStatus write_status = pager_->WritePage(root_page);
  if (!write_status.ok) {
    return FromPagerStatus(write_status);
  }

  root_page_id_ = new_root_page_id;
  *out_root_page_id = new_root_page_id;
  return BtreeIndexStatus::Ok("initialized btree index root");
}

BtreeIndexStatus BtreeIndex::Open(std::uint32_t root_page_id) {
  const BtreeIndexStatus pager_status = EnsurePagerOpen();
  if (!pager_status.ok) {
    return pager_status;
  }

  if (root_page_id == 0U) {
    return BtreeIndexStatus::Error("E5403", "btree root page id must be non-zero");
  }

  storage::Page root_page;
  const BtreeIndexStatus read_status = ReadTreePage(root_page_id, &root_page);
  if (!read_status.ok) {
    return read_status;
  }

  const NodeType root_type = DetectNodeType(root_page);
  if (root_type == NodeType::kLeaf) {
    std::uint16_t entry_count = 0U;
    const LeafNodeStatus count_status = GetLeafEntryCount(root_page, &entry_count);
    if (!count_status.ok) {
      return FromLeafStatus(count_status);
    }
  } else if (root_type == NodeType::kInternal) {
    std::uint16_t entry_count = 0U;
    const InternalNodeStatus count_status = GetInternalEntryCount(root_page, &entry_count);
    if (!count_status.ok) {
      return FromInternalStatus(count_status);
    }
  } else {
    return BtreeIndexStatus::Error("E5402", "btree root page has unknown node magic");
  }

  root_page_id_ = root_page_id;
  return BtreeIndexStatus::Ok("opened btree index root");
}

BtreeIndexStatus BtreeIndex::GetRootPageId(std::uint32_t* out_root_page_id) const {
  if (out_root_page_id == nullptr) {
    return BtreeIndexStatus::Error("E5400", "output root page id pointer is null");
  }

  const BtreeIndexStatus root_status = EnsureRootInitialized();
  if (!root_status.ok) {
    return root_status;
  }

  *out_root_page_id = root_page_id_;
  return BtreeIndexStatus::Ok("btree root page id available");
}

BtreeIndexStatus BtreeIndex::FindLeafPageForKey(std::int64_t key,
                                                std::uint32_t* out_leaf_page_id) const {
  if (out_leaf_page_id == nullptr) {
    return BtreeIndexStatus::Error("E5400", "output leaf page id pointer is null");
  }

  const BtreeIndexStatus pager_status = EnsurePagerOpen();
  if (!pager_status.ok) {
    return pager_status;
  }

  const BtreeIndexStatus root_status = EnsureRootInitialized();
  if (!root_status.ok) {
    return root_status;
  }

  const std::uint32_t page_hop_limit = pager_->Header().page_count;
  if (page_hop_limit == 0U) {
    return BtreeIndexStatus::Error("E5405", "btree traversal has invalid hop limit");
  }

  std::uint32_t current_page_id = root_page_id_;
  for (std::uint32_t hop_count = 0U; hop_count < page_hop_limit; ++hop_count) {
    storage::Page page;
    const BtreeIndexStatus read_status = ReadTreePage(current_page_id, &page);
    if (!read_status.ok) {
      return read_status;
    }

    const NodeType node_type = DetectNodeType(page);
    if (node_type == NodeType::kLeaf) {
      std::uint16_t entry_count = 0U;
      const LeafNodeStatus count_status = GetLeafEntryCount(page, &entry_count);
      if (!count_status.ok) {
        return FromLeafStatus(count_status);
      }

      *out_leaf_page_id = current_page_id;
      return BtreeIndexStatus::Ok("resolved leaf page for key");
    }

    if (node_type == NodeType::kInternal) {
      std::uint32_t child_page_id = 0U;
      const InternalNodeStatus child_status =
          FindInternalChildForKey(page, key, &child_page_id);
      if (!child_status.ok) {
        return FromInternalStatus(child_status);
      }

      current_page_id = child_page_id;
      continue;
    }

    return BtreeIndexStatus::Error("E5402", "btree page has unknown node magic");
  }

  return BtreeIndexStatus::Error("E5405", "btree traversal exceeded declared page-count bound");
}

BtreeIndexStatus BtreeIndex::ResolveLeafForKey(std::int64_t key,
                                               std::uint32_t* out_leaf_page_id) const {
  return FindLeafPageForKey(key, out_leaf_page_id);
}

BtreeIndexStatus BtreeIndex::ResolveFirstLeaf(std::uint32_t* out_leaf_page_id) const {
  if (out_leaf_page_id == nullptr) {
    return BtreeIndexStatus::Error("E5400", "output leaf page id pointer is null");
  }

  const BtreeIndexStatus pager_status = EnsurePagerOpen();
  if (!pager_status.ok) {
    return pager_status;
  }

  const BtreeIndexStatus root_status = EnsureRootInitialized();
  if (!root_status.ok) {
    return root_status;
  }

  const std::uint32_t page_hop_limit = pager_->Header().page_count;
  if (page_hop_limit == 0U) {
    return BtreeIndexStatus::Error("E5405", "btree traversal has invalid hop limit");
  }

  std::uint32_t current_page_id = root_page_id_;
  for (std::uint32_t hop_count = 0U; hop_count < page_hop_limit; ++hop_count) {
    storage::Page page;
    const BtreeIndexStatus read_status = ReadTreePage(current_page_id, &page);
    if (!read_status.ok) {
      return read_status;
    }

    const NodeType node_type = DetectNodeType(page);
    if (node_type == NodeType::kLeaf) {
      std::uint16_t entry_count = 0U;
      const LeafNodeStatus count_status = GetLeafEntryCount(page, &entry_count);
      if (!count_status.ok) {
        return FromLeafStatus(count_status);
      }

      *out_leaf_page_id = current_page_id;
      return BtreeIndexStatus::Ok("resolved first leaf page");
    }

    if (node_type == NodeType::kInternal) {
      std::uint32_t left_child_page_id = 0U;
      const InternalNodeStatus left_child_status =
          GetInternalLeftChild(page, &left_child_page_id);
      if (!left_child_status.ok) {
        return FromInternalStatus(left_child_status);
      }

      current_page_id = left_child_page_id;
      continue;
    }

    return BtreeIndexStatus::Error("E5402", "btree page has unknown node magic");
  }

  return BtreeIndexStatus::Error("E5405", "btree traversal exceeded declared page-count bound");
}

BtreeIndexStatus BtreeIndex::Find(std::int64_t key, LeafEntry* out_entry) const {
  if (out_entry == nullptr) {
    return BtreeIndexStatus::Error("E5400", "output leaf entry pointer is null");
  }

  std::uint32_t leaf_page_id = 0U;
  const BtreeIndexStatus leaf_status = FindLeafPageForKey(key, &leaf_page_id);
  if (!leaf_status.ok) {
    return leaf_status;
  }

  storage::Page leaf_page;
  const BtreeIndexStatus read_status = ReadTreePage(leaf_page_id, &leaf_page);
  if (!read_status.ok) {
    return read_status;
  }

  const LeafNodeStatus find_status = FindLeafEntryByKey(leaf_page, key, out_entry, nullptr);
  if (!find_status.ok) {
    return FromLeafStatus(find_status);
  }

  return BtreeIndexStatus::Ok("found btree key");
}

BtreeIndexStatus BtreeIndex::Insert(const LeafEntry& entry) {
  const BtreeIndexStatus pager_status = EnsurePagerOpen();
  if (!pager_status.ok) {
    return pager_status;
  }

  const BtreeIndexStatus root_status = EnsureRootInitialized();
  if (!root_status.ok) {
    return root_status;
  }

  SplitResult split_result;
  const BtreeIndexStatus insert_status =
      InsertRecursive(root_page_id_, entry, &split_result);
  if (!insert_status.ok) {
    return insert_status;
  }

  if (!split_result.did_split) {
    return BtreeIndexStatus::Ok("inserted btree key");
  }

  std::uint32_t new_root_page_id = 0U;
  const storage::PagerStatus allocate_status = pager_->AllocatePage(&new_root_page_id);
  if (!allocate_status.ok) {
    return FromPagerStatus(allocate_status);
  }

  storage::Page new_root_page = storage::CreateZeroedPage(new_root_page_id);
  const InternalNodeStatus init_status = InitializeInternalRootFromSplit(
      &new_root_page,
      root_page_id_,
      split_result.promoted_key,
      split_result.right_page_id);
  if (!init_status.ok) {
    return FromInternalStatus(init_status);
  }

  const storage::PagerStatus write_status = pager_->WritePage(new_root_page);
  if (!write_status.ok) {
    return FromPagerStatus(write_status);
  }

  root_page_id_ = new_root_page_id;
  return BtreeIndexStatus::Ok("inserted btree key and promoted new root");
}

BtreeIndexStatus BtreeIndex::InsertRecursive(std::uint32_t page_id,
                                             const LeafEntry& entry,
                                             SplitResult* out_split_result) {
  if (out_split_result == nullptr) {
    return BtreeIndexStatus::Error("E5400", "output split-result pointer is null");
  }

  out_split_result->did_split = false;
  out_split_result->promoted_key = 0;
  out_split_result->right_page_id = 0U;

  storage::Page page;
  const BtreeIndexStatus read_status = ReadTreePage(page_id, &page);
  if (!read_status.ok) {
    return read_status;
  }

  const NodeType node_type = DetectNodeType(page);
  if (node_type == NodeType::kLeaf) {
    std::uint16_t inserted_index = 0U;
    const LeafNodeStatus insert_status = InsertLeafEntry(&page, entry, &inserted_index);
    if (insert_status.ok) {
      const storage::PagerStatus write_status = pager_->WritePage(page);
      if (!write_status.ok) {
        return FromPagerStatus(write_status);
      }

      return BtreeIndexStatus::Ok("inserted key into leaf page");
    }

    if (insert_status.code != "E5103") {
      return FromLeafStatus(insert_status);
    }

    LeafEntry existing_entry;
    std::uint16_t existing_index = 0U;
    const LeafNodeStatus duplicate_status =
        FindLeafEntryByKey(page, entry.key, &existing_entry, &existing_index);
    if (duplicate_status.ok) {
      return BtreeIndexStatus::Error("E5104", "leaf entry key must be strictly increasing");
    }

    if (duplicate_status.code != "E5105") {
      return FromLeafStatus(duplicate_status);
    }

    std::uint32_t right_page_id = 0U;
    const storage::PagerStatus allocate_status = pager_->AllocatePage(&right_page_id);
    if (!allocate_status.ok) {
      return FromPagerStatus(allocate_status);
    }

    storage::Page right_page = storage::CreateZeroedPage(right_page_id);
    LeafSplitMetadata split_metadata;
    const LeafNodeStatus split_status =
        SplitLeafNode(&page, &right_page, &split_metadata);
    if (!split_status.ok) {
      return FromLeafStatus(split_status);
    }

    storage::Page* target_page =
        (entry.key >= split_metadata.promoted_key) ? &right_page : &page;
    const LeafNodeStatus post_split_insert_status =
        InsertLeafEntry(target_page, entry, &inserted_index);
    if (!post_split_insert_status.ok) {
      return FromLeafStatus(post_split_insert_status);
    }

    const storage::PagerStatus write_left_status = pager_->WritePage(page);
    if (!write_left_status.ok) {
      return FromPagerStatus(write_left_status);
    }

    const storage::PagerStatus write_right_status = pager_->WritePage(right_page);
    if (!write_right_status.ok) {
      return FromPagerStatus(write_right_status);
    }

    out_split_result->did_split = true;
    out_split_result->promoted_key = split_metadata.promoted_key;
    out_split_result->right_page_id = split_metadata.right_page_id;
    return BtreeIndexStatus::Ok("inserted key by splitting leaf page");
  }

  if (node_type == NodeType::kInternal) {
    std::uint32_t child_page_id = 0U;
    const InternalNodeStatus child_status =
        FindInternalChildForKey(page, entry.key, &child_page_id);
    if (!child_status.ok) {
      return FromInternalStatus(child_status);
    }

    SplitResult child_split;
    const BtreeIndexStatus child_insert_status =
        InsertRecursive(child_page_id, entry, &child_split);
    if (!child_insert_status.ok) {
      return child_insert_status;
    }

    if (!child_split.did_split) {
      return BtreeIndexStatus::Ok("inserted key into child subtree");
    }

    const InternalEntry separator_entry{child_split.promoted_key, child_split.right_page_id};
    std::uint16_t inserted_index = 0U;
    const InternalNodeStatus insert_status =
        InsertInternalEntry(&page, separator_entry, &inserted_index);
    if (insert_status.ok) {
      const storage::PagerStatus write_status = pager_->WritePage(page);
      if (!write_status.ok) {
        return FromPagerStatus(write_status);
      }

      return BtreeIndexStatus::Ok("inserted promoted separator into internal page");
    }

    if (insert_status.code != "E5203") {
      return FromInternalStatus(insert_status);
    }

    bool duplicate_separator = false;
    const BtreeIndexStatus duplicate_status =
        ContainsInternalSeparatorKey(page, separator_entry.key, &duplicate_separator);
    if (!duplicate_status.ok) {
      return duplicate_status;
    }

    if (duplicate_separator) {
      return BtreeIndexStatus::Error("E5204", "internal separator key must be unique");
    }

    std::uint32_t right_page_id = 0U;
    const storage::PagerStatus allocate_status = pager_->AllocatePage(&right_page_id);
    if (!allocate_status.ok) {
      return FromPagerStatus(allocate_status);
    }

    storage::Page right_page = storage::CreateZeroedPage(right_page_id);
    InternalSplitMetadata split_metadata;
    const InternalNodeStatus split_status =
        SplitInternalNode(&page, &right_page, &split_metadata);
    if (!split_status.ok) {
      return FromInternalStatus(split_status);
    }

    storage::Page* target_page =
        (separator_entry.key >= split_metadata.promoted_key) ? &right_page : &page;
    const InternalNodeStatus post_split_insert_status =
        InsertInternalEntry(target_page, separator_entry, &inserted_index);
    if (!post_split_insert_status.ok) {
      return FromInternalStatus(post_split_insert_status);
    }

    const storage::PagerStatus write_left_status = pager_->WritePage(page);
    if (!write_left_status.ok) {
      return FromPagerStatus(write_left_status);
    }

    const storage::PagerStatus write_right_status = pager_->WritePage(right_page);
    if (!write_right_status.ok) {
      return FromPagerStatus(write_right_status);
    }

    out_split_result->did_split = true;
    out_split_result->promoted_key = split_metadata.promoted_key;
    out_split_result->right_page_id = split_metadata.right_page_id;
    return BtreeIndexStatus::Ok("inserted separator by splitting internal page");
  }

  return BtreeIndexStatus::Error("E5402", "btree page has unknown node magic");
}

}  // namespace atlasdb::btree
