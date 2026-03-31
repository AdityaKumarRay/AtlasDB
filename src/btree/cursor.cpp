#include "atlasdb/btree/cursor.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>

namespace atlasdb::btree {

BtreeCursorStatus BtreeCursorStatus::Ok(std::string message) {
  return BtreeCursorStatus{true, "", std::move(message)};
}

BtreeCursorStatus BtreeCursorStatus::Error(std::string code, std::string message) {
  return BtreeCursorStatus{false, std::move(code), std::move(message)};
}

LeafCursor::LeafCursor(storage::Pager* pager) : pager_(pager) {}

BtreeCursorStatus LeafCursor::EnsurePagerOpen() const {
  if (pager_ == nullptr || !pager_->IsOpen()) {
    return BtreeCursorStatus::Error("E5301", "btree cursor pager is not open");
  }

  return BtreeCursorStatus::Ok();
}

BtreeCursorStatus LeafCursor::LoadLeafPage(std::uint32_t page_id,
                                           storage::Page* out_page,
                                           std::uint16_t* out_entry_count,
                                           std::uint32_t* out_next_page_id) const {
  if (out_page == nullptr || out_entry_count == nullptr || out_next_page_id == nullptr) {
    return BtreeCursorStatus::Error("E5300", "cursor output pointer is null");
  }

  if (page_id == 0U) {
    return BtreeCursorStatus::Error("E5302", "starting leaf page id must be non-zero");
  }

  const storage::PagerStatus read_status = pager_->ReadPage(page_id, out_page);
  if (!read_status.ok) {
    return BtreeCursorStatus::Error("E5303", read_status.code + ": " + read_status.message);
  }

  const LeafNodeStatus count_status = GetLeafEntryCount(*out_page, out_entry_count);
  if (!count_status.ok) {
    return BtreeCursorStatus::Error(count_status.code, count_status.message);
  }

  const LeafNodeStatus next_status = GetLeafNextPage(*out_page, out_next_page_id);
  if (!next_status.ok) {
    return BtreeCursorStatus::Error(next_status.code, next_status.message);
  }

  if (*out_next_page_id == page_id) {
    return BtreeCursorStatus::Error("E5305", "leaf chain contains a self-cycle");
  }

  return BtreeCursorStatus::Ok();
}

BtreeCursorStatus LeafCursor::SeekToFirstInChain(std::uint32_t start_leaf_page_id) {
  const std::uint32_t page_hop_limit = pager_->Header().page_count;
  if (page_hop_limit == 0U) {
    return BtreeCursorStatus::Error("E5305", "leaf chain traversal has invalid hop limit");
  }

  std::uint32_t current_page_id = start_leaf_page_id;
  for (std::uint32_t hop_count = 0U; hop_count < page_hop_limit; ++hop_count) {
    storage::Page page;
    std::uint16_t entry_count = 0U;
    std::uint32_t next_page_id = 0U;
    const BtreeCursorStatus load_status =
        LoadLeafPage(current_page_id, &page, &entry_count, &next_page_id);
    if (!load_status.ok) {
      return load_status;
    }

    if (entry_count > 0U) {
      current_leaf_page_id_ = current_page_id;
      current_entry_index_ = 0U;
      is_valid_ = true;
      return BtreeCursorStatus::Ok("cursor positioned on first entry in leaf chain");
    }

    if (next_page_id == 0U) {
      current_leaf_page_id_ = 0U;
      current_entry_index_ = 0U;
      is_valid_ = false;
      return BtreeCursorStatus::Ok("cursor positioned at end");
    }

    current_page_id = next_page_id;
  }

  return BtreeCursorStatus::Error("E5305", "leaf chain traversal exceeded page-count bound");
}

BtreeCursorStatus LeafCursor::SeekFirst(std::uint32_t first_leaf_page_id) {
  const BtreeCursorStatus pager_status = EnsurePagerOpen();
  if (!pager_status.ok) {
    return pager_status;
  }

  if (first_leaf_page_id == 0U) {
    return BtreeCursorStatus::Error("E5302", "starting leaf page id must be non-zero");
  }

  return SeekToFirstInChain(first_leaf_page_id);
}

BtreeCursorStatus LeafCursor::Seek(std::uint32_t first_leaf_page_id, std::int64_t key) {
  const BtreeCursorStatus pager_status = EnsurePagerOpen();
  if (!pager_status.ok) {
    return pager_status;
  }

  if (first_leaf_page_id == 0U) {
    return BtreeCursorStatus::Error("E5302", "starting leaf page id must be non-zero");
  }

  const std::uint32_t page_hop_limit = pager_->Header().page_count;
  if (page_hop_limit == 0U) {
    return BtreeCursorStatus::Error("E5305", "leaf chain traversal has invalid hop limit");
  }

  std::uint32_t current_page_id = first_leaf_page_id;
  for (std::uint32_t hop_count = 0U; hop_count < page_hop_limit; ++hop_count) {
    storage::Page page;
    std::uint16_t entry_count = 0U;
    std::uint32_t next_page_id = 0U;
    const BtreeCursorStatus load_status =
        LoadLeafPage(current_page_id, &page, &entry_count, &next_page_id);
    if (!load_status.ok) {
      return load_status;
    }

    if (entry_count > 0U) {
      std::uint16_t left = 0U;
      std::uint16_t right = entry_count;

      while (left < right) {
        const std::uint16_t mid =
            static_cast<std::uint16_t>(left + static_cast<std::uint16_t>((right - left) / 2U));

        LeafEntry mid_entry;
        const LeafNodeStatus read_status = ReadLeafEntry(page, mid, &mid_entry);
        if (!read_status.ok) {
          return BtreeCursorStatus::Error(read_status.code, read_status.message);
        }

        if (mid_entry.key < key) {
          left = static_cast<std::uint16_t>(mid + 1U);
        } else {
          right = mid;
        }
      }

      if (left < entry_count) {
        current_leaf_page_id_ = current_page_id;
        current_entry_index_ = left;
        is_valid_ = true;
        return BtreeCursorStatus::Ok("cursor positioned at key or next greater entry");
      }
    }

    if (next_page_id == 0U) {
      current_leaf_page_id_ = 0U;
      current_entry_index_ = 0U;
      is_valid_ = false;
      return BtreeCursorStatus::Ok("cursor positioned at end");
    }

    current_page_id = next_page_id;
  }

  return BtreeCursorStatus::Error("E5305", "leaf chain traversal exceeded page-count bound");
}

BtreeCursorStatus LeafCursor::Next() {
  const BtreeCursorStatus pager_status = EnsurePagerOpen();
  if (!pager_status.ok) {
    return pager_status;
  }

  if (!is_valid_) {
    return BtreeCursorStatus::Error("E5304", "cursor is not positioned on an entry");
  }

  storage::Page page;
  std::uint16_t entry_count = 0U;
  std::uint32_t next_page_id = 0U;
  const BtreeCursorStatus load_status =
      LoadLeafPage(current_leaf_page_id_, &page, &entry_count, &next_page_id);
  if (!load_status.ok) {
    return load_status;
  }

  if (current_entry_index_ + 1U < entry_count) {
    current_entry_index_ = static_cast<std::uint16_t>(current_entry_index_ + 1U);
    return BtreeCursorStatus::Ok("cursor advanced within current leaf page");
  }

  if (next_page_id == 0U) {
    current_leaf_page_id_ = 0U;
    current_entry_index_ = 0U;
    is_valid_ = false;
    return BtreeCursorStatus::Ok("cursor advanced to end");
  }

  const BtreeCursorStatus seek_status = SeekToFirstInChain(next_page_id);
  if (!seek_status.ok) {
    return seek_status;
  }

  if (is_valid_) {
    return BtreeCursorStatus::Ok("cursor advanced to next leaf entry");
  }

  return BtreeCursorStatus::Ok("cursor advanced to end");
}

BtreeCursorStatus LeafCursor::Current(LeafEntry* out_entry) const {
  if (out_entry == nullptr) {
    return BtreeCursorStatus::Error("E5300", "output leaf entry pointer is null");
  }

  const BtreeCursorStatus pager_status = EnsurePagerOpen();
  if (!pager_status.ok) {
    return pager_status;
  }

  if (!is_valid_) {
    return BtreeCursorStatus::Error("E5304", "cursor is not positioned on an entry");
  }

  storage::Page page;
  const storage::PagerStatus read_status = pager_->ReadPage(current_leaf_page_id_, &page);
  if (!read_status.ok) {
    return BtreeCursorStatus::Error("E5303", read_status.code + ": " + read_status.message);
  }

  const LeafNodeStatus entry_status = ReadLeafEntry(page, current_entry_index_, out_entry);
  if (!entry_status.ok) {
    return BtreeCursorStatus::Error(entry_status.code, entry_status.message);
  }

  return BtreeCursorStatus::Ok("cursor current entry available");
}

bool LeafCursor::IsValid() const noexcept {
  return is_valid_;
}

}  // namespace atlasdb::btree
