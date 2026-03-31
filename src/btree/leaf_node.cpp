#include "atlasdb/btree/leaf_node.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

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

constexpr std::size_t kMagicOffset = 0U;
constexpr std::size_t kVersionOffset = 8U;
constexpr std::size_t kEntryCountOffset = 10U;
constexpr std::size_t kNextPageOffset = 12U;
constexpr std::size_t kEntriesOffset = kLeafNodeHeaderSize;

void WriteUint16(storage::Page* page, std::size_t offset, std::uint16_t value) {
  page->bytes[offset + 0U] = static_cast<std::uint8_t>(value & 0xFFU);
  page->bytes[offset + 1U] = static_cast<std::uint8_t>((value >> 8U) & 0xFFU);
}

void WriteUint32(storage::Page* page, std::size_t offset, std::uint32_t value) {
  page->bytes[offset + 0U] = static_cast<std::uint8_t>(value & 0xFFU);
  page->bytes[offset + 1U] = static_cast<std::uint8_t>((value >> 8U) & 0xFFU);
  page->bytes[offset + 2U] = static_cast<std::uint8_t>((value >> 16U) & 0xFFU);
  page->bytes[offset + 3U] = static_cast<std::uint8_t>((value >> 24U) & 0xFFU);
}

void WriteUint64(storage::Page* page, std::size_t offset, std::uint64_t value) {
  page->bytes[offset + 0U] = static_cast<std::uint8_t>(value & 0xFFU);
  page->bytes[offset + 1U] = static_cast<std::uint8_t>((value >> 8U) & 0xFFU);
  page->bytes[offset + 2U] = static_cast<std::uint8_t>((value >> 16U) & 0xFFU);
  page->bytes[offset + 3U] = static_cast<std::uint8_t>((value >> 24U) & 0xFFU);
  page->bytes[offset + 4U] = static_cast<std::uint8_t>((value >> 32U) & 0xFFU);
  page->bytes[offset + 5U] = static_cast<std::uint8_t>((value >> 40U) & 0xFFU);
  page->bytes[offset + 6U] = static_cast<std::uint8_t>((value >> 48U) & 0xFFU);
  page->bytes[offset + 7U] = static_cast<std::uint8_t>((value >> 56U) & 0xFFU);
}

std::uint16_t ReadUint16(const storage::Page& page, std::size_t offset) {
  const std::uint16_t b0 = static_cast<std::uint16_t>(page.bytes[offset + 0U]);
  const std::uint16_t b1 = static_cast<std::uint16_t>(page.bytes[offset + 1U]) << 8U;
  return static_cast<std::uint16_t>(b0 | b1);
}

std::uint32_t ReadUint32(const storage::Page& page, std::size_t offset) {
  const std::uint32_t b0 = static_cast<std::uint32_t>(page.bytes[offset + 0U]);
  const std::uint32_t b1 = static_cast<std::uint32_t>(page.bytes[offset + 1U]) << 8U;
  const std::uint32_t b2 = static_cast<std::uint32_t>(page.bytes[offset + 2U]) << 16U;
  const std::uint32_t b3 = static_cast<std::uint32_t>(page.bytes[offset + 3U]) << 24U;
  return b0 | b1 | b2 | b3;
}

std::uint64_t ReadUint64(const storage::Page& page, std::size_t offset) {
  std::uint64_t value = 0U;
  value |= static_cast<std::uint64_t>(page.bytes[offset + 0U]);
  value |= static_cast<std::uint64_t>(page.bytes[offset + 1U]) << 8U;
  value |= static_cast<std::uint64_t>(page.bytes[offset + 2U]) << 16U;
  value |= static_cast<std::uint64_t>(page.bytes[offset + 3U]) << 24U;
  value |= static_cast<std::uint64_t>(page.bytes[offset + 4U]) << 32U;
  value |= static_cast<std::uint64_t>(page.bytes[offset + 5U]) << 40U;
  value |= static_cast<std::uint64_t>(page.bytes[offset + 6U]) << 48U;
  value |= static_cast<std::uint64_t>(page.bytes[offset + 7U]) << 56U;
  return value;
}

void WriteLeafEntry(storage::Page* page, std::size_t entry_index, const LeafEntry& entry) {
  const std::size_t entry_offset = kEntriesOffset + entry_index * kLeafNodeEntrySize;
  WriteUint64(page, entry_offset + 0U, static_cast<std::uint64_t>(entry.key));
  WriteUint32(page, entry_offset + 8U, entry.row_page_id);
  WriteUint16(page, entry_offset + 12U, entry.row_slot_index);
  WriteUint16(page, entry_offset + 14U, 0U);
}

LeafEntry ReadLeafEntryAt(const storage::Page& page, std::size_t entry_index) {
  const std::size_t entry_offset = kEntriesOffset + entry_index * kLeafNodeEntrySize;
  LeafEntry entry;
  entry.key = static_cast<std::int64_t>(ReadUint64(page, entry_offset + 0U));
  entry.row_page_id = ReadUint32(page, entry_offset + 8U);
  entry.row_slot_index = ReadUint16(page, entry_offset + 12U);
  return entry;
}

LeafNodeStatus ValidateLeafLayout(const storage::Page& page, std::uint16_t* out_entry_count) {
  for (std::size_t index = 0U; index < kLeafNodeMagic.size(); ++index) {
    if (page.bytes[kMagicOffset + index] != kLeafNodeMagic[index]) {
      return LeafNodeStatus::Error("E5101", "leaf node has invalid magic");
    }
  }

  const std::uint16_t version = ReadUint16(page, kVersionOffset);
  if (version != kLeafNodeFormatVersion) {
    return LeafNodeStatus::Error("E5101", "leaf node has unsupported format version");
  }

  const std::uint16_t entry_count = ReadUint16(page, kEntryCountOffset);
  if (entry_count > static_cast<std::uint16_t>(kLeafNodeMaxEntries)) {
    return LeafNodeStatus::Error("E5101", "leaf node entry count exceeds capacity");
  }

  if (entry_count > 1U) {
    LeafEntry previous = ReadLeafEntryAt(page, 0U);
    for (std::uint16_t index = 1U; index < entry_count; ++index) {
      const LeafEntry current = ReadLeafEntryAt(page, index);
      if (current.key <= previous.key) {
        return LeafNodeStatus::Error("E5101", "leaf node keys are not strictly increasing");
      }
      previous = current;
    }
  }

  if (out_entry_count != nullptr) {
    *out_entry_count = entry_count;
  }

  return LeafNodeStatus::Ok();
}

}  // namespace

LeafNodeStatus LeafNodeStatus::Ok(std::string message) {
  return LeafNodeStatus{true, "", std::move(message)};
}

LeafNodeStatus LeafNodeStatus::Error(std::string code, std::string message) {
  return LeafNodeStatus{false, std::move(code), std::move(message)};
}

LeafNodeStatus InitializeLeafNode(storage::Page* page) {
  if (page == nullptr) {
    return LeafNodeStatus::Error("E5100", "leaf node page pointer is null");
  }

  page->bytes.fill(0U);
  for (std::size_t index = 0U; index < kLeafNodeMagic.size(); ++index) {
    page->bytes[kMagicOffset + index] = kLeafNodeMagic[index];
  }

  WriteUint16(page, kVersionOffset, kLeafNodeFormatVersion);
  WriteUint16(page, kEntryCountOffset, 0U);
  WriteUint32(page, kNextPageOffset, 0U);

  return LeafNodeStatus::Ok("initialized leaf node");
}

LeafNodeStatus AppendLeafEntry(storage::Page* page,
                               const LeafEntry& entry,
                               std::uint16_t* out_index) {
  if (page == nullptr) {
    return LeafNodeStatus::Error("E5100", "leaf node page pointer is null");
  }

  if (out_index == nullptr) {
    return LeafNodeStatus::Error("E5100", "output entry index pointer is null");
  }

  std::uint16_t entry_count = 0U;
  const LeafNodeStatus layout_status = ValidateLeafLayout(*page, &entry_count);
  if (!layout_status.ok) {
    return layout_status;
  }

  if (entry_count == static_cast<std::uint16_t>(kLeafNodeMaxEntries)) {
    return LeafNodeStatus::Error("E5103", "leaf node is full");
  }

  if (entry_count > 0U) {
    const LeafEntry last_entry = ReadLeafEntryAt(*page, static_cast<std::size_t>(entry_count - 1U));
    if (entry.key <= last_entry.key) {
      return LeafNodeStatus::Error("E5104", "leaf entry key must be strictly increasing");
    }
  }

  WriteLeafEntry(page, static_cast<std::size_t>(entry_count), entry);
  WriteUint16(page, kEntryCountOffset, static_cast<std::uint16_t>(entry_count + 1U));

  *out_index = entry_count;
  return LeafNodeStatus::Ok("appended leaf entry");
}

LeafNodeStatus ReadLeafEntry(const storage::Page& page,
                             std::uint16_t index,
                             LeafEntry* out_entry) {
  if (out_entry == nullptr) {
    return LeafNodeStatus::Error("E5100", "output leaf entry pointer is null");
  }

  std::uint16_t entry_count = 0U;
  const LeafNodeStatus layout_status = ValidateLeafLayout(page, &entry_count);
  if (!layout_status.ok) {
    return layout_status;
  }

  if (index >= entry_count) {
    return LeafNodeStatus::Error("E5102", "leaf entry index is outside entry count");
  }

  *out_entry = ReadLeafEntryAt(page, static_cast<std::size_t>(index));
  return LeafNodeStatus::Ok("read leaf entry");
}

LeafNodeStatus FindLeafEntryByKey(const storage::Page& page,
                                  std::int64_t key,
                                  LeafEntry* out_entry,
                                  std::uint16_t* out_index) {
  if (out_entry == nullptr && out_index == nullptr) {
    return LeafNodeStatus::Error("E5100", "output entry and index pointers are null");
  }

  std::uint16_t entry_count = 0U;
  const LeafNodeStatus layout_status = ValidateLeafLayout(page, &entry_count);
  if (!layout_status.ok) {
    return layout_status;
  }

  std::size_t left = 0U;
  std::size_t right = static_cast<std::size_t>(entry_count);

  while (left < right) {
    const std::size_t mid = left + (right - left) / 2U;
    const LeafEntry mid_entry = ReadLeafEntryAt(page, mid);

    if (mid_entry.key == key) {
      if (out_entry != nullptr) {
        *out_entry = mid_entry;
      }
      if (out_index != nullptr) {
        *out_index = static_cast<std::uint16_t>(mid);
      }
      return LeafNodeStatus::Ok("found leaf entry by key");
    }

    if (mid_entry.key < key) {
      left = mid + 1U;
    } else {
      right = mid;
    }
  }

  return LeafNodeStatus::Error("E5105", "leaf entry key not found");
}

LeafNodeStatus GetLeafEntryCount(const storage::Page& page, std::uint16_t* out_count) {
  if (out_count == nullptr) {
    return LeafNodeStatus::Error("E5100", "output entry count pointer is null");
  }

  std::uint16_t entry_count = 0U;
  const LeafNodeStatus layout_status = ValidateLeafLayout(page, &entry_count);
  if (!layout_status.ok) {
    return layout_status;
  }

  *out_count = entry_count;
  return LeafNodeStatus::Ok("leaf entry count available");
}

LeafNodeStatus SetLeafNextPage(storage::Page* page, std::uint32_t next_page_id) {
  if (page == nullptr) {
    return LeafNodeStatus::Error("E5100", "leaf node page pointer is null");
  }

  const LeafNodeStatus layout_status = ValidateLeafLayout(*page, nullptr);
  if (!layout_status.ok) {
    return layout_status;
  }

  WriteUint32(page, kNextPageOffset, next_page_id);
  return LeafNodeStatus::Ok("updated leaf next-page pointer");
}

LeafNodeStatus GetLeafNextPage(const storage::Page& page, std::uint32_t* out_next_page_id) {
  if (out_next_page_id == nullptr) {
    return LeafNodeStatus::Error("E5100", "output next-page pointer is null");
  }

  const LeafNodeStatus layout_status = ValidateLeafLayout(page, nullptr);
  if (!layout_status.ok) {
    return layout_status;
  }

  *out_next_page_id = ReadUint32(page, kNextPageOffset);
  return LeafNodeStatus::Ok("leaf next-page pointer available");
}

LeafNodeStatus SplitLeafNode(storage::Page* left_page,
                             storage::Page* right_page,
                             LeafSplitMetadata* out_metadata) {
  if (left_page == nullptr || right_page == nullptr || out_metadata == nullptr) {
    return LeafNodeStatus::Error("E5100", "leaf split pointer argument is null");
  }

  if (right_page->id == 0U) {
    return LeafNodeStatus::Error("E5106", "right leaf page id must be non-zero");
  }

  if (right_page->id == left_page->id) {
    return LeafNodeStatus::Error("E5106", "right leaf page id must differ from left leaf page id");
  }

  std::uint16_t original_entry_count = 0U;
  const LeafNodeStatus left_layout_status = ValidateLeafLayout(*left_page, &original_entry_count);
  if (!left_layout_status.ok) {
    return left_layout_status;
  }

  if (original_entry_count < 2U) {
    return LeafNodeStatus::Error("E5106", "leaf node must have at least two entries to split");
  }

  std::uint32_t original_next_page_id = 0U;
  const LeafNodeStatus next_status = GetLeafNextPage(*left_page, &original_next_page_id);
  if (!next_status.ok) {
    return next_status;
  }

  const std::uint16_t split_index = static_cast<std::uint16_t>(original_entry_count / 2U);
  std::vector<LeafEntry> left_entries;
  std::vector<LeafEntry> right_entries;
  left_entries.reserve(static_cast<std::size_t>(split_index));
  right_entries.reserve(static_cast<std::size_t>(original_entry_count - split_index));

  for (std::uint16_t index = 0U; index < original_entry_count; ++index) {
    const LeafEntry entry = ReadLeafEntryAt(*left_page, static_cast<std::size_t>(index));
    if (index < split_index) {
      left_entries.push_back(entry);
    } else {
      right_entries.push_back(entry);
    }
  }

  if (right_entries.empty()) {
    return LeafNodeStatus::Error("E5106", "leaf split produced an empty right side");
  }

  const std::uint32_t right_page_id = right_page->id;
  const std::uint32_t left_page_id = left_page->id;

  const LeafNodeStatus init_right_status = InitializeLeafNode(right_page);
  if (!init_right_status.ok) {
    return init_right_status;
  }

  const LeafNodeStatus right_next_status = SetLeafNextPage(right_page, original_next_page_id);
  if (!right_next_status.ok) {
    return right_next_status;
  }

  for (const LeafEntry& entry : right_entries) {
    std::uint16_t appended_index = 0U;
    const LeafNodeStatus append_status = AppendLeafEntry(right_page, entry, &appended_index);
    if (!append_status.ok) {
      return append_status;
    }
  }

  storage::Page rebuilt_left = storage::CreateZeroedPage(left_page_id);
  const LeafNodeStatus init_left_status = InitializeLeafNode(&rebuilt_left);
  if (!init_left_status.ok) {
    return init_left_status;
  }

  const LeafNodeStatus left_next_status = SetLeafNextPage(&rebuilt_left, right_page_id);
  if (!left_next_status.ok) {
    return left_next_status;
  }

  for (const LeafEntry& entry : left_entries) {
    std::uint16_t appended_index = 0U;
    const LeafNodeStatus append_status = AppendLeafEntry(&rebuilt_left, entry, &appended_index);
    if (!append_status.ok) {
      return append_status;
    }
  }

  *left_page = rebuilt_left;

  out_metadata->promoted_key = right_entries.front().key;
  out_metadata->left_entry_count = static_cast<std::uint16_t>(left_entries.size());
  out_metadata->right_entry_count = static_cast<std::uint16_t>(right_entries.size());
  out_metadata->right_page_id = right_page_id;

  return LeafNodeStatus::Ok("split leaf node and produced promoted separator key");
}

}  // namespace atlasdb::btree
