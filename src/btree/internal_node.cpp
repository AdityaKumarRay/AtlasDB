#include "atlasdb/btree/internal_node.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

namespace atlasdb::btree {
namespace {

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

constexpr std::size_t kMagicOffset = 0U;
constexpr std::size_t kVersionOffset = 8U;
constexpr std::size_t kEntryCountOffset = 10U;
constexpr std::size_t kLeftChildOffset = 12U;
constexpr std::size_t kEntriesOffset = kInternalNodeHeaderSize;

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

void WriteInternalEntry(storage::Page* page, std::size_t entry_index, const InternalEntry& entry) {
  const std::size_t entry_offset = kEntriesOffset + entry_index * kInternalNodeEntrySize;
  WriteUint64(page, entry_offset + 0U, static_cast<std::uint64_t>(entry.key));
  WriteUint32(page, entry_offset + 8U, entry.child_page_id);
  WriteUint32(page, entry_offset + 12U, 0U);
}

InternalEntry ReadInternalEntryAt(const storage::Page& page, std::size_t entry_index) {
  const std::size_t entry_offset = kEntriesOffset + entry_index * kInternalNodeEntrySize;
  InternalEntry entry;
  entry.key = static_cast<std::int64_t>(ReadUint64(page, entry_offset + 0U));
  entry.child_page_id = ReadUint32(page, entry_offset + 8U);
  return entry;
}

InternalNodeStatus ValidateInternalLayout(const storage::Page& page,
                                          std::uint16_t* out_entry_count,
                                          std::uint32_t* out_left_child_page_id) {
  for (std::size_t index = 0U; index < kInternalNodeMagic.size(); ++index) {
    if (page.bytes[kMagicOffset + index] != kInternalNodeMagic[index]) {
      return InternalNodeStatus::Error("E5201", "internal node has invalid magic");
    }
  }

  const std::uint16_t version = ReadUint16(page, kVersionOffset);
  if (version != kInternalNodeFormatVersion) {
    return InternalNodeStatus::Error("E5201", "internal node has unsupported format version");
  }

  const std::uint16_t entry_count = ReadUint16(page, kEntryCountOffset);
  if (entry_count > static_cast<std::uint16_t>(kInternalNodeMaxEntries)) {
    return InternalNodeStatus::Error("E5201", "internal node entry count exceeds capacity");
  }

  const std::uint32_t left_child_page_id = ReadUint32(page, kLeftChildOffset);
  if (left_child_page_id == 0U) {
    return InternalNodeStatus::Error("E5201", "internal node left child page id is invalid");
  }

  if (entry_count > 0U) {
    InternalEntry previous = ReadInternalEntryAt(page, 0U);
    if (previous.child_page_id == 0U) {
      return InternalNodeStatus::Error("E5201", "internal node child page id is invalid");
    }

    for (std::uint16_t index = 1U; index < entry_count; ++index) {
      const InternalEntry current = ReadInternalEntryAt(page, index);
      if (current.child_page_id == 0U) {
        return InternalNodeStatus::Error("E5201", "internal node child page id is invalid");
      }
      if (current.key <= previous.key) {
        return InternalNodeStatus::Error("E5201", "internal node keys are not strictly increasing");
      }
      previous = current;
    }
  }

  if (out_entry_count != nullptr) {
    *out_entry_count = entry_count;
  }
  if (out_left_child_page_id != nullptr) {
    *out_left_child_page_id = left_child_page_id;
  }

  return InternalNodeStatus::Ok();
}

}  // namespace

InternalNodeStatus InternalNodeStatus::Ok(std::string message) {
  return InternalNodeStatus{true, "", std::move(message)};
}

InternalNodeStatus InternalNodeStatus::Error(std::string code, std::string message) {
  return InternalNodeStatus{false, std::move(code), std::move(message)};
}

InternalNodeStatus InitializeInternalNode(storage::Page* page, std::uint32_t left_child_page_id) {
  if (page == nullptr) {
    return InternalNodeStatus::Error("E5200", "internal node page pointer is null");
  }

  if (left_child_page_id == 0U) {
    return InternalNodeStatus::Error("E5202", "left child page id must be non-zero");
  }

  page->bytes.fill(0U);
  for (std::size_t index = 0U; index < kInternalNodeMagic.size(); ++index) {
    page->bytes[kMagicOffset + index] = kInternalNodeMagic[index];
  }

  WriteUint16(page, kVersionOffset, kInternalNodeFormatVersion);
  WriteUint16(page, kEntryCountOffset, 0U);
  WriteUint32(page, kLeftChildOffset, left_child_page_id);

  return InternalNodeStatus::Ok("initialized internal node");
}

InternalNodeStatus AppendInternalEntry(storage::Page* page,
                                       const InternalEntry& entry,
                                       std::uint16_t* out_index) {
  if (page == nullptr) {
    return InternalNodeStatus::Error("E5200", "internal node page pointer is null");
  }

  if (out_index == nullptr) {
    return InternalNodeStatus::Error("E5200", "output entry index pointer is null");
  }

  if (entry.child_page_id == 0U) {
    return InternalNodeStatus::Error("E5202", "internal entry child page id must be non-zero");
  }

  std::uint16_t entry_count = 0U;
  const InternalNodeStatus layout_status = ValidateInternalLayout(*page, &entry_count, nullptr);
  if (!layout_status.ok) {
    return layout_status;
  }

  if (entry_count == static_cast<std::uint16_t>(kInternalNodeMaxEntries)) {
    return InternalNodeStatus::Error("E5203", "internal node is full");
  }

  if (entry_count > 0U) {
    const InternalEntry last_entry = ReadInternalEntryAt(*page, static_cast<std::size_t>(entry_count - 1U));
    if (entry.key <= last_entry.key) {
      return InternalNodeStatus::Error("E5204", "internal entry key must be strictly increasing");
    }
  }

  WriteInternalEntry(page, static_cast<std::size_t>(entry_count), entry);
  WriteUint16(page, kEntryCountOffset, static_cast<std::uint16_t>(entry_count + 1U));

  *out_index = entry_count;
  return InternalNodeStatus::Ok("appended internal entry");
}

InternalNodeStatus InsertInternalEntry(storage::Page* page,
                                       const InternalEntry& entry,
                                       std::uint16_t* out_index) {
  if (page == nullptr) {
    return InternalNodeStatus::Error("E5200", "internal node page pointer is null");
  }

  if (out_index == nullptr) {
    return InternalNodeStatus::Error("E5200", "output entry index pointer is null");
  }

  if (entry.child_page_id == 0U) {
    return InternalNodeStatus::Error("E5202", "internal entry child page id must be non-zero");
  }

  std::uint16_t entry_count = 0U;
  const InternalNodeStatus layout_status = ValidateInternalLayout(*page, &entry_count, nullptr);
  if (!layout_status.ok) {
    return layout_status;
  }

  if (entry_count == static_cast<std::uint16_t>(kInternalNodeMaxEntries)) {
    return InternalNodeStatus::Error("E5203", "internal node is full");
  }

  std::size_t insert_index = 0U;
  while (insert_index < static_cast<std::size_t>(entry_count)) {
    const InternalEntry current = ReadInternalEntryAt(*page, insert_index);
    if (entry.key <= current.key) {
      break;
    }
    ++insert_index;
  }

  if (insert_index < static_cast<std::size_t>(entry_count)) {
    const InternalEntry current = ReadInternalEntryAt(*page, insert_index);
    if (entry.key == current.key) {
      return InternalNodeStatus::Error("E5204", "internal separator key must be unique");
    }
  }

  for (std::size_t index = static_cast<std::size_t>(entry_count); index > insert_index; --index) {
    const InternalEntry previous = ReadInternalEntryAt(*page, index - 1U);
    WriteInternalEntry(page, index, previous);
  }

  WriteInternalEntry(page, insert_index, entry);
  WriteUint16(page, kEntryCountOffset, static_cast<std::uint16_t>(entry_count + 1U));

  *out_index = static_cast<std::uint16_t>(insert_index);
  return InternalNodeStatus::Ok("inserted internal entry");
}

InternalNodeStatus ReadInternalEntry(const storage::Page& page,
                                     std::uint16_t index,
                                     InternalEntry* out_entry) {
  if (out_entry == nullptr) {
    return InternalNodeStatus::Error("E5200", "output internal entry pointer is null");
  }

  std::uint16_t entry_count = 0U;
  const InternalNodeStatus layout_status = ValidateInternalLayout(page, &entry_count, nullptr);
  if (!layout_status.ok) {
    return layout_status;
  }

  if (index >= entry_count) {
    return InternalNodeStatus::Error("E5205", "internal entry index is outside entry count");
  }

  *out_entry = ReadInternalEntryAt(page, static_cast<std::size_t>(index));
  return InternalNodeStatus::Ok("read internal entry");
}

InternalNodeStatus GetInternalEntryCount(const storage::Page& page, std::uint16_t* out_count) {
  if (out_count == nullptr) {
    return InternalNodeStatus::Error("E5200", "output entry count pointer is null");
  }

  std::uint16_t entry_count = 0U;
  const InternalNodeStatus layout_status = ValidateInternalLayout(page, &entry_count, nullptr);
  if (!layout_status.ok) {
    return layout_status;
  }

  *out_count = entry_count;
  return InternalNodeStatus::Ok("internal entry count available");
}

InternalNodeStatus SetInternalLeftChild(storage::Page* page, std::uint32_t left_child_page_id) {
  if (page == nullptr) {
    return InternalNodeStatus::Error("E5200", "internal node page pointer is null");
  }

  if (left_child_page_id == 0U) {
    return InternalNodeStatus::Error("E5202", "left child page id must be non-zero");
  }

  const InternalNodeStatus layout_status = ValidateInternalLayout(*page, nullptr, nullptr);
  if (!layout_status.ok) {
    return layout_status;
  }

  WriteUint32(page, kLeftChildOffset, left_child_page_id);
  return InternalNodeStatus::Ok("updated internal left child page id");
}

InternalNodeStatus GetInternalLeftChild(const storage::Page& page,
                                        std::uint32_t* out_left_child_page_id) {
  if (out_left_child_page_id == nullptr) {
    return InternalNodeStatus::Error("E5200", "output left child pointer is null");
  }

  std::uint32_t left_child_page_id = 0U;
  const InternalNodeStatus layout_status = ValidateInternalLayout(page, nullptr, &left_child_page_id);
  if (!layout_status.ok) {
    return layout_status;
  }

  *out_left_child_page_id = left_child_page_id;
  return InternalNodeStatus::Ok("internal left child page id available");
}

InternalNodeStatus FindInternalChildForKey(const storage::Page& page,
                                           std::int64_t key,
                                           std::uint32_t* out_child_page_id) {
  if (out_child_page_id == nullptr) {
    return InternalNodeStatus::Error("E5200", "output child page pointer is null");
  }

  std::uint16_t entry_count = 0U;
  std::uint32_t left_child_page_id = 0U;
  const InternalNodeStatus layout_status =
      ValidateInternalLayout(page, &entry_count, &left_child_page_id);
  if (!layout_status.ok) {
    return layout_status;
  }

  std::uint32_t candidate_child = left_child_page_id;
  for (std::uint16_t index = 0U; index < entry_count; ++index) {
    const InternalEntry entry = ReadInternalEntryAt(page, index);
    if (key < entry.key) {
      *out_child_page_id = candidate_child;
      return InternalNodeStatus::Ok("resolved internal child page for key");
    }
    candidate_child = entry.child_page_id;
  }

  *out_child_page_id = candidate_child;
  return InternalNodeStatus::Ok("resolved internal child page for key");
}

InternalNodeStatus InitializeInternalRootFromSplit(storage::Page* page,
                                                   std::uint32_t left_child_page_id,
                                                   std::int64_t separator_key,
                                                   std::uint32_t right_child_page_id) {
  if (page == nullptr) {
    return InternalNodeStatus::Error("E5200", "internal root page pointer is null");
  }

  if (left_child_page_id == 0U || right_child_page_id == 0U) {
    return InternalNodeStatus::Error("E5202", "split child page ids must be non-zero");
  }

  if (left_child_page_id == right_child_page_id) {
    return InternalNodeStatus::Error("E5206", "split child page ids must be distinct");
  }

  const InternalNodeStatus initialize_status = InitializeInternalNode(page, left_child_page_id);
  if (!initialize_status.ok) {
    return initialize_status;
  }

  std::uint16_t entry_index = 0U;
  const InternalNodeStatus append_status =
      AppendInternalEntry(page, InternalEntry{separator_key, right_child_page_id}, &entry_index);
  if (!append_status.ok) {
    return append_status;
  }

  return InternalNodeStatus::Ok("initialized internal root from split metadata");
}

}  // namespace atlasdb::btree
