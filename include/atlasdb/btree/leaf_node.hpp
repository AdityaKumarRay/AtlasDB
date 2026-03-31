#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "atlasdb/storage/page.hpp"

namespace atlasdb::btree {

struct LeafNodeStatus {
  bool ok;
  std::string code;
  std::string message;

  static LeafNodeStatus Ok(std::string message = {});
  static LeafNodeStatus Error(std::string code, std::string message);
};

struct LeafEntry {
  std::int64_t key{0};
  std::uint32_t row_page_id{0};
  std::uint16_t row_slot_index{0};
};

struct LeafSplitMetadata {
  std::int64_t promoted_key{0};
  std::uint16_t left_entry_count{0};
  std::uint16_t right_entry_count{0};
  std::uint32_t right_page_id{0};
};

inline constexpr std::uint16_t kLeafNodeFormatVersion = 1U;
inline constexpr std::size_t kLeafNodeHeaderSize = 24U;
inline constexpr std::size_t kLeafNodeEntrySize = 16U;
inline constexpr std::size_t kLeafNodeMaxEntries =
    (storage::kPageSize - kLeafNodeHeaderSize) / kLeafNodeEntrySize;

[[nodiscard]] LeafNodeStatus InitializeLeafNode(storage::Page* page);
[[nodiscard]] LeafNodeStatus AppendLeafEntry(storage::Page* page,
                                             const LeafEntry& entry,
                                             std::uint16_t* out_index);
[[nodiscard]] LeafNodeStatus ReadLeafEntry(const storage::Page& page,
                                           std::uint16_t index,
                                           LeafEntry* out_entry);
[[nodiscard]] LeafNodeStatus FindLeafEntryByKey(const storage::Page& page,
                                                std::int64_t key,
                                                LeafEntry* out_entry,
                                                std::uint16_t* out_index);
[[nodiscard]] LeafNodeStatus GetLeafEntryCount(const storage::Page& page,
                                               std::uint16_t* out_count);
[[nodiscard]] LeafNodeStatus SetLeafNextPage(storage::Page* page,
                                             std::uint32_t next_page_id);
[[nodiscard]] LeafNodeStatus GetLeafNextPage(const storage::Page& page,
                                             std::uint32_t* out_next_page_id);
[[nodiscard]] LeafNodeStatus SplitLeafNode(storage::Page* left_page,
                                           storage::Page* right_page,
                                           LeafSplitMetadata* out_metadata);

}  // namespace atlasdb::btree
