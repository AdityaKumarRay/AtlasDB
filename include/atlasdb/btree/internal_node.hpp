#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "atlasdb/storage/page.hpp"

namespace atlasdb::btree {

struct InternalNodeStatus {
  bool ok;
  std::string code;
  std::string message;

  static InternalNodeStatus Ok(std::string message = {});
  static InternalNodeStatus Error(std::string code, std::string message);
};

struct InternalEntry {
  std::int64_t key{0};
  std::uint32_t child_page_id{0};
};

struct InternalSplitMetadata {
  std::int64_t promoted_key{0};
  std::uint16_t left_entry_count{0};
  std::uint16_t right_entry_count{0};
  std::uint32_t right_page_id{0};
  std::uint32_t right_left_child_page_id{0};
};

inline constexpr std::uint16_t kInternalNodeFormatVersion = 1U;
inline constexpr std::size_t kInternalNodeHeaderSize = 24U;
inline constexpr std::size_t kInternalNodeEntrySize = 16U;
inline constexpr std::size_t kInternalNodeMaxEntries =
    (storage::kPageSize - kInternalNodeHeaderSize) / kInternalNodeEntrySize;

[[nodiscard]] InternalNodeStatus InitializeInternalNode(storage::Page* page,
                                                        std::uint32_t left_child_page_id);
[[nodiscard]] InternalNodeStatus AppendInternalEntry(storage::Page* page,
                                                     const InternalEntry& entry,
                                                     std::uint16_t* out_index);
[[nodiscard]] InternalNodeStatus InsertInternalEntry(storage::Page* page,
                                                     const InternalEntry& entry,
                                                     std::uint16_t* out_index);
[[nodiscard]] InternalNodeStatus ReadInternalEntry(const storage::Page& page,
                                                   std::uint16_t index,
                                                   InternalEntry* out_entry);
[[nodiscard]] InternalNodeStatus GetInternalEntryCount(const storage::Page& page,
                                                       std::uint16_t* out_count);
[[nodiscard]] InternalNodeStatus SetInternalLeftChild(storage::Page* page,
                                                      std::uint32_t left_child_page_id);
[[nodiscard]] InternalNodeStatus GetInternalLeftChild(const storage::Page& page,
                                                      std::uint32_t* out_left_child_page_id);
[[nodiscard]] InternalNodeStatus FindInternalChildForKey(const storage::Page& page,
                                                         std::int64_t key,
                                                         std::uint32_t* out_child_page_id);
[[nodiscard]] InternalNodeStatus InitializeInternalRootFromSplit(storage::Page* page,
                                                                 std::uint32_t left_child_page_id,
                                                                 std::int64_t separator_key,
                                                                 std::uint32_t right_child_page_id);
[[nodiscard]] InternalNodeStatus SplitInternalNode(storage::Page* left_page,
                                                   storage::Page* right_page,
                                                   InternalSplitMetadata* out_metadata);

}  // namespace atlasdb::btree
