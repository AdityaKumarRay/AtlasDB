#pragma once

#include <cstdint>
#include <string>

#include "atlasdb/btree/leaf_node.hpp"
#include "atlasdb/storage/pager.hpp"

namespace atlasdb::btree {

struct BtreeCursorStatus {
  bool ok;
  std::string code;
  std::string message;

  static BtreeCursorStatus Ok(std::string message = {});
  static BtreeCursorStatus Error(std::string code, std::string message);
};

class LeafCursor {
 public:
  explicit LeafCursor(storage::Pager* pager);

  [[nodiscard]] BtreeCursorStatus SeekFirst(std::uint32_t first_leaf_page_id);
  [[nodiscard]] BtreeCursorStatus Seek(std::uint32_t first_leaf_page_id, std::int64_t key);
  [[nodiscard]] BtreeCursorStatus Next();
  [[nodiscard]] BtreeCursorStatus Current(LeafEntry* out_entry) const;
  [[nodiscard]] bool IsValid() const noexcept;

 private:
  [[nodiscard]] BtreeCursorStatus EnsurePagerOpen() const;
  [[nodiscard]] BtreeCursorStatus LoadLeafPage(std::uint32_t page_id,
                                               storage::Page* out_page,
                                               std::uint16_t* out_entry_count,
                                               std::uint32_t* out_next_page_id) const;
  [[nodiscard]] BtreeCursorStatus SeekToFirstInChain(std::uint32_t start_leaf_page_id);

  storage::Pager* pager_{nullptr};
  std::uint32_t current_leaf_page_id_{0};
  std::uint16_t current_entry_index_{0};
  bool is_valid_{false};
};

}  // namespace atlasdb::btree
