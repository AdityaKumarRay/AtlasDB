#pragma once

#include <cstdint>
#include <string>

#include "atlasdb/btree/leaf_node.hpp"
#include "atlasdb/storage/pager.hpp"

namespace atlasdb::btree {

struct BtreeIndexStatus {
  bool ok;
  std::string code;
  std::string message;

  static BtreeIndexStatus Ok(std::string message = {});
  static BtreeIndexStatus Error(std::string code, std::string message);
};

class BtreeIndex {
 public:
  explicit BtreeIndex(storage::Pager* pager);

  [[nodiscard]] BtreeIndexStatus Initialize(std::uint32_t* out_root_page_id);
  [[nodiscard]] BtreeIndexStatus Open(std::uint32_t root_page_id);
  [[nodiscard]] BtreeIndexStatus GetRootPageId(std::uint32_t* out_root_page_id) const;

  [[nodiscard]] BtreeIndexStatus ResolveLeafForKey(std::int64_t key,
                                                   std::uint32_t* out_leaf_page_id) const;
  [[nodiscard]] BtreeIndexStatus ResolveFirstLeaf(std::uint32_t* out_leaf_page_id) const;

  [[nodiscard]] BtreeIndexStatus Insert(const LeafEntry& entry);
  [[nodiscard]] BtreeIndexStatus Find(std::int64_t key, LeafEntry* out_entry) const;

 private:
  struct SplitResult {
    bool did_split{false};
    std::int64_t promoted_key{0};
    std::uint32_t right_page_id{0};
  };

  [[nodiscard]] BtreeIndexStatus EnsurePagerOpen() const;
  [[nodiscard]] BtreeIndexStatus EnsureRootInitialized() const;
  [[nodiscard]] BtreeIndexStatus ReadTreePage(std::uint32_t page_id,
                                              storage::Page* out_page) const;
  [[nodiscard]] BtreeIndexStatus FindLeafPageForKey(std::int64_t key,
                                                    std::uint32_t* out_leaf_page_id) const;
  [[nodiscard]] BtreeIndexStatus InsertRecursive(std::uint32_t page_id,
                                                 const LeafEntry& entry,
                                                 SplitResult* out_split_result);

  storage::Pager* pager_{nullptr};
  std::uint32_t root_page_id_{0};
};

}  // namespace atlasdb::btree
